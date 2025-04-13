using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;
using Moq;
using VsrReplica.Networking.Interfaces;
using VsrReplica.Networking.Transport;

namespace VsrTests.VsrReplicaTests.NetworkingTests.Transport;

public class ConnectionTests : IAsyncLifetime
{
    private static readonly TimeSpan TestTimeout =
        Debugger.IsAttached ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(10);

    private Mock<ISocket> _mockSocket = null!;
    private Mock<ISenderPool> _mockSenderPool = null!;
    private Mock<IReceiver> _mockReceiver = null!;
    private Mock<ISender> _mockSender = null!;
    private MemoryPool<byte> _memoryPool = null!;
    private Connection _connection = null!;
    private CancellationTokenSource _testTimeoutCts = null!;

    // --- Timeout Helper Methods ---
    private static async Task WaitTaskAsync(Task task, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s!).TrySetResult(true), tcs))
        {
            var completedTask = await Task.WhenAny(task, tcs.Task);
            if (completedTask != task)
            {
                // Operation cancelled
                throw new OperationCanceledException(cancellationToken);
            }
        }

        await task; // Propagate exceptions from the original task
    }

    private static async Task<T> WaitTaskAsync<T>(Task<T> task, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s!).TrySetResult(true), tcs))
        {
            var completedTask = await Task.WhenAny(task, tcs.Task);
            if (completedTask != task)
            {
                // Operation cancelled
                throw new OperationCanceledException(cancellationToken);
            }
        }

        return await task; // Propagate exceptions or return result from the original task
    }
    // --- End Timeout Helper Methods ---


    // Helper to run Connection loops and wait for them to finish
    private async Task RunConnectionUntilClosedAsync(Connection connection)
    {
        var closedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var errorTcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        connection.OnClosed += () => closedTcs.TrySetResult(true);
        connection.OnError += ex => errorTcs.TrySetException(ex);

        // Start the connection loops
        connection.Start();

        // Wait for either the connection to close naturally or an error to occur
        var completedTask = await Task.WhenAny(closedTcs.Task, errorTcs.Task);
        await WaitTaskAsync(completedTask, _testTimeoutCts.Token); // Use helper

        // If an error occurred, rethrow it to fail the test clearly
        if (errorTcs.Task.IsFaulted)
        {
            // Accessing .Exception should be safe after await WhenAny
            throw errorTcs.Task.Exception!.InnerException ?? errorTcs.Task.Exception;
        }
        else if (!closedTcs.Task.IsCompletedSuccessfully)
        {
            // If neither completed successfully (e.g., timeout hit before WhenAny), throw.
            if (!_testTimeoutCts.IsCancellationRequested) // Check if it wasn't our timeout
                throw new TimeoutException("Connection did not close or error out within the expected time.");
            else // If it was our timeout, OperationCanceledException will be thrown by WaitTaskAsync below
                await Task.Delay(1); // Give cancellation exception a chance to propagate
        }


        // Ensure DisposeAsync also completes without hanging
        await WaitTaskAsync(connection.DisposeAsync().AsTask(), _testTimeoutCts.Token); // Use helper
    }


    public Task InitializeAsync()
    {
        _mockSocket = new Mock<ISocket>(MockBehavior.Strict); // Use Strict to catch unexpected calls
        _mockSenderPool = new Mock<ISenderPool>(MockBehavior.Strict);
        _mockReceiver = new Mock<IReceiver>(MockBehavior.Strict);
        _mockSender = new Mock<ISender>(MockBehavior.Loose);
        _memoryPool = MemoryPool<byte>.Shared;
        _testTimeoutCts = new CancellationTokenSource(TestTimeout);

        // --- Default Strict Setups ---
        _mockSocket.Setup(s => s.Shutdown(SocketShutdown.Both));
        _mockSocket.Setup(s => s.Dispose());

        _mockSenderPool.Setup(p => p.Rent()).Returns(_mockSender.Object);
        _mockSenderPool.Setup(p => p.Return(It.Is<ISender>(s => s == _mockSender.Object)));
        _mockSenderPool.Setup(p => p.Dispose());

        // Default SendAsync: Simulate consuming all data successfully
        _mockSender.Setup(s =>
                s.SendAsync(It.Is<ISocket>(sock => sock == _mockSocket.Object), It.IsAny<ReadOnlySequence<byte>>()))
            .Returns<ISocket, ReadOnlySequence<byte>>(async (sock, buffer) =>
            {
                // Explicit types
                await Task.Yield();
                return (int)buffer.Length;
            });
        _mockSender.Setup(s => s.Reset());
        _mockSender.Setup(s => s.Dispose());

        // Default ReceiveAsync: Simulate immediate graceful close
        _mockReceiver.Setup(r =>
                r.ReceiveAsync(It.Is<ISocket>(sock => sock == _mockSocket.Object), It.IsAny<Memory<byte>>()))
            .Returns<ISocket, Memory<byte>>(async (sock, mem) =>
            {
                // Explicit types
                await Task.Yield();
                return 0; // Signal close
            });
        _mockReceiver.Setup(r => r.Dispose());
        // --- End Default Strict Setups ---


        _connection = new Connection(
            _mockSocket.Object,
            _mockSenderPool.Object,
            _mockReceiver.Object,
            PipeScheduler.ThreadPool,
            PipeScheduler.ThreadPool,
            _memoryPool);

        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_connection != null && !_connection.IsClosed)
        {
            await _connection.Output.CompleteAsync();
            await _connection.Input.CompleteAsync();
            await Task.Delay(50); // Small delay for loops

            // Use helper for timeout during dispose
            await WaitTaskAsync(_connection.DisposeAsync().AsTask(), _testTimeoutCts.Token);
        }

        _testTimeoutCts?.Cancel(); // Cancel any pending delays
        _testTimeoutCts?.Dispose();
    }

    [Fact]
    public void Constructor_InitializesProperties()
    {
        Assert.NotNull(_connection.Input);
        Assert.NotNull(_connection.Output);
        Assert.False(_connection.IsClosed);
    }

    [Fact]
    public async Task Start_LoopsComplete_WhenReceiverClosesAndOutputCompletes()
    {
        // Arrange: Receiver mock already set up in InitializeAsync to return 0 (close)

        // Act & Assert
        var runTask = RunConnectionUntilClosedAsync(_connection);

        // Need to complete the output pipe to signal SendLoop to finish
        await _connection.Output.CompleteAsync();

        // Wait for the connection loops to fully close and dispose (using helper inside RunConnectionUntilClosedAsync)
        await runTask;

        // Verify mocks were called as expected for this scenario
        _mockReceiver.Verify(r => r.ReceiveAsync(_mockSocket.Object, It.IsAny<Memory<byte>>()), Times.AtLeastOnce);
        _mockSocket.Verify(s => s.Shutdown(SocketShutdown.Both), Times.AtLeastOnce);
        _mockSocket.Verify(s => s.Dispose(), Times.AtLeastOnce);
        _mockReceiver.Verify(r => r.Dispose(), Times.Once);
    }
    

    [Fact]
    public async Task ReceiveLoop_ReceivesData_DataAvailableOnInput()
    {
        // Arrange
        var dataToReceive = Encoding.UTF8.GetBytes("Hello Receiver");

        // Setup receiver mock with state to simulate sequence
        int receiveCallCount = 0; // State variable for the mock
        _mockReceiver.Setup(r => r.ReceiveAsync(_mockSocket.Object, It.IsAny<Memory<byte>>()))
            // Make the lambda async to allow awaits inside
            .Returns<ISocket, Memory<byte>>(async (socket, buffer) =>
            {
                int callNum = Interlocked.Increment(ref receiveCallCount); // Track calls
                if (callNum == 1)
                {
                    // First call: copy data to the buffer's Span
                    await Task.Yield(); // Good practice in async mock returns
                    dataToReceive.CopyTo(buffer.Span);
                    return dataToReceive.Length; // Return int, ValueTask<int> is inferred
                }
                else
                {
                    // Subsequent calls: return 0 (close) AFTER A SMALL DELAY
                    await Task.Delay(10, _testTimeoutCts.Token); // <--- ADD DELAY HERE
                    _testTimeoutCts.Token.ThrowIfCancellationRequested(); // Check cancellation after delay
                    return 0;
                }
            });

        // Keep SendLoop alive (rest of the setup remains the same)
        _mockSenderPool.Setup(p => p.Rent()).Returns(_mockSender.Object);
        _mockSender.Setup(s => s.SendAsync(It.IsAny<ISocket>(), It.IsAny<ReadOnlySequence<byte>>()))
            .Returns<ISocket, ReadOnlySequence<byte>>(async (sock, bufferSeq) =>
            {
                // Explicit types
                await Task.Delay(Timeout.InfiniteTimeSpan, _testTimeoutCts.Token);
                _testTimeoutCts.Token.ThrowIfCancellationRequested();
                return (int)bufferSeq.Length;
            });


        _connection.Start();

        // Act
        // Wait for the data to appear in the Input pipe (using helper)
        var readResult = await WaitTaskAsync(_connection.Input.ReadAsync(_testTimeoutCts.Token).AsTask(),
            _testTimeoutCts.Token);

        // Assert
        Assert.False(readResult.IsCanceled);
        // THIS ASSERT SHOULD NOW PASS because the delay prevents premature completion
        Assert.False(readResult.IsCompleted);
        Assert.Equal(dataToReceive.Length, readResult.Buffer.Length);
        Assert.Equal(dataToReceive, readResult.Buffer.ToArray());

        // Advance the reader
        _connection.Input.AdvanceTo(readResult.Buffer.End);

        // Now expect the loop to call ReceiveAsync again (after the delay), get 0,
        // and complete the pipe (using helper)
        var finalReadResult = await WaitTaskAsync(_connection.Input.ReadAsync(_testTimeoutCts.Token).AsTask(),
            _testTimeoutCts.Token);

        // Assert loop completion
        Assert.True(finalReadResult.IsCompleted); // This should still be true eventually
        _mockReceiver.Verify(r => r.ReceiveAsync(_mockSocket.Object, It.IsAny<Memory<byte>>()),
            Times.Exactly(2)); // Verify it was called twice

        // Cleanup
        _testTimeoutCts.Cancel(); // Cancel the infinite delay in sender mock
        await _connection.Output.CompleteAsync(); // Allow send loop to finish
    }

    [Fact]
    public async Task Shutdown_CallsSocketShutdownAndDispose_SetsIsClosed()
    {
        // Arrange
        // Keep loops busy
        _mockReceiver.Setup(r => r.ReceiveAsync(It.IsAny<ISocket>(), It.IsAny<Memory<byte>>()))
            .Returns<ISocket, Memory<byte>>(async (sock, mem) =>
            {
                // Explicit types
                await Task.Delay(Timeout.InfiniteTimeSpan, _testTimeoutCts.Token);
                _testTimeoutCts.Token.ThrowIfCancellationRequested();
                return 0;
            });
        _mockSenderPool.Setup(p => p.Rent()).Returns(_mockSender.Object);
        _mockSender.Setup(s => s.SendAsync(It.IsAny<ISocket>(), It.IsAny<ReadOnlySequence<byte>>()))
            .Returns<ISocket, ReadOnlySequence<byte>>(async (sock, buffer) =>
            {
                // Explicit types
                await Task.Delay(Timeout.InfiniteTimeSpan, _testTimeoutCts.Token);
                _testTimeoutCts.Token.ThrowIfCancellationRequested();
                return (int)buffer.Length;
            });

        _connection.Start();
        await Task.Yield(); // Ensure loops have started

        // Act
        _connection.Shutdown();

        // Assert
        _mockSocket.Verify(s => s.Shutdown(SocketShutdown.Both), Times.Once);
        _mockSocket.Verify(s => s.Dispose(), Times.Once);
        Assert.True(_connection.IsClosed);

        // Act again (should be idempotent)
        _connection.Shutdown();

        // Assert again (counts should not increase)
        _mockSocket.Verify(s => s.Shutdown(SocketShutdown.Both), Times.Once);
        _mockSocket.Verify(s => s.Dispose(), Times.Once);

        // Cleanup
        _testTimeoutCts.Cancel(); // Stop the loops
    }

    [Fact]
    public async Task ReceiveLoop_ExceptionDuringReceive_InvokesOnErrorAndCloses()
    {
        // Arrange
        var exception = new SocketException((int)SocketError.Interrupted);
        var onErrorTcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var onClosedTcs =
            new TaskCompletionSource<bool>(TaskCreationOptions
                .RunContinuationsAsynchronously); // Need to wait for OnClosed too

        _mockReceiver.Reset(); // Reset default mock setup
        _mockReceiver.Setup(r => r.ReceiveAsync(_mockSocket.Object, It.IsAny<Memory<byte>>()))
            .Returns<ISocket, Memory<byte>>(async (sock, mem) =>
            {
                // Explicit types
                await Task.Yield();
                throw exception;
            });
        _mockReceiver.Setup(r => r.Dispose());

        // Keep sender busy (Setup remains the same as before)
        _mockSenderPool.Setup(p => p.Rent()).Returns(_mockSender.Object);
        _mockSender.Setup(s => s.SendAsync(It.IsAny<ISocket>(), It.IsAny<ReadOnlySequence<byte>>()))
            .Returns<ISocket, ReadOnlySequence<byte>>(async (sock, buffer) =>
            {
                // Explicit types
                await Task.Delay(Timeout.InfiniteTimeSpan, _testTimeoutCts.Token);
                _testTimeoutCts.Token.ThrowIfCancellationRequested();
                return (int)buffer.Length;
            });
        _mockSender.Setup(s => s.Reset());
        _mockSender.Setup(s => s.Dispose());
        _mockSenderPool.Setup(p => p.Return(It.IsAny<ISender>()));
        _mockSenderPool.Setup(p => p.Dispose());


        _connection.OnError += ex => onErrorTcs.TrySetResult(ex);
        _connection.OnClosed += () => onClosedTcs.TrySetResult(true); // Hook up OnClosed

        // Act
        _connection.Start(); // Start the connection. The exception will be thrown internally.

        // Assert
        // 1. Wait specifically for the OnError event to be triggered
        var capturedException = await WaitTaskAsync(onErrorTcs.Task, _testTimeoutCts.Token);
        Assert.Same(exception, capturedException); // Verify the correct exception was passed via the event

        // 2. Wait for the OnClosed event, which should be triggered by the finally block after the exception
        await WaitTaskAsync(onClosedTcs.Task, _testTimeoutCts.Token);

        // 3. Verify shutdown was called due to the error handling in the finally block
        _mockSocket.Verify(s => s.Shutdown(SocketShutdown.Both), Times.AtLeastOnce);
        _mockSocket.Verify(s => s.Dispose(), Times.AtLeastOnce);

        // Cleanup
        _testTimeoutCts.Cancel(); // Ensure sender mock stops its delay
        // DisposeAsync will be called by the test framework's IAsyncLifetime.DisposeAsync
    }
}