using System.Buffers;
using Serilog;
using VsrReplica.Networking.Interfaces;
using VsrReplica.Networking.MemoryPool;
using VsrReplica.Networking.Transport;

namespace VsrReplica.Networking;

public class ManagedTcpConnection<TMessage> : IAsyncDisposable where TMessage : INetworkMessage
{
    private readonly record struct ParsedMessageResult(TMessage Message, IMemoryOwner<byte> Owner);

    private readonly Connection _connection;
    private readonly INetworkMessageSerializer<TMessage> _serializer;
    private readonly PinnedBlockMemoryPool _memoryPool;
    private readonly Func<ConnectionId, TMessage, ValueTask> _onMessageReceived;
    private readonly Func<ConnectionId, Exception?, ValueTask> _onClosed;
    private readonly Func<ConnectionId, Exception, ValueTask> _onError;
    private readonly CancellationTokenSource _cts = new();
    private Task _processTask = Task.CompletedTask;
    private readonly object _disposeLock = new();
    private bool _isDisposed;

    public ConnectionId Id { get; }
    public bool IsClosed => _isDisposed || _connection.IsClosed;

    public ManagedTcpConnection(
        ConnectionId connectionId,
        Connection connection,
        INetworkMessageSerializer<TMessage> serializer,
        PinnedBlockMemoryPool memoryPool,
        Func<ConnectionId, TMessage, ValueTask> onMessageReceived,
        Func<ConnectionId, Exception?, ValueTask> onClosed,
        Func<ConnectionId, Exception, ValueTask> onError)
    {
        Id = connectionId;
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _memoryPool = memoryPool ?? throw new ArgumentNullException(nameof(memoryPool));
        _onMessageReceived = onMessageReceived ?? throw new ArgumentNullException(nameof(onMessageReceived));
        _onClosed = onClosed ?? throw new ArgumentNullException(nameof(onClosed));
        _onError = onError ?? throw new ArgumentNullException(nameof(onError));

        _connection.OnError += HandleConnectionError;
        _connection.OnClosed += HandleConnectionClosed;
    }

    public void StartProcessing()
    {
        if (_isDisposed) throw new ObjectDisposedException(GetType().FullName);
        _connection.Start();
        _processTask = ProcessMessagesAsync(_cts.Token);
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        Log.Debug("ManagedConnection[{ConnectionId}]: Starting message processing loop.", Id);
        var reader = _connection.Input;
        Exception? error = null;

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_connection.IsClosed)
            {
                var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;
                var consumed = buffer.Start;

                if (result.IsCanceled)
                {
                    Log.Debug("ManagedConnection[{ConnectionId}]: PipeReader read cancelled.", Id);
                    break;
                }

                try
                {
                    Log.Verbose("ManagedConnection[{ConnectionId}]: Processing read result.", Id);
                    while (TryParseMessage(ref buffer, out ParsedMessageResult? parsedResult))
                    {
                        if (parsedResult == null) continue;

                        var message = parsedResult.Value.Message;
                        var owner = parsedResult.Value.Owner;

                        Log.Verbose("ManagedConnection[{ConnectionId}]: Received message.", Id);
                        try
                        {
                            await _onMessageReceived(Id, message);
                        }
                        catch (Exception callbackEx)
                        {
                            Log.Error(callbackEx,
                                "ManagedConnection[{ConnectionId}]: Error in OnMessageReceived callback.", Id);
                            try
                            {
                                await _onError(Id, callbackEx).ConfigureAwait(false);
                            }
                            catch (Exception onErrorEx)
                            {
                                Log.Error(onErrorEx,
                                    "ManagedConnection[{ConnectionId}]: Error executing OnError callback itself.", Id);
                            }
                        }
                        finally
                        {
                            owner.Dispose();
                            message.Dispose();
                        }

                        consumed = buffer.Start;
                    }

                    var examined = buffer.End;
                    reader.AdvanceTo(consumed, examined);

                    if (!result.IsCompleted) continue;
                    Log.Debug("ManagedConnection[{ConnectionId}]: PipeReader completed.", Id);
                    break;
                }
                catch (InvalidDataException dataEx)
                {
                    Log.Error(dataEx,
                        "ManagedConnection[{ConnectionId}]: Invalid data encountered. Closing connection.", Id);
                    error = dataEx;
                    try
                    {
                        await _onError(Id, dataEx).ConfigureAwait(false);
                    }
                    catch
                    {
                        /* Ignore */
                    }

                    await reader.CompleteAsync(error).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "ManagedConnection[{ConnectionId}]: Error processing message buffer.", Id);
                    error = ex;
                    try
                    {
                        await _onError(Id, ex).ConfigureAwait(false);
                    }
                    catch
                    {
                        /* Ignore */
                    }

                    break;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.Debug("ManagedConnection[{ConnectionId}]: Message processing loop cancelled.", Id);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ManagedConnection[{ConnectionId}]: Error reading from pipe.", Id);
            error = ex;
            try
            {
                await _onError(Id, ex).ConfigureAwait(false);
            }
            catch
            {
                /* Ignore */
            }
        }
        finally
        {
            await reader.CompleteAsync(error).ConfigureAwait(false);
            Log.Debug("ManagedConnection[{ConnectionId}]: Message processing loop finished.", Id);
            await DisposeAsyncInternal(error).ConfigureAwait(false);
        }
    }

    private bool TryParseMessage(ref ReadOnlySequence<byte> buffer, out ParsedMessageResult? result)
    {
        result = null;
        if (buffer.IsEmpty)
        {
            return false;
        }

        var sizeCheckBuffer = buffer;
        if (!_serializer.TryReadMessageSize(sizeCheckBuffer, out int messageSize))
        {
            Log.Verbose("ManagedConnection[{ConnectionId}]: Buffer too small to determine message size.", Id);
            return false;
        }

        if (messageSize <= 0)
        {
            Log.Warning("ManagedConnection[{ConnectionId}]: Invalid message size ({MessageSize}) detected.", Id,
                messageSize);
            throw new InvalidDataException($"Invalid message size detected: {messageSize}");
        }

        if (buffer.Length < messageSize)
        {
            Log.Verbose(
                "ManagedConnection[{ConnectionId}]: Incomplete message. Need {NeededBytes}, have {AvailableBytes}.", Id,
                messageSize, buffer.Length);
            return false;
        }

        var messageSequence = buffer.Slice(0, messageSize);
        IMemoryOwner<byte>? owner = null;

        try
        {
            owner = _memoryPool.Rent(messageSize);
            var rentedMemory = owner.Memory[..messageSize];
            messageSequence.CopyTo(rentedMemory.Span);

            Log.Verbose("ManagedConnection[{ConnectionId}]: Copied {MessageSize} bytes into rented buffer.", Id,
                messageSize);
            var message = _serializer.Deserialize(rentedMemory, owner);

            result = new ParsedMessageResult(message, owner);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ManagedConnection[{ConnectionId}]: Failed to rent, copy, or deserialize message.", Id);
            owner?.Dispose();
            throw new InvalidDataException("Failed to process message data (rent/copy/deserialize).", ex);
        }

        buffer = buffer.Slice(messageSize);
        Log.Verbose("ManagedConnection[{ConnectionId}]: Successfully parsed message ({MessageSize} bytes).", Id,
            messageSize);
        return true;
    }

    public async ValueTask SendMessageAsync(TMessage message)
    {
        if (IsClosed)
        {
            throw new InvalidOperationException($"Cannot send message on a closed connection (ID: {Id}).");
        }

        using var serializedMessage = _serializer.Serialize(message, _memoryPool);

        if (serializedMessage.Memory.IsEmpty)
        {
            Log.Warning(
                "ManagedConnection[{ConnectionId}]: Serializer returned empty memory for message. Skipping send.", Id);
            return;
        }

        try
        {
            Log.Verbose("ManagedConnection[{ConnectionId}]: Writing {ByteCount} bytes to output pipe.", Id,
                serializedMessage.Memory.Length);
            await _connection.Output.WriteAsync(serializedMessage.Memory, _cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            Log.Warning("ManagedConnection[{ConnectionId}]: Send operation cancelled due to connection disposal.", Id);
            await DisposeAsyncInternal(new OperationCanceledException("Send cancelled by disposal."))
                .ConfigureAwait(false);
            throw new ObjectDisposedException(GetType().FullName, "Send cancelled by disposal.");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ManagedConnection[{ConnectionId}]: Error writing message to output pipe.", Id);
            if (!_isDisposed)
            {
                await DisposeAsyncInternal(ex).ConfigureAwait(false);
            }

            throw;
        }
    }

    public async ValueTask SendRawBytesAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var token = linkedCts.Token;
        if (IsClosed || _cts.IsCancellationRequested)
        {
            if (_isDisposed) throw new ObjectDisposedException(GetType().FullName);
            throw new OperationCanceledException("Connection is closing.");
        }

        if (data.IsEmpty)
        {
            return;
        }

        try
        {
            Log.Verbose("ManagedConnection[{ConnectionId}]: Writing {ByteCount} raw bytes to output pipe.", Id,
                data.Length);
            await _connection.Output.WriteAsync(data, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Log specific reason if possible
            if (_cts.IsCancellationRequested)
                Log.Warning("ManagedConnection[{ConnectionId}]: Send raw bytes cancelled due to connection disposal.",
                    Id);
            else if (cancellationToken.IsCancellationRequested)
                Log.Warning("ManagedConnection[{ConnectionId}]: Send raw bytes cancelled by caller.", Id);
            else Log.Warning("ManagedConnection[{ConnectionId}]: Send raw bytes cancelled by linked token.", Id);
            throw; // Re-throw cancellation
        }
        catch (ObjectDisposedException)
        {
            Log.Warning("ManagedConnection[{ConnectionId}]: Send raw bytes failed, connection disposed.", Id);
            if (!_isDisposed)
                await DisposeAsyncInternal(new ObjectDisposedException("Connection disposed during raw send."))
                    .ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ManagedConnection[{ConnectionId}]: Error writing raw bytes to output pipe.", Id);
            if (!_isDisposed) await DisposeAsyncInternal(ex).ConfigureAwait(false);
            throw;
        }
    }

    private async ValueTask DisposeAsyncInternal(Exception? error = null)
    {
        bool needsCleanup = false;
        lock (_disposeLock)
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                needsCleanup = true;
            }
        }

        if (!needsCleanup) return;

        Log.Information("ManagedConnection[{ConnectionId}]: Disposing connection. Error: {Error}", Id,
            error?.Message ?? "None");

        if (!_cts.IsCancellationRequested)
        {
            try
            {
                await _cts.CancelAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                /* Ignore */
            }
        }

        _connection.OnError -= HandleConnectionError;
        _connection.OnClosed -= HandleConnectionClosed;

        try
        {
            await _processTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            Log.Warning(
                "ManagedConnection[{ConnectionId}]: Timeout waiting for processing task completion during disposal.",
                Id);
        }
        catch (Exception ex) when (ex is not ObjectDisposedException)
        {
            Log.Warning(ex, "ManagedConnection[{ConnectionId}]: Exception during processing task completion wait.", Id);
        }

        await _connection.DisposeAsync().ConfigureAwait(false);

        try
        {
            await _onClosed(Id, error).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ManagedConnection[{ConnectionId}]: Error executing OnClosed callback.", Id);
        }

        _cts.Dispose();
        Log.Information("ManagedConnection[{ConnectionId}]: Disposal complete.", Id);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncInternal().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void HandleConnectionError(Exception ex)
    {
        if (_isDisposed) return;
        Log.Warning(ex, "ManagedConnection[{ConnectionId}]: Underlying connection error detected.", Id);
        _ = DisposeAsyncInternal(ex);
    }

    private void HandleConnectionClosed()
    {
        if (_isDisposed) return;
        Log.Information("ManagedConnection[{ConnectionId}]: Underlying connection closed detected.", Id);
        _ = DisposeAsyncInternal();
    }
}