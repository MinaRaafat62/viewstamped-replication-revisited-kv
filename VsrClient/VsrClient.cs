using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Serilog;
using VsrReplica.Networking.MemoryPool;
using VsrReplica.VsrCore;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.Utils;

namespace VsrClient;

public class VsrClient : IAsyncDisposable
{
    private readonly record struct PendingRequest(
        TaskCompletionSource<VsrMessage> Tcs,
        CancellationTokenRegistration CtsReg);

    internal class ReplicaConnection : IAsyncDisposable
    {
        private enum ConnectionState
        {
            Initial, // Before Start() is called
            Disconnected, // Actively trying to connect or waiting for retry timer
            Connecting, // ConnectAsync in progress
            Connected, // Socket is connected, I/O tasks are running
            Retrying, // Waiting for retry timer
            Disposing, // DisposeAsync has been called, shutting down
            Disposed // Fully disposed
        }

        public IPEndPoint EndPoint { get; }
        private readonly VsrClient _parentClient;
        private readonly CancellationTokenSource _instanceDisposalCts = new(); // Signals overall disposal
        private Task _mainLoopTask = Task.CompletedTask;

        private ConnectionState _state = ConnectionState.Initial;
        private readonly object _stateLock = new();
        private Timer? _retryTimer;
        private int _retryAttempts = 0;

        // Resources for the *current active* socket session
        private Socket? _currentSocket;
        private CancellationTokenSource? _currentSessionCts; // For I/O tasks of _currentSocket
        // Pipe is created per session now

        public bool IsEffectivelyConnected
        {
            get
            {
                lock (_stateLock)
                {
                    return _state == ConnectionState.Connected && _currentSocket is { Connected: true };
                }
            }
        }

        public ReplicaConnection(IPEndPoint endPoint, VsrClient parentClient)
        {
            EndPoint = endPoint;
            _parentClient = parentClient;
        }

        public void Start()
        {
            lock (_stateLock)
            {
                if (_state != ConnectionState.Initial)
                {
                    Log.Debug(
                        "ReplicaConnection[{Endpoint}]: Start called but already started or disposed. State: {State}",
                        EndPoint, _state);
                    return;
                }

                _state = ConnectionState.Disconnected; // Initial state for the loop
                Log.Information("ReplicaConnection[{Endpoint}]: Starting main connection loop.", EndPoint);
                _mainLoopTask = MainConnectionLoopAsync(_instanceDisposalCts.Token);
            }
        }

        private async Task MainConnectionLoopAsync(CancellationToken disposalToken)
        {
            Log.Information("ReplicaConnection[{Endpoint}]: Main connection loop initiated.", EndPoint);
            try
            {
                while (!disposalToken.IsCancellationRequested)
                {
                    ConnectionState currentState;
                    lock (_stateLock)
                    {
                        currentState = _state;
                    }

                    Log.Verbose("ReplicaConnection[{Endpoint}]: Loop tick. State: {State}", EndPoint, currentState);

                    switch (currentState)
                    {
                        case ConnectionState.Disconnected:
                            // AttemptConnectionAsync will call HandleSocketSessionEndedAsync internally if it fails
                            // and needs to schedule a retry or transition state.
                            await AttemptConnectionAsync(disposalToken).ConfigureAwait(false);
                            break;
                        case ConnectionState.Connecting:
                        case ConnectionState.Connected:
                        case ConnectionState.Retrying:
                            await Task.Delay(TimeSpan.FromMilliseconds(200), disposalToken).ConfigureAwait(false);
                            break;
                        case ConnectionState.Disposing: // Should be handled by disposalToken.IsCancellationRequested
                        case ConnectionState.Disposed:
                            Log.Debug(
                                "ReplicaConnection[{Endpoint}]: Main loop exiting due to Disposing/Disposed state.",
                                EndPoint);
                            return;
                    }
                }
            }
            catch (OperationCanceledException) when (disposalToken.IsCancellationRequested)
            {
                Log.Information("ReplicaConnection[{Endpoint}]: Main connection loop cancelled due to disposal.",
                    EndPoint);
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    "ReplicaConnection[{Endpoint}]: Unhandled exception in main connection loop. This is unexpected.",
                    EndPoint);
                // Ensure state reflects failure if loop crashes and HandleSocketSessionEndedAsync is called
                await HandleSocketSessionEndedAsync(ex, disposalToken, true)
                    .ConfigureAwait(false); // Pass true for mainLoopIsExiting
            }
            finally
            {
                Log.Information("ReplicaConnection[{Endpoint}]: Main connection loop ended.", EndPoint);
                // Ensure final cleanup of any active session if the loop exits.
                // The 'true' for mainLoopIsExiting signals it's part of the overall shutdown of this loop.
                await HandleSocketSessionEndedAsync(new TaskCanceledException("Main connection loop ended."),
                    disposalToken, true).ConfigureAwait(false);
            }
        }

        private async Task AttemptConnectionAsync(CancellationToken disposalToken)
        {
            Socket? newSessionSocket = null;
            Pipe? newSessionPipe = null;
            CancellationTokenSource? newSessionCts = null;

            lock (_stateLock)
            {
                // Double-check state in case it changed while waiting for the lock
                if (_state != ConnectionState.Disconnected || disposalToken.IsCancellationRequested) return;
                _state = ConnectionState.Connecting;
            }

            Log.Debug("ReplicaConnection[{Endpoint}]: Attempting to connect (Attempt: {Attempt})...", EndPoint,
                _retryAttempts + 1);

            try
            {
                newSessionSocket = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    { NoDelay = true };
                newSessionPipe = new Pipe(new PipeOptions(
                    pool: _parentClient._memoryPool,
                    readerScheduler: PipeScheduler.ThreadPool,
                    writerScheduler: PipeScheduler.ThreadPool,
                    useSynchronizationContext: false,
                    minimumSegmentSize: VsrClient.MinBufferSegmentSize
                ));
                newSessionCts =
                    CancellationTokenSource.CreateLinkedTokenSource(disposalToken); // Link to overall disposal

                using var connectTimeoutCts = new CancellationTokenSource(_parentClient._connectTimeout);
                using var attemptConnectCts =
                    CancellationTokenSource.CreateLinkedTokenSource(newSessionCts.Token, connectTimeoutCts.Token);

                await newSessionSocket.ConnectAsync(EndPoint, attemptConnectCts.Token).ConfigureAwait(false);

                // Socket connected successfully
                lock (_stateLock)
                {
                    if (_state == ConnectionState.Disposed || disposalToken.IsCancellationRequested)
                    {
                        CleanupFailedAttemptResources(newSessionSocket, newSessionPipe, newSessionCts,
                            new OperationCanceledException("Disposed during connect success."));
                        return;
                    }

                    if (_state != ConnectionState.Connecting) // e.g. another thread called DisposeAsync
                    {
                        Log.Warning(
                            "ReplicaConnection[{Endpoint}]: Connected socket but state changed from Connecting to {CurrentState}. Discarding new connection.",
                            EndPoint, _state);
                        CleanupFailedAttemptResources(newSessionSocket, newSessionPipe, newSessionCts,
                            new InvalidOperationException("State changed during connect."));
                        return;
                    }

                    // Clean up any *previous* session's resources. This is a safeguard.
                    // HandleSessionEndedAsync should have done this, but races are possible.
                    _currentSocket?.Dispose();
                    if (_currentSessionCts != null && !_currentSessionCts.IsCancellationRequested)
                    {
                        try
                        {
                            _currentSessionCts.Cancel();
                        }
                        catch
                        {
                        }
                    }

                    _currentSessionCts?.Dispose();

                    _currentSocket = newSessionSocket;
                    _currentSessionCts = newSessionCts;
                    _state = ConnectionState.Connected;
                    _retryAttempts = 0; // Reset on success
                    Log.Information("ReplicaConnection[{Endpoint}]: Successfully connected. Socket: {SocketHandle}",
                        EndPoint, _currentSocket.Handle);

                    // Start I/O tasks for this new session. This task is not awaited by ConnectionLoopAsync.
                    // Its lifecycle is managed by _currentSessionCts.
                    _ = ProcessSocketSessionAsync(_currentSocket, newSessionPipe.Writer, newSessionPipe.Reader,
                        _currentSessionCts.Token);
                }

                _parentClient.NotifyConnectionStatusChanged(this, true);
            }
            catch (OperationCanceledException ex) when (disposalToken.IsCancellationRequested)
            {
                Log.Information("ReplicaConnection[{Endpoint}]: Main loop cancelled during connect attempt.", EndPoint);
                CleanupFailedAttemptResources(newSessionSocket, newSessionPipe, newSessionCts, ex);
                // No need to call HandleSocketSessionEndedAsync here, the main loop's finally will handle it.
                // Or, if this cancellation is specific to the attempt (e.g. connectTimeoutCts), then HandleSocketSessionEndedAsync is appropriate.
                // Let's assume if disposalToken is cancelled, the main loop is shutting down.
                // If it's attemptConnectCts (due to connectTimeoutCts), then we should handle it as a session end.
                if (!disposalToken.IsCancellationRequested && newSessionCts != null &&
                    newSessionCts.Token.IsCancellationRequested) // Check if it was the session's CTS
                {
                    await HandleSocketSessionEndedAsync(ex, disposalToken).ConfigureAwait(false);
                }
                else if (disposalToken.IsCancellationRequested)
                {
                    // Main disposal, loop will exit.
                }
                else // Other cancellation, likely connect timeout
                {
                    await HandleSocketSessionEndedAsync(ex, disposalToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException ex) // Connect timeout or newSessionCts cancellation
            {
                Log.Warning("ReplicaConnection[{Endpoint}]: Connection attempt cancelled (e.g., timeout).", EndPoint);
                CleanupFailedAttemptResources(newSessionSocket, newSessionPipe, newSessionCts, ex);
                await HandleSocketSessionEndedAsync(ex, disposalToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "ReplicaConnection[{Endpoint}]: Connection attempt failed.", EndPoint);
                CleanupFailedAttemptResources(newSessionSocket, newSessionPipe, newSessionCts, ex);
                await HandleSocketSessionEndedAsync(ex, disposalToken).ConfigureAwait(false);
            }
        }

        private void CleanupFailedAttemptResources(Socket? socket, Pipe? pipe, CancellationTokenSource? cts,
            Exception? reason)
        {
            socket?.Dispose();
            if (cts != null)
            {
                if (!cts.IsCancellationRequested)
                    try
                    {
                        cts.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                    }

                cts.Dispose();
            }

            pipe?.Writer.Complete(reason);
            pipe?.Reader.Complete(reason);
        }

        private async Task ProcessSocketSessionAsync(Socket activeSocket, PipeWriter pipeWriter, PipeReader pipeReader,
            CancellationToken sessionToken)
        {
            Log.Debug("ReplicaConnection[{Endpoint}]: Starting I/O session for socket {SocketHandle}.", EndPoint,
                activeSocket.Handle);
            Task fillPipeTask = FillPipeFromSocketAsync(activeSocket, pipeWriter, sessionToken);
            Task processPipeTask = ProcessPipeMessagesAsync(pipeReader, sessionToken);
            Exception? sessionError = null;

            try
            {
                // Wait for either task to complete. If one fails, the sessionToken 
                // (which is _currentSessionCts.Token) will be cancelled by HandleSocketSessionEndedAsync,
                // which in turn should stop the other task.
                var completedTask = await Task.WhenAny(fillPipeTask, processPipeTask).ConfigureAwait(false);

                if (completedTask.IsFaulted) sessionError = completedTask.Exception?.GetBaseException();
                else if (completedTask.IsCanceled)
                    sessionError = new OperationCanceledException("A socket I/O task was cancelled.", sessionToken);

                // Ensure both tasks are fully awaited to observe their exceptions and allow finally blocks to run
                try
                {
                    await Task.WhenAll(fillPipeTask, processPipeTask).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (sessionError == null) sessionError = ex; // Prefer the first error
                    Log.Debug(ex,
                        "ReplicaConnection[{Endpoint}]: Exception observed during WhenAll in ProcessSocketSessionAsync for {SocketHandle}.",
                        EndPoint, activeSocket.Handle);
                }
            }
            catch (OperationCanceledException ex) when (sessionToken.IsCancellationRequested)
            {
                Log.Debug(
                    "ReplicaConnection[{Endpoint}]: Socket session cancelled for {SocketHandle}. Reason: {Reason}",
                    EndPoint, activeSocket.Handle, ex.Message);
                sessionError = ex;
            }
            catch (Exception ex) // Should be rare if sub-tasks handle their errors
            {
                Log.Error(ex,
                    "ReplicaConnection[{Endpoint}]: Unexpected error in ProcessSocketSessionAsync supervisor for {SocketHandle}.",
                    EndPoint, activeSocket.Handle);
                sessionError = ex;
            }
            finally
            {
                Log.Debug(
                    "ReplicaConnection[{Endpoint}]: Socket session processing finished for {SocketHandle}. Error: {ErrorMsg}",
                    EndPoint, activeSocket.Handle, sessionError?.Message ?? "None");
                // Pipe writer/reader completion is handled in FillPipe/ProcessPipe's finally blocks.

                // If the overall instance is not being disposed, an unexpected session end means we need to handle it.
                if (!_instanceDisposalCts.IsCancellationRequested)
                {
                    await HandleSocketSessionEndedAsync(sessionError, _instanceDisposalCts.Token).ConfigureAwait(false);
                }
            }
        }

        private async Task FillPipeFromSocketAsync(Socket currentSocket, PipeWriter writer,
            CancellationToken sessionToken)
        {
            Exception? error = null;
            var socketHandle = currentSocket.Handle; // Capture for logging in case currentSocket becomes null
            Log.Debug("ReplicaConnection[{Endpoint}]: FillPipe loop started for socket {SocketHandle}.", EndPoint,
                socketHandle);
            try
            {
                while (!sessionToken.IsCancellationRequested)
                {
                    Memory<byte> memory = writer.GetMemory(MinBufferSegmentSize);
                    int bytesRead = await currentSocket.ReceiveAsync(memory, SocketFlags.None, sessionToken)
                        .ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        Log.Information(
                            "ReplicaConnection[{Endpoint}]: Socket {SocketHandle} closed gracefully by remote peer.",
                            EndPoint, socketHandle);
                        break;
                    }

                    Log.Verbose("ReplicaConnection[{Endpoint}]: Received {BytesRead} bytes from socket {SocketHandle}.",
                        EndPoint, bytesRead, socketHandle);
                    writer.Advance(bytesRead);
                    FlushResult result = await writer.FlushAsync(sessionToken).ConfigureAwait(false);
                    if (result.IsCompleted || result.IsCanceled)
                    {
                        Log.Debug(
                            "ReplicaConnection[{Endpoint}]: FillPipe FlushAsync completed/cancelled for socket {SocketHandle}.",
                            EndPoint, socketHandle);
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (sessionToken.IsCancellationRequested)
            {
                Log.Debug("ReplicaConnection[{Endpoint}]: FillPipe loop cancelled for socket {SocketHandle}.", EndPoint,
                    socketHandle);
                error = new OperationCanceledException("FillPipe cancelled by sessionToken", sessionToken);
            }
            catch (SocketException ex) when (IsSocketCloseError(ex.SocketErrorCode))
            {
                Log.Warning(
                    "ReplicaConnection[{Endpoint}]: Socket {SocketHandle} connection closed in FillPipe ({SocketErrorCode}).",
                    EndPoint, socketHandle, ex.SocketErrorCode);
                error = ex;
            }
            catch (ObjectDisposedException)
            {
                Log.Debug("ReplicaConnection[{Endpoint}]: FillPipe loop stopped, socket {SocketHandle} disposed.",
                    EndPoint, socketHandle);
                error = new ObjectDisposedException($"Socket in FillPipe for {EndPoint}");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "ReplicaConnection[{Endpoint}]: Error in FillPipe loop for socket {SocketHandle}.",
                    EndPoint, socketHandle);
                error = ex;
            }
            finally
            {
                await writer.CompleteAsync(error).ConfigureAwait(false);
                Log.Debug(
                    "ReplicaConnection[{Endpoint}]: FillPipe loop finished for socket {SocketHandle}. Error: {ErrorMsg}",
                    EndPoint, socketHandle, error?.Message ?? "None");
            }
        }

        private async Task ProcessPipeMessagesAsync(PipeReader reader, CancellationToken sessionToken)
        {
            Exception? error = null;
            Log.Debug("ReplicaConnection[{Endpoint}]: ProcessPipe loop started.", EndPoint);
            try
            {
                while (!sessionToken.IsCancellationRequested)
                {
                    ReadResult result = await reader.ReadAsync(sessionToken).ConfigureAwait(false);
                    ReadOnlySequence<byte> buffer = result.Buffer;
                    Log.Verbose(
                        "ReplicaConnection[{Endpoint}]: ProcessPipe ReadAsync. Buffer: {Length}, Canceled: {IsCanceled}, Completed: {IsCompleted}",
                        EndPoint, buffer.Length, result.IsCanceled, result.IsCompleted);

                    if (result.IsCanceled) break;
                    if (result.IsCompleted && buffer.IsEmpty)
                    {
                        Log.Debug("ReplicaConnection[{Endpoint}]: ProcessPipe reader completed with empty buffer.",
                            EndPoint);
                        break;
                    }

                    try
                    {
                        while (_parentClient.TryParseMessageFromBuffer(ref buffer, out VsrMessage? parsedMsg,
                                   out IMemoryOwner<byte>? owner))
                        {
                            if (parsedMsg != null)
                            {
                                Log.Debug("ReplicaConnection[{Endpoint}]: Parsed message: {Command}, Req: {Request}",
                                    EndPoint, parsedMsg.Header.Command, parsedMsg.Header.Request);
                                _parentClient.HandleReceivedMessage(parsedMsg,
                                    EndPoint); // HandleReceivedMessage is responsible for disposing
                            }
                        }

                        reader.AdvanceTo(buffer.Start, buffer.End);
                    }
                    catch (InvalidDataException dataEx) // From TryParseMessageFromBuffer
                    {
                        Log.Error(dataEx,
                            "ReplicaConnection[{Endpoint}]: Invalid data encountered during message parsing.",
                            EndPoint);
                        error = dataEx;
                        break; // Exit loop, pipe will be completed in finally
                    }
                    catch (Exception ex) // Other unexpected errors during parsing loop
                    {
                        Log.Error(ex, "ReplicaConnection[{Endpoint}]: Unexpected error during message parsing loop.",
                            EndPoint);
                        error = ex;
                        break; // Exit loop
                    }

                    if (result.IsCompleted)
                    {
                        Log.Debug("ReplicaConnection[{Endpoint}]: ProcessPipe reader completed by writer.", EndPoint);
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (sessionToken.IsCancellationRequested)
            {
                Log.Debug("ReplicaConnection[{Endpoint}]: ProcessPipe loop cancelled.", EndPoint);
                error = new OperationCanceledException("ProcessPipe cancelled by sessionToken", sessionToken);
            }
            catch (Exception ex) // Errors from reader.ReadAsync() itself
            {
                Log.Error(ex, "ReplicaConnection[{Endpoint}]: Error reading from pipe in ProcessPipe.", EndPoint);
                error = ex;
            }
            finally
            {
                await reader.CompleteAsync(error).ConfigureAwait(false);
                Log.Debug("ReplicaConnection[{Endpoint}]: ProcessPipe loop finished. Error: {ErrorMsg}", EndPoint,
                    error?.Message ?? "None");
            }
        }

        private static bool IsSocketCloseError(SocketError errorCode) =>
            errorCode == SocketError.ConnectionReset || errorCode == SocketError.ConnectionAborted ||
            errorCode == SocketError.Shutdown || errorCode == SocketError.SocketError ||
            errorCode == SocketError.NetworkDown || errorCode == SocketError.NetworkUnreachable ||
            errorCode == SocketError.ConnectionRefused || errorCode == SocketError.NotConnected;

        private Task HandleSocketSessionEndedAsync(Exception? closeReason, CancellationToken disposalToken,
            bool mainLoopIsExiting = false)
        {
            Socket? socketToDispose = null;
            CancellationTokenSource? sessionCtsToCleanup = null;
            var wasConnectedAndNeedsRetry = false;
            var wasConnectedAtAll = false;

            lock (_stateLock)
            {
                if (_state == ConnectionState.Disposed && !mainLoopIsExiting)
                    return Task.CompletedTask;

                Log.Debug(
                    "ReplicaConnection[{Endpoint}]: HandleSocketSessionEndedAsync. Current State: {State}, Reason: {Reason}, MainLoopExiting: {MainLoopExiting}",
                    EndPoint, _state, closeReason?.GetType().Name ?? "Normal", mainLoopIsExiting);

                wasConnectedAtAll = (_state == ConnectionState.Connected);

                socketToDispose = _currentSocket;
                _currentSocket = null;
                sessionCtsToCleanup = _currentSessionCts;

                if (mainLoopIsExiting || disposalToken.IsCancellationRequested)
                {
                    _state = ConnectionState.Disposed; // Final state
                }
                else if (_state == ConnectionState.Connected || _state == ConnectionState.Connecting)
                {
                    _state = ConnectionState.Disconnected;
                    wasConnectedAndNeedsRetry =
                        true; // It was connected or trying to, and now it's not, and we're not disposing.
                }
                // If already Disconnected or Retrying, no state change needed here, just cleanup resources.
            }

            // Perform cleanup outside the lock
            if (sessionCtsToCleanup != null)
            {
                if (!sessionCtsToCleanup.IsCancellationRequested)
                {
                    try
                    {
                        sessionCtsToCleanup.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }

                sessionCtsToCleanup.Dispose();
            }

            if (socketToDispose != null)
            {
                Log.Debug("ReplicaConnection[{Endpoint}]: Disposing socket {SocketHandle} from session end.", EndPoint,
                    socketToDispose.Handle);
                try
                {
                    socketToDispose.Shutdown(SocketShutdown.Both);
                }
                catch
                {
                    /* Ignored */
                }

                socketToDispose.Dispose();
            }

            if (wasConnectedAtAll) // Notify parent only if it was ever considered connected in this session
            {
                _parentClient.NotifyConnectionStatusChanged(this, false);
            }

            if (!mainLoopIsExiting && !disposalToken.IsCancellationRequested && wasConnectedAndNeedsRetry)
            {
                ScheduleRetry(disposalToken);
            }

            return Task.CompletedTask;
        }

        private void ScheduleRetry(CancellationToken overallDisposalToken)
        {
            lock (_stateLock)
            {
                if (_state == ConnectionState.Disposed || overallDisposalToken.IsCancellationRequested) return;

                _retryAttempts++;
                _state = ConnectionState.Retrying;
                var delayMs = Math.Min(
                    _parentClient._initialConnectRetryDelay.TotalMilliseconds *
                    Math.Pow(1.5,
                        Math.Min(_retryAttempts,
                            8)), // Max 8 attempts for backoff calculation to avoid overly long delays
                    _parentClient._maxConnectRetryDelay.TotalMilliseconds
                );
                var jitter = delayMs * 0.2 * (Random.Shared.NextDouble() * 2 - 1); // +/- 20% jitter
                var finalDelay = TimeSpan.FromMilliseconds(Math.Max(200, delayMs + jitter)); // Ensure a minimum delay

                Log.Debug("ReplicaConnection[{Endpoint}]: Scheduling connection retry {Attempt} in {Delay}.",
                    EndPoint, _retryAttempts, finalDelay);

                _retryTimer?.Dispose();
                _retryTimer = new Timer(_ =>
                {
                    // Check the overallDisposalToken before changing state and allowing the main loop to retry
                    if (overallDisposalToken.IsCancellationRequested)
                    {
                        Log.Debug(
                            "ReplicaConnection[{Endpoint}]: Retry timer fired but instance is disposing. Aborting retry.",
                            EndPoint);
                        return;
                    }

                    lock (_stateLock)
                    {
                        // Only transition if still in Retrying state; could have been disposed in the meantime.
                        if (_state != ConnectionState.Retrying) return;
                        _state = ConnectionState.Disconnected;
                        Log.Debug(
                            "ReplicaConnection[{Endpoint}]: Retry timer fired. Transitioning to Disconnected to allow MainConnectionLoop to attempt connection.",
                            EndPoint);
                    }
                }, null, finalDelay, Timeout.InfiniteTimeSpan);
            }
        }

        public async Task<bool> TrySendAsync(ReadOnlyMemory<byte> data, CancellationToken requestToken)
        {
            Socket? socketForSend;
            CancellationToken sessionTokenForSend; // Token for this specific socket session

            lock (_stateLock)
            {
                if (_state != ConnectionState.Connected || _currentSocket == null || !_currentSocket.Connected ||
                    _currentSessionCts == null || _currentSessionCts.IsCancellationRequested)
                {
                    Log.Warning(
                        "ReplicaConnection[{Endpoint}]: Cannot send, not connected or session ending. State: {CurrentState}",
                        EndPoint, _state);
                    return false;
                }

                socketForSend = _currentSocket;
                sessionTokenForSend = _currentSessionCts.Token;
            }

            try
            {
                // Link the session token, request token, and overall instance disposal token
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(sessionTokenForSend, requestToken,
                    _instanceDisposalCts.Token);
                await socketForSend.SendAsync(data, SocketFlags.None, linkedCts.Token).ConfigureAwait(false);
                Log.Verbose("ReplicaConnection[{Endpoint}]: Sent {BytesCount} bytes via socket {SocketHandle}.",
                    EndPoint, data.Length, socketForSend.Handle);
                return true;
            }
            catch (OperationCanceledException ex)
            {
                // Check which token caused the cancellation
                if (requestToken.IsCancellationRequested)
                    Log.Warning(
                        "ReplicaConnection[{Endpoint}]: Send cancelled by requestToken for socket {SocketHandle}.",
                        EndPoint, socketForSend.Handle);
                else if (_instanceDisposalCts.IsCancellationRequested)
                    Log.Warning(
                        "ReplicaConnection[{Endpoint}]: Send cancelled by instanceDisposalCts for socket {SocketHandle}.",
                        EndPoint, socketForSend.Handle);
                else if (sessionTokenForSend.IsCancellationRequested) // This implies the session was already ending
                    Log.Warning(
                        "ReplicaConnection[{Endpoint}]: Send cancelled by sessionTokenForSend for socket {SocketHandle}.",
                        EndPoint, socketForSend.Handle);
                else
                    Log.Warning(ex,
                        "ReplicaConnection[{Endpoint}]: Send operation cancelled for socket {SocketHandle}.", EndPoint,
                        socketForSend.Handle);

                // If cancellation was due to session or instance disposal, the connection is already being handled.
                // If it was the requestToken, we just rethrow.
                if (requestToken.IsCancellationRequested) throw;
                return false; // Indicate send failure if due to session/instance cancellation
            }
            catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException)
            {
                Log.Warning(ex,
                    "ReplicaConnection[{Endpoint}]: Send failed on socket {SocketHandle}, connection likely lost.",
                    EndPoint, socketForSend.Handle);
                // If the session isn't already being torn down by its own CTS, initiate closure.
                if (!sessionTokenForSend.IsCancellationRequested && !_instanceDisposalCts.IsCancellationRequested)
                {
                    _ = HandleSocketSessionEndedAsync(ex, _instanceDisposalCts.Token);
                }

                return false;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Task? mainLoopTaskToAwait;
            Timer? timerToDispose;
            CancellationTokenSource? sessionCtsToDispose;
            Socket? socketToDispose;

            lock (_stateLock)
            {
                if (_state == ConnectionState.Disposed) return;
                Log.Information("ReplicaConnection[{Endpoint}]: DisposeAsync called.", EndPoint);
                _state = ConnectionState.Disposing; // Mark as disposing first

                mainLoopTaskToAwait = _mainLoopTask;
                timerToDispose = _retryTimer;
                _retryTimer = null;
                sessionCtsToDispose = _currentSessionCts;
                _currentSessionCts = null;
                socketToDispose = _currentSocket;
                _currentSocket = null;
            }

            // 1. Signal the main instance CTS. This will cause MainConnectionLoopAsync to exit.
            if (!_instanceDisposalCts.IsCancellationRequested)
            {
                try
                {
                    _instanceDisposalCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            // 2. Dispose timer immediately
            timerToDispose?.Dispose();

            // 3. Clean up resources from the *last active session* if any.
            //    MainConnectionLoopAsync's finally block will also call HandleSocketSessionEndedAsync,
            //    but this ensures prompt cleanup if DisposeAsync is called while a session is active.
            if (sessionCtsToDispose != null)
            {
                if (!sessionCtsToDispose.IsCancellationRequested)
                    try
                    {
                        sessionCtsToDispose.Cancel();
                    }
                    catch
                    {
                        // ignored
                    }

                sessionCtsToDispose.Dispose();
            }

            if (socketToDispose != null)
            {
                try
                {
                    socketToDispose.Shutdown(SocketShutdown.Both);
                }
                catch
                {
                    // ignored
                }

                socketToDispose.Dispose();
            }

            // 4. Wait for the main loop to complete.
            try
            {
                if (!mainLoopTaskToAwait.IsCompleted)
                {
                    // Give it a reasonable time to unwind.
                    await Task.WhenAny(mainLoopTaskToAwait, Task.Delay(TimeSpan.FromSeconds(3))).ConfigureAwait(false);
                    if (!mainLoopTaskToAwait.IsCompleted)
                    {
                        Log.Warning(
                            "ReplicaConnection[{Endpoint}]: Main connection loop did not complete promptly during disposal.",
                            EndPoint);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "ReplicaConnection[{Endpoint}]: Exception during main loop task await on dispose.",
                    EndPoint);
            }

            // 5. Dispose the main CTS for this instance.
            _instanceDisposalCts.Dispose();

            lock (_stateLock)
            {
                _state = ConnectionState.Disposed;
            } // Final state set

            _parentClient.NotifyConnectionStatusChanged(this, false); // Final notification
            Log.Information("ReplicaConnection[{Endpoint}]: Disposed.", EndPoint);
            GC.SuppressFinalize(this);
        }
    }

    private readonly List<IPEndPoint> _replicaEndpoints;
    private readonly ConcurrentDictionary<IPEndPoint, ReplicaConnection> _replicaConnections = new();
    private readonly VsrMessageSerializer _serializer = new();
    private readonly MemoryPool<byte> _memoryPool;
    private readonly ConcurrentDictionary<uint, PendingRequest> _pendingRequests = new();
    private readonly CancellationTokenSource _clientLifetimeCts = new();
    private readonly UInt128 _clientId;
    private long _requestCounter = 0;
    private IPEndPoint? _currentPrimaryEndpoint;
    private readonly object _primaryLock = new();

    private readonly TimeSpan _connectTimeout = TimeSpan.FromSeconds(2);
    private readonly TimeSpan _requestTimeout = TimeSpan.FromSeconds(10);
    private readonly TimeSpan _initialConnectRetryDelay = TimeSpan.FromMilliseconds(500);
    private readonly TimeSpan _maxConnectRetryDelay = TimeSpan.FromSeconds(30);
    private const int MinBufferSegmentSize = 4096;

    public VsrClient(List<IPEndPoint> replicaEndpoints, MemoryPool<byte>? memoryPool = null)
    {
        _replicaEndpoints = replicaEndpoints ?? throw new ArgumentNullException(nameof(replicaEndpoints));
        _memoryPool = memoryPool ?? PinnedBlockMemoryPool.Shared; // Use provided or default
        _clientId = BinaryUtils.NewGuidUInt128(); // From VsrReplica.VsrCore.Utils
        Log.Information("VsrClient initialized with ID: {ClientId}", _clientId);

        foreach (var ep in _replicaEndpoints)
        {
            _replicaConnections[ep] = new ReplicaConnection(ep, this);
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("VsrClient: Starting connections to replicas...");
        // Start all replica connection managers
        foreach (var manager in _replicaConnections.Values)
        {
            manager.Start();
        }

        // Initial wait for at least one connection
        using var startupCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _clientLifetimeCts.Token);
        try
        {
            await WaitForFirstConnectionAsync(startupCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Log.Error("VsrClient: Startup cancelled or timed out waiting for initial connection.");
            throw;
        }

        UpdateCurrentPrimary(); // Select initial primary
        Log.Information("VsrClient: Started. Initial primary: {Primary}", _currentPrimaryEndpoint ?? (object)"None");
    }

    private async Task WaitForFirstConnectionAsync(CancellationToken token)
    {
        Log.Debug("VsrClient: Waiting for at least one replica connection...");
        while (!token.IsCancellationRequested)
        {
            if (_replicaConnections.Values.Any(rc => rc.IsEffectivelyConnected))
            {
                Log.Information("VsrClient: At least one replica connected.");
                return;
            }

            await Task.Delay(100, token).ConfigureAwait(false);
        }

        token.ThrowIfCancellationRequested();
    }


    internal void NotifyConnectionStatusChanged(ReplicaConnection connection, bool isConnected)
    {
        Log.Information("VsrClient: Connection to {Endpoint} status changed. Connected: {IsConnected}",
            connection.EndPoint, isConnected);
        lock (_primaryLock)
        {
            if (!isConnected && connection.EndPoint.Equals(_currentPrimaryEndpoint))
            {
                Log.Warning("VsrClient: Current primary {PrimaryEndpoint} disconnected.", _currentPrimaryEndpoint);
                _currentPrimaryEndpoint = null; // Clear primary, will pick new one on next request
                UpdateCurrentPrimary(); // Attempt to find a new one immediately
            }
            else if (isConnected && _currentPrimaryEndpoint == null)
            {
                UpdateCurrentPrimary(); // A connection came up, try to set a primary
            }
        }
    }

    private void UpdateCurrentPrimary(IPEndPoint? newPrimaryHint = null)
    {
        lock (_primaryLock)
        {
            if (newPrimaryHint != null && _replicaConnections.ContainsKey(newPrimaryHint) &&
                _replicaConnections[newPrimaryHint].IsEffectivelyConnected)
            {
                if (newPrimaryHint.Equals(_currentPrimaryEndpoint)) return;
                Log.Information("VsrClient: Updating primary to {NewPrimary} based on hint.", newPrimaryHint);
                _currentPrimaryEndpoint = newPrimaryHint;

                return;
            }

            // If no hint or hint is not valid, try to pick one
            var preferredOrder = _replicaEndpoints; // Use original order as a simple preference
            var newPrimary = preferredOrder.FirstOrDefault(ep =>
                _replicaConnections.TryGetValue(ep, out var rc) && rc.IsEffectivelyConnected);

            if (newPrimary != null && !newPrimary.Equals(_currentPrimaryEndpoint))
            {
                Log.Information("VsrClient: Selecting new primary: {NewPrimary}. Old: {OldPrimary}", newPrimary,
                    _currentPrimaryEndpoint ?? (object)"None");
                _currentPrimaryEndpoint = newPrimary;
            }
            else if (newPrimary == null && _currentPrimaryEndpoint != null)
            {
                Log.Warning(
                    "VsrClient: No connected replicas found to select as primary. Clearing current primary {OldPrimary}.",
                    _currentPrimaryEndpoint);
                _currentPrimaryEndpoint = null;
            }
            else if (newPrimary == null && _currentPrimaryEndpoint == null)
            {
                Log.Debug("VsrClient: No connected replicas, no primary to select.");
            }
        }
    }


    private ReplicaConnection? GetTargetReplicaConnection()
    {
        lock (_primaryLock)
        {
            if (_currentPrimaryEndpoint != null &&
                _replicaConnections.TryGetValue(_currentPrimaryEndpoint, out var primaryConn) &&
                primaryConn.IsEffectivelyConnected)
            {
                return primaryConn;
            }

            // Fallback: try any connected replica
            foreach (var ep in _replicaEndpoints) // Iterate in preferred order
            {
                if (_replicaConnections.TryGetValue(ep, out var fallbackConn) && fallbackConn.IsEffectivelyConnected)
                {
                    Log.Warning("VsrClient: Primary {CurrentPrimary} not available. Using fallback {FallbackEndpoint}.",
                        _currentPrimaryEndpoint ?? (object)"None", fallbackConn.EndPoint);
                    _currentPrimaryEndpoint = fallbackConn.EndPoint; // Tentatively set new primary
                    return fallbackConn;
                }
            }
        }

        Log.Error("VsrClient: No active replica connection available to send request.");
        return null;
    }

    private bool TryParseMessageFromBuffer(ref ReadOnlySequence<byte> buffer, out VsrMessage? message,
        out IMemoryOwner<byte>? owner)
    {
        message = null;
        owner = null;

        if (!_serializer.TryReadMessageSize(buffer, out int messageSize)) return false;
        if (messageSize <= 0 || messageSize > (GlobalConfig.HeaderSize + 10 * 1024 * 1024)) // Max 10MB payload sanity
        {
            Log.Error("VsrClient: Invalid message size {MessageSize} from replica. Corrupt data?", messageSize);
            throw new InvalidDataException($"Invalid message size from replica: {messageSize}");
        }

        if (buffer.Length < messageSize) return false;

        var messageSequence = buffer.Slice(0, messageSize);
        var rentedOwner = _memoryPool.Rent(messageSize);
        try
        {
            messageSequence.CopyTo(rentedOwner.Memory.Span[..messageSize]);
            message = _serializer.Deserialize(rentedOwner.Memory[..messageSize], rentedOwner);
            owner = rentedOwner; // Transfer ownership
            buffer = buffer.Slice(messageSize);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "VsrClient: Failed to deserialize message from replica.");
            rentedOwner.Dispose(); // Critical: dispose if we can't pass ownership
            throw; // Re-throw to signal pipe processing error
        }
    }

    private void HandleReceivedMessage(VsrMessage message, IPEndPoint sourceEndpoint)
    {
        var handled = false;
        if (message.Header.Client == _clientId)
        {
            if (message.Header.Command == Command.Reply || message.Header.Command == Command.Pong)
            {
                if (_pendingRequests.TryRemove(message.Header.Request, out var pending))
                {
                    Log.Debug(
                        "VsrClient: Received {Command} for RequestId {RequestId} from {SourceEndpoint} (Replica {ReplicaId}).",
                        message.Header.Command, message.Header.Request, sourceEndpoint, message.Header.Replica);

                    // If this reply came from a replica that wasn't our current primary, update our primary hint.
                    if (message.Header.Command == Command.Reply)
                    {
                        // Only trust REPLIES for primary indication
                        lock (_primaryLock)
                        {
                            if (_currentPrimaryEndpoint == null || !_currentPrimaryEndpoint.Equals(sourceEndpoint))
                            {
                                Log.Information(
                                    "VsrClient: Reply from {SourceEndpoint} (Replica {ReplicaId}) suggests it's the primary. Updating primary hint.",
                                    sourceEndpoint, message.Header.Replica);
                                UpdateCurrentPrimary(sourceEndpoint);
                            }
                        }
                    }

                    pending.CtsReg.Unregister();
                    pending.Tcs.TrySetResult(message); // Transfers ownership of message
                    handled = true;
                }
                else
                {
                    Log.Warning(
                        "VsrClient: Received {Command} for unknown/completed RequestId {RequestId} from {SourceEndpoint}.",
                        message.Header.Command, message.Header.Request, sourceEndpoint);
                }
            }
        }

        if (handled) return;
        Log.Debug("VsrClient: Received unhandled/irrelevant message: {Command} from {SourceEndpoint}.",
            message.Header.Command, sourceEndpoint);
        message.Dispose(); // Dispose if not handled by a TCS
    }

    private async Task<VsrMessage> SendRequestInternalAsync(VsrHeader header, ReadOnlyMemory<byte> payload,
        CancellationToken externalToken)
    {
        uint requestId = (uint)Interlocked.Increment(ref _requestCounter);
        header.Request = requestId;
        header.Client = _clientId;

        // The VsrMessage created here does not own the payload memory,
        // as the payload comes from an external source (e.g., Encoding.UTF8.GetBytes).
        // The VsrMessage created from a reply *will* own its memory via IMemoryOwner.
        var messageToSend = new VsrMessage(header, MemoryMarshal.AsMemory(payload));

        var tcs = new TaskCompletionSource<VsrMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Setup combined cancellation for the request's lifetime
        using var requestTimeoutCts = new CancellationTokenSource(_requestTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            _clientLifetimeCts.Token, // Overall client lifetime
            requestTimeoutCts.Token, // Specific request timeout
            externalToken // External cancellation from the caller
        );

        CancellationTokenRegistration ctsRegistration = default;
        bool requestAddedToPending = false;

        try
        {
            // Register the cancellation callback *before* adding to _pendingRequests
            // to handle race conditions where cancellation might occur immediately.
            ctsRegistration = linkedCts.Token.Register(() =>
            {
                // This callback is executed when linkedCts.Token is cancelled.
                if (_pendingRequests.TryRemove(requestId, out var pendingToCancel))
                {
                    string reason = requestTimeoutCts.IsCancellationRequested ? "Request timed out."
                        : externalToken.IsCancellationRequested ? "Operation cancelled by caller."
                        : _clientLifetimeCts.IsCancellationRequested ? "Client shutting down."
                        : "Request cancelled by an internal mechanism.";
                    Log.Warning("VsrClient: RequestId {RequestId} cancellation triggered. Reason: {Reason}", requestId,
                        reason);
                    pendingToCancel.Tcs.TrySetCanceled(linkedCts.Token); // Use the token that caused cancellation
                    // The CtsReg inside pendingToCancel will be unregistered when the PendingRequest is removed.
                }
            });

            if (!_pendingRequests.TryAdd(requestId, new PendingRequest(tcs, ctsRegistration)))
            {
                // If TryAdd fails, it means the requestId was somehow already in use (highly unlikely with Interlocked).
                // Or, more likely, if cancellation happened *extremely* quickly and the callback already removed it.
                // In any case, the ctsRegistration for *this attempt* needs to be cleaned up if it's not going to be managed by the dictionary.
                await ctsRegistration.DisposeAsync(); // Dispose the registration as it's not stored
                Log.Error(
                    "VsrClient: Failed to add pending request (duplicate ID or immediate cancellation?): {RequestId}",
                    requestId);
                throw new InvalidOperationException($"Failed to register pending request {requestId}.");
            }

            requestAddedToPending = true; // Mark that it was successfully added

            Log.Debug("VsrClient: Sending RequestId {RequestId}, Command: {Command}, Op: {Operation}", requestId,
                header.Command, header.Operation);

            // Serialize message (rents buffer from pool)
            // This using statement ensures serializedMessage and its owned memory are disposed.
            using var serializedMessage = VsrMessageSerializer.SerializeMessage(messageToSend, _memoryPool);

            int attempts = 0;
            // Allow retrying on different replicas if the first one fails due to connection issues.
            int maxAttempts =
                _replicaConnections.Count > 0 ? _replicaConnections.Count + 1 : 1; // Try each + 1 for good measure

            while (attempts < maxAttempts && !linkedCts.Token.IsCancellationRequested)
            {
                var connection = GetTargetReplicaConnection(); // Gets current primary or a fallback
                if (connection == null)
                {
                    Log.Warning(
                        "VsrClient: No connection available for attempt {Attempt} for ReqId {RequestId}. Waiting briefly...",
                        attempts + 1, requestId);
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(200 + Random.Shared.Next(0, 100)), linkedCts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    } // Loop will exit due to linkedCts.Token.IsCancellationRequested

                    attempts++;
                    continue;
                }

                Log.Debug("VsrClient: Attempt {Attempt}/{MaxAttempts} sending ReqId {RequestId} to {Endpoint}",
                    attempts + 1, maxAttempts, requestId, connection.EndPoint);

                if (await connection.TrySendAsync(serializedMessage.Memory, linkedCts.Token).ConfigureAwait(false))
                {
                    // Send was successful (or at least didn't immediately throw/return false for a known closed state)
                    // Now wait for the TaskCompletionSource to be completed by the receive loop or cancelled by the linkedCts.
                    try
                    {
                        // Await the TCS. If linkedCts cancels, this await will throw OperationCanceledException.
                        VsrMessage result = await tcs.Task.ConfigureAwait(false);
                        // If we reach here, the TCS was completed successfully with a result.
                        // The CtsRegistration will be disposed when the request is removed from _pendingRequests (which happens before returning).
                        // No need to unregister here as it's part of the PendingRequest struct.
                        return result; // Ownership of the VsrMessage (and its buffer) is transferred to the caller.
                    }
                    catch (OperationCanceledException ex)
                    {
                        // This catch block is for when tcs.Task itself is cancelled.
                        // This would typically happen if linkedCts.Token caused the cancellation via the registered callback.
                        Log.Warning(ex,
                            "VsrClient: Wait for reply for ReqId {RequestId} was cancelled. Attempt: {Attempt}",
                            requestId, attempts + 1);
                        if (linkedCts.Token.IsCancellationRequested)
                        {
                            // If the overall cancellation (timeout, external, shutdown) triggered this, rethrow.
                            throw;
                        }

                        // If only the TCS was cancelled (e.g., by a connection-specific issue that triggered
                        // its cancellation callback, but linkedCts is NOT yet cancelled), we might want to retry
                        // on another replica if attempts remain.
                        Log.Warning(
                            "VsrClient: ReqId {RequestId} TCS cancelled, but overall request not yet. Possibly connection issue with {Endpoint}.",
                            requestId, connection.EndPoint);
                    }
                    catch (Exception ex)
                    {
                        // This catches other exceptions if tcs.TrySetException was called.
                        Log.Error(ex,
                            "VsrClient: Error awaiting reply for ReqId {RequestId} after sending to {Endpoint}. Attempt: {Attempt}",
                            requestId, connection.EndPoint);
                        // Allow loop to retry if attempts remain.
                    }
                }
                else
                {
                    Log.Warning(
                        "VsrClient: TrySendAsync failed for ReqId {RequestId} to {Endpoint}. Will try next replica if available. Attempt: {Attempt}",
                        requestId, connection.EndPoint, attempts + 1);
                }

                // If send failed or reply wait was cancelled/faulted for a specific connection attempt:
                lock (_primaryLock)
                {
                    _currentPrimaryEndpoint = null;
                } // Force re-selection of primary

                UpdateCurrentPrimary(); // Attempt to find a new primary
                attempts++;

                if (attempts < maxAttempts && !linkedCts.Token.IsCancellationRequested)
                {
                    Log.Debug("VsrClient: Delaying before retry attempt {NextAttempt} for ReqId {RequestId}",
                        attempts + 1, requestId);
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100 + Random.Shared.Next(0, 150)), linkedCts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    } // Exit loop if cancelled during delay
                }
            } // End of while loop

            // If we exit the loop, it's either due to overall cancellation or all attempts exhausted.
            // The CtsRegistration should have already run if linkedCts was cancelled.
            // We still need to ensure the request is removed from _pendingRequests if it wasn't already.

            // Final cleanup and exception determination:
            if (_pendingRequests.TryRemove(requestId, out var finalPending))
            {
                // If we removed it here, it means the cancellation callback might not have run or didn't remove it.
                // Or, it means the loop exhausted attempts before the TCS was ever set.
                await finalPending.CtsReg.DisposeAsync(); // Ensure the registration is disposed.
            }


            // Determine the final exception to throw based on the state.
            if (linkedCts.Token.IsCancellationRequested)
            {
                string reason = requestTimeoutCts.IsCancellationRequested ? "Request timed out."
                    : externalToken.IsCancellationRequested ? "Operation cancelled by caller."
                    : _clientLifetimeCts.IsCancellationRequested ? "Client shutting down."
                    : "Request cancelled by an internal mechanism (e.g., all connections failed during attempts).";
                Log.Warning(
                    "VsrClient: RequestId {RequestId} definitively cancelled after all attempts or due to overall cancellation. Reason: {Reason}",
                    requestId, reason);
                throw new OperationCanceledException(reason, linkedCts.Token);
            }

            // If linkedCts was NOT cancelled, but the TCS is in a faulted/cancelled state (e.g., from a specific connection issue that wasn't retried)
            // This path should be less common if the loop retries on connection-specific issues.
            if (tcs.Task.IsFaulted)
            {
                Log.Error(tcs.Task.Exception?.GetBaseException(),
                    "VsrClient: RequestId {RequestId} TCS is faulted after loop completion.", requestId);
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(tcs.Task.Exception!.GetBaseException())
                    .Throw();
            }

            if (tcs.Task.IsCanceled) // Check after IsFaulted
            {
                Log.Warning(
                    "VsrClient: RequestId {RequestId} TCS was cancelled after loop completion, but overall request token was not.",
                    requestId);
                // This could happen if a connection-specific cancellation occurred on the last attempt,
                // and the linkedCts wasn't the source of that specific cancellation.
                throw new OperationCanceledException(
                    $"Request {requestId} was canceled, possibly due to a connection issue on the final attempt.");
            }

            // If none of the above, it implies all attempts were exhausted, and the TCS is still waiting.
            Log.Error(
                "VsrClient: RequestId {RequestId} failed after {Attempts} attempts. Final TCS Status: {TcsStatus}. No overall cancellation detected.",
                requestId, attempts, tcs.Task.Status);
            throw new TimeoutException(
                $"VsrClient: Failed to send request {requestId} and receive a reply after {attempts} attempts. All connections/retries failed. Final TCS Status: {tcs.Task.Status}.");
        }
        finally
        {
            // This block ensures that if an exception is thrown *before* requestAddedToPending is true,
            // or if an exception bypasses the main try-catch within the loop,
            // the CancellationTokenRegistration is disposed if it was created.
            if (!requestAddedToPending && ctsRegistration != default)
            {
                await ctsRegistration.DisposeAsync();
            }
            // If requestAddedToPending is true, the CtsReg is part of the PendingRequest struct
            // and will be handled when the item is removed from _pendingRequests or during the final cleanup.
        }
    }


    public async Task<string> SetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        Log.Information("VsrClient: Sending SET request: Key={Key}", key);
        var payload = Encoding.UTF8.GetBytes($"{key}:{value}");
        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Set, GlobalConfig.CurrentVersion);
        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken).ConfigureAwait(false);
            var replyPayload = reply.Payload.Length > 0 ? Encoding.UTF8.GetString(reply.Payload.Span) : "[No Payload]";
            Log.Information("VsrClient: Received SET reply: {ReplyPayload} from Replica {ReplicaId}", replyPayload,
                reply.Header.Replica);
            return $"Set OK. Reply from {reply.Header.Replica}: {replyPayload}";
        }
        finally
        {
            reply?.Dispose();
        }
    }

    public async Task<string> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Log.Information("VsrClient: Sending GET request: Key={Key}", key);
        var payload = Encoding.UTF8.GetBytes(key);
        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Get, GlobalConfig.CurrentVersion);
        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken).ConfigureAwait(false);
            var valueStr = reply.Payload.Length > 0
                ? Encoding.UTF8.GetString(reply.Payload.Span)
                : "[Not Found or Empty]";
            Log.Information("VsrClient: Received GET reply: Value={Value} from Replica {ReplicaId}", valueStr,
                reply.Header.Replica);
            return valueStr;
        }
        finally
        {
            reply?.Dispose();
        }
    }

    public async Task<string> UpdateAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        Log.Information("VsrClient: Sending UPDATE request: Key={Key}", key);
        var payload = Encoding.UTF8.GetBytes($"{key}:{value}");
        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Update, GlobalConfig.CurrentVersion);
        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken).ConfigureAwait(false);
            var replyPayload = reply.Payload.Length > 0 ? Encoding.UTF8.GetString(reply.Payload.Span) : "[No Payload]";
            Log.Information("VsrClient: Received UPDATE reply: {ReplyPayload} from Replica {ReplicaId}", replyPayload,
                reply.Header.Replica);
            return $"Update OK. Reply from {reply.Header.Replica}: {replyPayload}";
        }
        finally
        {
            reply?.Dispose();
        }
    }

    public async Task PingAllAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("VsrClient: Pinging all configured replicas...");
        var pingTasks = new List<Task>();
        uint baseRequestId =
            (uint)Interlocked.Add(ref _requestCounter, _replicaConnections.Count); // Reserve a block of request IDs

        int i = 0;
        foreach (var connection in _replicaConnections.Values)
        {
            uint currentRequestId = baseRequestId - (uint)i;
            i++;
            pingTasks.Add(SendSinglePingAsync(connection, currentRequestId, cancellationToken));
        }

        await Task.WhenAll(pingTasks).ConfigureAwait(false);
        Log.Information("VsrClient: Ping sequence completed.");
    }

    private async Task SendSinglePingAsync(ReplicaConnection connection, uint requestId,
        CancellationToken externalToken)
    {
        var header = new VsrHeader(0, _clientId, 0, 0, requestId, 0, 0, 0, 0, 0, 0, 0, Command.Ping, Operation.Reserved,
            GlobalConfig.CurrentVersion);
        var message = new VsrMessage(header, Memory<byte>.Empty);

        var tcs = new TaskCompletionSource<VsrMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2)); // Shorter timeout for pings
        using var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(_clientLifetimeCts.Token, timeoutCts.Token, externalToken);

        var ctsRegistration = linkedCts.Token.Register(() =>
        {
            if (_pendingRequests.TryRemove(requestId, out var p)) p.Tcs.TrySetCanceled(linkedCts.Token);
        });

        if (!_pendingRequests.TryAdd(requestId, new PendingRequest(tcs, ctsRegistration)))
        {
            ctsRegistration.Unregister();
            Log.Error("VsrClient: Failed to add pending ping request (duplicate ID?): {RequestId} to {Endpoint}",
                requestId, connection.EndPoint);
            return;
        }

        Log.Debug("VsrClient: Sending Ping RequestId {RequestId} to {Endpoint}", requestId, connection.EndPoint);
        using var serializedMessage = VsrMessageSerializer.SerializeMessage(message, _memoryPool);
        VsrMessage? pongMessage = null;
        try
        {
            if (await connection.TrySendAsync(serializedMessage.Memory, linkedCts.Token).ConfigureAwait(false))
            {
                pongMessage = await tcs.Task.ConfigureAwait(false); // Will throw if cancelled/faulted by linkedCts
                Log.Information(
                    "VsrClient: Received Pong from {Endpoint} (Replica {ReplicaId}) for RequestId {RequestId}",
                    connection.EndPoint, pongMessage.Header.Replica, requestId);
            }
            else
            {
                Log.Warning("VsrClient: Ping to {Endpoint} (ReqId {RequestId}) failed at send.", connection.EndPoint,
                    requestId);
            }
        }
        catch (OperationCanceledException)
        {
            Log.Warning("VsrClient: Ping to {Endpoint} (ReqId {RequestId}) timed out or was cancelled.",
                connection.EndPoint, requestId);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "VsrClient: Error during Ping to {Endpoint} (ReqId {RequestId})", connection.EndPoint,
                requestId);
        }
        finally
        {
            ctsRegistration.Unregister(); // Ensure unregistration
            _pendingRequests.TryRemove(requestId, out _); // Clean up
            pongMessage?.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        Log.Information("VsrClient: Disposing...");
        _clientLifetimeCts.Cancel();

        var disposeTasks = _replicaConnections.Values.Select(connMgr => connMgr.DisposeAsync().AsTask()).ToList();
        await Task.WhenAll(disposeTasks).ConfigureAwait(false);

        foreach (var pending in _pendingRequests.Values)
        {
            pending.CtsReg.Unregister();
            pending.Tcs.TrySetException(new ObjectDisposedException("VsrClient is disposing."));
        }

        _pendingRequests.Clear();
        _replicaConnections.Clear();
        _clientLifetimeCts.Dispose();
        Log.Information("VsrClient: Disposed.");
        GC.SuppressFinalize(this);
    }
}