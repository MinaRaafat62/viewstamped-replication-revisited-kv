using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Serilog;
using VsrReplica.VsrCore;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.Utils;

namespace VsrClient;

public class VsrClient : IAsyncDisposable
{
    private class ClientConnection : IAsyncDisposable
    {
        public IPEndPoint EndPoint { get; }
        public Socket Socket { get; }
        private readonly CancellationTokenSource _connectionCts;
        private readonly Func<ClientConnection, Task> _receiveLoopFactory;
        private readonly Func<IPEndPoint, ValueTask> _onDisconnect;
        private Task? _receiveTask;
        private volatile bool _isDisposed;

        public ClientConnection(IPEndPoint endPoint, Socket socket, Func<ClientConnection, Task> receiveLoopFactory,
            Func<IPEndPoint, ValueTask> onDisconnect)
        {
            EndPoint = endPoint;
            Socket = socket;
            _connectionCts = new CancellationTokenSource();
            _receiveLoopFactory = receiveLoopFactory;
            _onDisconnect = onDisconnect;
        }

        public CancellationToken Token => _connectionCts.Token;

        public void StartReceiving()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(ClientConnection));
            _receiveTask = _receiveLoopFactory(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            Log.Debug("ClientConnection[{Endpoint}]: Disposing.", EndPoint);
            if (!_connectionCts.IsCancellationRequested)
            {
                try
                {
                    _connectionCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            // Close socket first to interrupt ReceiveAsync
            try
            {
                Socket.Shutdown(SocketShutdown.Both);
            }
            catch
            {
                /* Ignore */
            }

            try
            {
                Socket.Close();
            }
            catch
            {
                /* Ignore */
            }

            Socket.Dispose();

            if (_receiveTask != null)
            {
                try
                {
                    // Give the receive loop a chance to exit cleanly after cancellation/socket closure
                    await Task.WhenAny(_receiveTask, Task.Delay(TimeSpan.FromSeconds(2)));
                    if (!_receiveTask.IsCompleted)
                    {
                        Log.Warning(
                            "ClientConnection[{Endpoint}]: Receive loop did not complete quickly during disposal.",
                            EndPoint);
                    }
                }
                catch (Exception ex)
                {
                    Log.Warning(ex, "ClientConnection[{Endpoint}]: Exception waiting for receive loop during disposal.",
                        EndPoint);
                }
            }

            _connectionCts.Dispose();
            await _onDisconnect(EndPoint); // Notify manager
            Log.Information("ClientConnection[{Endpoint}]: Disposed.", EndPoint);
        }
    }

    private readonly List<IPEndPoint> _replicaEndpoints;
    private readonly ConcurrentDictionary<IPEndPoint, ClientConnection> _connections = new();
    private readonly VsrMessageSerializer _serializer = new();
    private readonly MemoryPool<byte> _memoryPool = MemoryPool<byte>.Shared; // Use shared pool or your custom one
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<VsrMessage>> _pendingRequests = new();
    private readonly CancellationTokenSource _clientLifetimeCts = new();
    private readonly UInt128 _clientId;
    private long _requestCounter = 0; // Use long for Interlocked
    private IPEndPoint? _primaryReplicaEndpoint;

    private readonly TimeSpan _connectTimeout = TimeSpan.FromSeconds(1);
    private readonly TimeSpan _requestTimeout = TimeSpan.FromSeconds(2);
    private const int MinBufferSegmentSize = 4096; // For pipe reading

    public VsrClient(List<IPEndPoint> replicaEndpoints)
    {
        _replicaEndpoints = replicaEndpoints ?? throw new ArgumentNullException(nameof(replicaEndpoints));
        _clientId = GenerateClientId();
        Log.Information("VsrClient initialized with ID: {ClientId}", _clientId);
    }

    private static UInt128 GenerateClientId()
    {
        Span<byte> idBytes = stackalloc byte[16];
        Guid.NewGuid().TryWriteBytes(idBytes);
        return BinaryUtils.BytesToUInt128BigEndian(idBytes);
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("Attempting to connect to replicas...");
        using var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _clientLifetimeCts.Token);
        var connectTasks = _replicaEndpoints.Select(ep => ConnectToReplicaAsync(ep, linkedCts.Token));
        await Task.WhenAll(connectTasks);

        if (_connections.IsEmpty)
        {
            Log.Error("Failed to connect to any replica.");
            throw new InvalidOperationException("Could not connect to any VSR replica.");
        }

        UpdatePrimary(); // Select initial primary
        Log.Information("Connected to {Count} replicas. Primary set to: {Primary}", _connections.Count,
            _primaryReplicaEndpoint ?? (object)"None");
    }

    private async Task ConnectToReplicaAsync(IPEndPoint endpoint, CancellationToken cancellationToken)
    {
        Log.Debug("Attempting connection to {Endpoint}", endpoint);
        Socket? socket = null;
        ClientConnection? connection = null;
        try
        {
            socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            using var timeoutCts = new CancellationTokenSource(_connectTimeout);
            using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await socket.ConnectAsync(endpoint, connectCts.Token);
            Log.Information("Successfully connected to {Endpoint}", endpoint);

            connection = new ClientConnection(endpoint, socket, ReceiveLoopAsync, HandleDisconnectAsync);

            if (_connections.TryAdd(endpoint, connection))
            {
                connection.StartReceiving(); // Fire-and-forget receive loop
            }
            else
            {
                Log.Warning("Failed to add connection {Endpoint} to dictionary (race condition?). Disposing.",
                    endpoint);
                await connection.DisposeAsync(); // Dispose the newly created but unused connection
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.Information("Connection attempt to {Endpoint} cancelled.", endpoint);
            socket?.Dispose();
            await (connection?.DisposeAsync() ?? ValueTask.CompletedTask);
        }
        catch (OperationCanceledException) // Catches the timeoutCts cancellation
        {
            Log.Warning("Connection attempt to {Endpoint} timed out after {Timeout}.", endpoint, _connectTimeout);
            socket?.Dispose();
            await (connection?.DisposeAsync() ?? ValueTask.CompletedTask);
        }
        catch (SocketException ex)
        {
            Log.Error(ex, "Failed to connect to {Endpoint}", endpoint);
            socket?.Dispose();
            await (connection?.DisposeAsync() ?? ValueTask.CompletedTask);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Unexpected error connecting to {Endpoint}", endpoint);
            socket?.Dispose();
            await (connection?.DisposeAsync() ?? ValueTask.CompletedTask);
        }
    }

    private async Task ReceiveLoopAsync(ClientConnection connection)
    {
        Log.Debug("ClientConnection[{Endpoint}]: Starting ReceiveLoop.", connection.EndPoint);
        var pipe = new Pipe(new PipeOptions(pool: _memoryPool, readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.Inline, useSynchronizationContext: false));
        Task fillPipeTask = FillPipeFromSocketAsync(connection, pipe.Writer);
        Task processPipeTask = ProcessPipeMessagesAsync(connection, pipe.Reader);

        try
        {
            // Wait for either task to complete (normally or with error)
            await Task.WhenAny(fillPipeTask, processPipeTask);
        }
        catch (Exception ex) // Should not happen if tasks handle their exceptions, but as safeguard
        {
            Log.Error(ex, "ClientConnection[{Endpoint}]: Unexpected error in ReceiveLoop supervisor.",
                connection.EndPoint);
        }
        finally
        {
            Log.Debug("ClientConnection[{Endpoint}]: ReceiveLoop finishing.", connection.EndPoint);
            // Ensure disposal is triggered if not already happening
            _ = connection.DisposeAsync();
        }
    }

    private async Task FillPipeFromSocketAsync(ClientConnection connection, PipeWriter writer)
    {
        Exception? error = null;
        try
        {
            while (!connection.Token.IsCancellationRequested)
            {
                Memory<byte> memory = writer.GetMemory(MinBufferSegmentSize);
                int bytesRead = await connection.Socket.ReceiveAsync(memory, SocketFlags.None, connection.Token);

                if (bytesRead == 0)
                {
                    Log.Information("ClientConnection[{Endpoint}]: Socket closed gracefully by remote peer.",
                        connection.EndPoint);
                    break; // Connection closed
                }

                writer.Advance(bytesRead);
                FlushResult result = await writer.FlushAsync(connection.Token);
                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch (OperationCanceledException)
        {
            Log.Debug("ClientConnection[{Endpoint}]: FillPipe loop cancelled.", connection.EndPoint);
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset ||
                                         ex.SocketErrorCode == SocketError.ConnectionAborted ||
                                         ex.SocketErrorCode == SocketError.Shutdown)
        {
            Log.Warning("ClientConnection[{Endpoint}]: Socket connection closed ({SocketErrorCode}).",
                connection.EndPoint, ex.SocketErrorCode);
            error = ex;
        }
        catch (ObjectDisposedException)
        {
            Log.Debug("ClientConnection[{Endpoint}]: FillPipe loop stopped due to object disposal.",
                connection.EndPoint);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ClientConnection[{Endpoint}]: Error in FillPipe loop.", connection.EndPoint);
            error = ex;
        }
        finally
        {
            await writer.CompleteAsync(error);
            Log.Debug("ClientConnection[{Endpoint}]: FillPipe loop finished.", connection.EndPoint);
        }
    }

    private async Task ProcessPipeMessagesAsync(ClientConnection connection, PipeReader reader)
    {
        Exception? error = null;
        try
        {
            while (!connection.Token.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(connection.Token);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (TryParseMessage(ref buffer, out VsrMessage? message, out IMemoryOwner<byte>? owner))
                {
                    if (message != null)
                    {
                        HandleReceivedMessage(
                            message); // HandleReceivedMessage is responsible for disposing the message/owner
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted || result.IsCanceled) break;
            }
        }
        catch (OperationCanceledException)
        {
            Log.Debug("ClientConnection[{Endpoint}]: ProcessPipe loop cancelled.", connection.EndPoint);
        }
        catch (InvalidDataException dataEx)
        {
            Log.Error(dataEx, "ClientConnection[{Endpoint}]: Invalid data in ProcessPipe loop.", connection.EndPoint);
            error = dataEx;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "ClientConnection[{Endpoint}]: Error in ProcessPipe loop.", connection.EndPoint);
            error = ex;
        }
        finally
        {
            await reader.CompleteAsync(error);
            Log.Debug("ClientConnection[{Endpoint}]: ProcessPipe loop finished.", connection.EndPoint);
        }
    }

    private bool TryParseMessage(ref ReadOnlySequence<byte> buffer, out VsrMessage? message,
        out IMemoryOwner<byte>? owner)
    {
        message = null;
        owner = null;

        if (!_serializer.TryReadMessageSize(buffer, out int messageSize)) return false;
        if (messageSize <= 0 ||
            messageSize > (GlobalConfig.HeaderSize + 16 * 1024 * 1024)) // Sanity check max payload size
        {
            Log.Error("Invalid message size detected: {Size}. Corrupt data?", messageSize);
            throw new InvalidDataException($"Invalid message size: {messageSize}");
        }

        if (buffer.Length < messageSize) return false;

        var messageSequence = buffer.Slice(0, messageSize);
        IMemoryOwner<byte> rentedOwner = _memoryPool.Rent(messageSize); // Rent exact size
        try
        {
            messageSequence.CopyTo(rentedOwner.Memory.Span[..messageSize]);
            message = _serializer.Deserialize(rentedOwner.Memory[..messageSize], rentedOwner);
            owner = rentedOwner; // Transfer ownership out
            buffer = buffer.Slice(messageSize);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to deserialize message.");
            rentedOwner.Dispose(); // Dispose if deserialization failed
            throw new InvalidDataException("Failed to deserialize message.", ex);
        }
    }

    private void HandleReceivedMessage(VsrMessage message)
    {
        // Using a switch expression for clarity
        bool handled = message.Header.Command switch
        {
            Command.Reply when message.Header.Client == _clientId =>
                TryCompleteRequest(message.Header.Request, message),
            Command.Pong when message.Header.Client == _clientId => TryCompleteRequest(message.Header.Request, message),
            _ => false // Not a reply/pong for us
        };

        if (!handled)
        {
            Log.Debug("Received unhandled or irrelevant message: {Command}, Client: {Client}, Request: {Request}",
                message.Header.Command, message.Header.Client, message.Header.Request);
            message.Dispose(); // Dispose unhandled messages
        }
        // If handled, TryCompleteRequest transfers ownership to the TCS, which is responsible for disposal later.
    }

    private bool TryCompleteRequest(uint requestId, VsrMessage message)
    {
        if (_pendingRequests.TryRemove(requestId, out var tcs))
        {
            Log.Debug("Matching reply/pong found for RequestId: {RequestId}, Command: {Command}", requestId,
                message.Header.Command);
            // Transfer ownership of the message (and its buffer) to the TCS.
            // The caller awaiting the request will get the message and must dispose it.
            tcs.TrySetResult(message);
            return true;
        }
        else
        {
            Log.Warning(
                "Received reply/pong for unknown or already completed RequestId: {RequestId}, Command: {Command}",
                requestId, message.Header.Command);
            return false; // Indicate not handled (will be disposed by caller)
        }
    }

    private async ValueTask HandleDisconnectAsync(IPEndPoint endpoint)
    {
        Log.Warning("Connection to {Endpoint} disconnected.", endpoint);
        // Connection disposal is handled by ClientConnection itself, just remove from map
        _connections.TryRemove(endpoint, out _);

        if (_primaryReplicaEndpoint == endpoint)
        {
            Log.Warning("Primary replica {Endpoint} disconnected. Selecting new primary.", endpoint);
            UpdatePrimary();
            Log.Information("New primary replica set to: {Primary}", _primaryReplicaEndpoint ?? (object)"None");
        }

        // Fail any requests potentially waiting on this specific connection?
        // Current design sends to primary/fallback, so generic timeout handles this.
        // Reconnection logic could be added here.
        await ValueTask.CompletedTask;
    }

    private void UpdatePrimary()
    {
        // Simple strategy: pick the first available connection alphabetically by endpoint string
        _primaryReplicaEndpoint = _connections.Keys
            .OrderBy(ep => ep.ToString())
            .FirstOrDefault();
    }

    private ClientConnection? GetTargetConnection()
    {
        if (_primaryReplicaEndpoint != null && _connections.TryGetValue(_primaryReplicaEndpoint, out var primaryConn))
        {
            return primaryConn;
        }

        // Fallback if primary is down or not set
        var fallback = _connections.Values.FirstOrDefault(); // Simple fallback
        if (fallback != null && _primaryReplicaEndpoint != fallback.EndPoint)
        {
            Log.Warning("Primary replica not available or not found. Sending request via fallback {Endpoint}",
                fallback.EndPoint);
            _primaryReplicaEndpoint = fallback.EndPoint; // Tentatively set new primary
        }
        else if (fallback == null)
        {
            Log.Error("No active connections available to send request.");
        }

        return fallback;
    }

    // Internal method for sending request and waiting for reply
    private async Task<VsrMessage> SendRequestInternalAsync(VsrHeader header, ReadOnlyMemory<byte> payload,
        CancellationToken externalToken = default)
    {
        var connection = GetTargetConnection();
        if (connection == null) throw new InvalidOperationException("No active replica connections.");

        uint requestId = (uint)Interlocked.Increment(ref _requestCounter);
        header.Request = requestId;
        header.Client = _clientId;

        // Create message - payload is NOT owned by this message object
        var message = new VsrMessage(header, MemoryMarshal.AsMemory(payload));

        var tcs = new TaskCompletionSource<VsrMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pendingRequests.TryAdd(requestId, tcs))
        {
            Log.Error("Failed to add pending request (duplicate ID?): {RequestId}", requestId);
            throw new InvalidOperationException("Failed to register pending request."); // Should be rare
        }

        Log.Debug("Sending RequestId: {RequestId}, Command: {Command}, Op: {Operation} to {Endpoint}",
            requestId, header.Command, header.Operation, connection.EndPoint);

        // Serialize message (rents buffer from pool)
        using var serializedMessage = VsrMessageSerializer.SerializeMessage(message, _memoryPool);

        // Link client lifetime, connection lifetime, request timeout, and external token
        using var timeoutCts = new CancellationTokenSource(_requestTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            _clientLifetimeCts.Token, connection.Token, timeoutCts.Token, externalToken);

        try
        {
            // Send directly to socket
            int bytesSent =
                await connection.Socket.SendAsync(serializedMessage.Memory, SocketFlags.None, linkedCts.Token);
            Log.Verbose("ClientConnection[{Endpoint}]: Sent {BytesSent} bytes for RequestId {RequestId}.",
                connection.EndPoint, bytesSent, requestId);

            // Wait for the reply via the TaskCompletionSource or cancellation
            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(-1, linkedCts.Token));

            if (completedTask == tcs.Task)
            {
                // Success! Return the result (caller must dispose the returned message)
                return await tcs.Task; // Propagates potential exception from TrySetException
            }
            else
            {
                // Task was cancelled
                string reason = timeoutCts.IsCancellationRequested ? "Request timed out."
                    : connection.Token.IsCancellationRequested ? "Connection closed."
                    : externalToken.IsCancellationRequested ? "Operation cancelled by caller."
                    : "Request cancelled.";
                Log.Warning("Wait for reply for RequestId {RequestId} failed: {Reason}", requestId, reason);
                throw new OperationCanceledException(reason, linkedCts.Token);
            }
        }
        catch (Exception ex) // Catches SendAsync errors or unexpected issues
        {
            Log.Error(ex, "Error during Send/Wait for RequestId {RequestId}", requestId);
            // Ensure TCS is cleaned up and faulted on error
            if (_pendingRequests.TryRemove(requestId, out var failedTcs))
            {
                failedTcs.TrySetException(ex);
            }

            if (ex is SocketException || ex is ObjectDisposedException)
            {
                // Trigger connection disposal on socket errors
                _ = connection.DisposeAsync();
            }

            throw; // Re-throw the original exception
        }
        finally
        {
            // Ensure cleanup if TCS wasn't completed/removed above (e.g., cancellation before TCS completion)
            if (_pendingRequests.TryRemove(requestId, out var leftoverTcs))
            {
                // If cancelled, signal cancellation. Otherwise, maybe an unknown state? Log it.
                if (linkedCts.IsCancellationRequested)
                    leftoverTcs.TrySetCanceled(linkedCts.Token);
                else
                    leftoverTcs.TrySetException(new InvalidOperationException("Request left in unexpected state."));
            }
        }
    }

    // --- Public API Methods ---

    public async Task<string> SetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        Log.Information("Sending SET request: Key={Key}", key);
        // Simple concatenation, adjust if your protocol needs length prefixes etc.
        var payload = Encoding.UTF8.GetBytes(key + ":" + value);

        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Set, GlobalConfig.CurrentVersion);

        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken);
            var replyPayload = reply.Payload.Length > 0 ? Encoding.UTF8.GetString(reply.Payload.Span) : "[No Payload]";
            Log.Information("Received SET reply: {ReplyPayload}", replyPayload);
            return $"Set OK. Reply: {replyPayload}";
        }
        finally
        {
            reply?.Dispose(); // Ensure reply message buffer is disposed
        }
    }

    public async Task<string> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Log.Information("Sending GET request: Key={Key}", key);
        var payload = Encoding.UTF8.GetBytes(key);
        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Get, GlobalConfig.CurrentVersion);

        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken);
            var value = reply.Payload.Length > 0 ? Encoding.UTF8.GetString(reply.Payload.Span) : "[Not Found or Empty]";
            Log.Information("Received GET reply: Value={Value}", value);
            return value;
        }
        finally
        {
            reply?.Dispose();
        }
    }

    public async Task<string> UpdateAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        Log.Information("Sending UPDATE request: Key={Key}", key);
        var payload = Encoding.UTF8.GetBytes(key + ":" + value); // Adjust format as needed
        var header = new VsrHeader(0, 0, 0, (uint)payload.Length, 0, 0, 0, 0, 0, 0, 0, 0, Command.Request,
            Operation.Update, GlobalConfig.CurrentVersion);

        VsrMessage? reply = null;
        try
        {
            reply = await SendRequestInternalAsync(header, payload, cancellationToken);
            var replyPayload = reply.Payload.Length > 0 ? Encoding.UTF8.GetString(reply.Payload.Span) : "[No Payload]";
            Log.Information("Received UPDATE reply: {ReplyPayload}", replyPayload);
            return $"Update OK. Reply: {replyPayload}";
        }
        finally
        {
            reply?.Dispose();
        }
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("Pinging all connected replicas...");
        var currentConnections = _connections.ToList(); // Snapshot

        if (!currentConnections.Any())
        {
            Log.Warning("No active connections to ping.");
            return;
        }

        var pingTasks = new List<Task>();
        foreach (var kvp in currentConnections)
        {
            pingTasks.Add(SendSinglePingAsync(kvp.Value, cancellationToken));
        }

        await Task.WhenAll(pingTasks);
        Log.Information("Ping sequence completed.");
    }

    private async Task SendSinglePingAsync(ClientConnection connection, CancellationToken cancellationToken)
    {
        uint requestId = (uint)Interlocked.Increment(ref _requestCounter);
        var header = new VsrHeader(0, _clientId, 0, 0, requestId, 0, 0, 0, 0, 0, 0, 0, Command.Ping, Operation.Reserved,
            GlobalConfig.CurrentVersion);
        var message = new VsrMessage(header, Memory<byte>.Empty); // No payload

        var tcs = new TaskCompletionSource<VsrMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pendingRequests.TryAdd(requestId, tcs))
        {
            Log.Error("Failed to add pending ping request (duplicate ID?): {RequestId} to {Endpoint}", requestId,
                connection.EndPoint);
            return; // Skip this one
        }

        Log.Debug("Sending Ping RequestId: {RequestId} to {Endpoint}", requestId, connection.EndPoint);
        using var serializedMessage = VsrMessageSerializer.SerializeMessage(message, _memoryPool);

        // Link client lifetime, connection lifetime, request timeout, and external token
        using var timeoutCts = new CancellationTokenSource(_requestTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            _clientLifetimeCts.Token, connection.Token, timeoutCts.Token, cancellationToken);

        VsrMessage? pongMessage = null;
        try
        {
            int bytesSent =
                await connection.Socket.SendAsync(serializedMessage.Memory, SocketFlags.None, linkedCts.Token);
            Log.Verbose("ClientConnection[{Endpoint}]: Sent {BytesSent} bytes for Ping RequestId {RequestId}.",
                connection.EndPoint, bytesSent, requestId);

            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(-1, linkedCts.Token));

            if (completedTask == tcs.Task)
            {
                pongMessage = await tcs.Task; // Get the pong message
                Log.Information("Received Pong from {Endpoint} (Replica {ReplicaId}) for RequestId: {RequestId}",
                    connection.EndPoint, pongMessage.Header.Replica, requestId);
            }
            else
            {
                string reason = timeoutCts.IsCancellationRequested ? "timed out"
                    : connection.Token.IsCancellationRequested ? "connection closed"
                    : cancellationToken.IsCancellationRequested ? "cancelled by caller"
                    : "cancelled";
                Log.Warning("Ping to {Endpoint} (RequestId: {RequestId}) {Reason}.", connection.EndPoint, requestId,
                    reason);
            }
        }
        catch (Exception ex) // Catches SendAsync errors or unexpected issues
        {
            Log.Error(ex, "Error during Ping Send/Wait for RequestId {RequestId} to {Endpoint}", requestId,
                connection.EndPoint);
            if (ex is SocketException || ex is ObjectDisposedException) _ = connection.DisposeAsync();
            // Ensure TCS is cleaned up and faulted on error
            if (_pendingRequests.TryRemove(requestId, out var failedTcs))
            {
                failedTcs.TrySetException(ex);
            }
        }
        finally
        {
            pongMessage?.Dispose(); // Dispose the received pong message if we got one
            // Ensure pending request is removed if it wasn't completed successfully
            _pendingRequests.TryRemove(requestId, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Log.Information("Disposing VsrClient...");
        if (!_clientLifetimeCts.IsCancellationRequested)
        {
            try
            {
                _clientLifetimeCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        var disposeTasks = _connections.Values.Select(conn => conn.DisposeAsync().AsTask());
        await Task.WhenAll(disposeTasks); // Dispose all connections concurrently

        // Fail any outstanding requests
        foreach (var kvp in _pendingRequests)
        {
            kvp.Value.TrySetException(new ObjectDisposedException("VsrClient is being disposed."));
        }

        _pendingRequests.Clear();
        _connections.Clear();

        _clientLifetimeCts.Dispose();
        Log.Information("VsrClient disposed.");
        GC.SuppressFinalize(this);
    }
}