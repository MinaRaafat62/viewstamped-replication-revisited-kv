using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Serilog;
using VsrReplica.Networking.Interfaces;
using VsrReplica.Networking.MemoryPool;
using VsrReplica.Networking.Transport;

namespace VsrReplica.Networking;

public class TcpNetworkManager<TMessage> : INetworkManager where TMessage : INetworkMessage
{
    private readonly Func<ConnectionId, TMessage, ValueTask> _onMessageReceived;
    private readonly Func<ConnectionId, Exception, ValueTask> _onProcessingError;
    private readonly Func<ConnectionId, IPEndPoint?, Exception?, ValueTask> _onConnectionClosed;
    private readonly Func<IPEndPoint, Exception, ValueTask> _onConnectionFailed;
    private readonly INetworkMessageSerializer<TMessage> _serializer;
    private readonly SenderPool _senderPool;
    private readonly IoQueue _transportScheduler;
    private readonly PipeScheduler _applicationScheduler;
    private readonly PinnedBlockMemoryPool _memoryPool;
    private readonly TcpServer _tcpServer;
    private long _nextConnectionId;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopped;
    private readonly object _stopLock = new();
    private readonly ConcurrentDictionary<ConnectionId, ManagedTcpConnection<TMessage>> _activeConnections = new();
    private readonly ConcurrentDictionary<IPEndPoint, ConnectionId> _endpointToConnectionIdMap = new();
    private readonly Dictionary<byte, IPEndPoint> _replicaIdToEndPointMap;

    private readonly int _maxConnectRetries;
    private readonly TimeSpan _initialConnectRetryDelay;
    private readonly TimeSpan _maxConnectRetryDelay;
    private readonly TimeSpan _connectTimeout;

    public IPEndPoint LocalEndPoint { get; }

    public TcpNetworkManager(
        IPEndPoint listenEndPoint,
        SenderPool senderPool,
        IoQueue transportScheduler,
        PipeScheduler applicationScheduler,
        PinnedBlockMemoryPool memoryPool,
        INetworkMessageSerializer<TMessage> serializer,
        Func<ConnectionId, TMessage, ValueTask> onMessageReceived,
        Func<ConnectionId, Exception, ValueTask> onProcessingError,
        Func<ConnectionId, IPEndPoint?, Exception?, ValueTask> onConnectionClosed,
        Func<IPEndPoint, Exception, ValueTask> onConnectionFailed,
        Dictionary<byte, IPEndPoint> replicaIdToEndPointMap,
        int maxConnectRetries = 5,
        TimeSpan? initialConnectRetryDelay = null,
        TimeSpan? maxConnectRetryDelay = null,
        TimeSpan? connectTimeout = null
    )
    {
        LocalEndPoint = listenEndPoint ?? throw new ArgumentNullException(nameof(listenEndPoint));
        _senderPool = senderPool ?? throw new ArgumentNullException(nameof(senderPool));
        _transportScheduler = transportScheduler ?? throw new ArgumentNullException(nameof(transportScheduler));
        _applicationScheduler = applicationScheduler ?? throw new ArgumentNullException(nameof(applicationScheduler));
        _memoryPool = memoryPool ?? throw new ArgumentNullException(nameof(memoryPool));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _onMessageReceived = onMessageReceived ?? throw new ArgumentNullException(nameof(onMessageReceived));
        _onProcessingError = onProcessingError ?? throw new ArgumentNullException(nameof(onProcessingError));
        _onConnectionClosed = onConnectionClosed ?? throw new ArgumentNullException(nameof(onConnectionClosed));
        _onConnectionFailed = onConnectionFailed ?? throw new ArgumentNullException(nameof(onConnectionFailed));

        _maxConnectRetries = maxConnectRetries > 0 ? maxConnectRetries : 1;
        _initialConnectRetryDelay = initialConnectRetryDelay ?? TimeSpan.FromSeconds(1);
        _maxConnectRetryDelay = maxConnectRetryDelay ?? TimeSpan.FromSeconds(15);
        _connectTimeout = connectTimeout ?? TimeSpan.FromSeconds(5);
        _replicaIdToEndPointMap =
            replicaIdToEndPointMap ?? throw new ArgumentNullException(nameof(replicaIdToEndPointMap));

        _tcpServer = new TcpServer(
            LocalEndPoint,
            OnNewConnectionAccepted,
            _senderPool,
            _transportScheduler,
            _applicationScheduler,
            _memoryPool);
    }

    public Task StartAsync()
    {
        lock (_stopLock)
        {
            if (_stopped)
                throw new InvalidOperationException("NetworkManager has been stopped and cannot be restarted.");
        }

        Log.Information("NetworkManager[{ListenEndPoint}]: Starting listening.", LocalEndPoint);
        _tcpServer.Start();
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        bool shouldStop = false;
        lock (_stopLock)
        {
            if (!_stopped)
            {
                _stopped = true;
                shouldStop = true;
            }
        }

        if (!shouldStop) return;

        Log.Information("NetworkManager[{ListenEndPoint}]: Stopping...", LocalEndPoint);

        try
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }

        await _tcpServer.DisposeAsync().ConfigureAwait(false);

        var connectionIds = _activeConnections.Keys.ToList();
        Log.Debug("NetworkManager[{ListenEndPoint}]: Disposing {Count} active connections.", LocalEndPoint,
            connectionIds.Count);

        var disposeTasks = new List<Task>(connectionIds.Count);
        foreach (var connectionId in connectionIds)
        {
            if (_activeConnections.TryRemove(connectionId, out var connection))
            {
                var endpointKvp = _endpointToConnectionIdMap.FirstOrDefault(kvp => kvp.Value == connectionId);
                if (endpointKvp.Key != null)
                {
                    _endpointToConnectionIdMap.TryRemove(endpointKvp);
                }

                disposeTasks.Add(connection.DisposeAsync().AsTask());
            }
        }

        try
        {
            await Task.WhenAll(disposeTasks).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex,
                "NetworkManager[{ListenEndPoint}]: One or more errors occurred during bulk connection disposal.",
                LocalEndPoint);
        }

        _activeConnections.Clear();
        _endpointToConnectionIdMap.Clear();
        _cts.Dispose();

        Log.Information("NetworkManager[{ListenEndPoint}]: Stopped.", LocalEndPoint);
    }

    private void OnNewConnectionAccepted(Connection lowLevelConnection)
    {
        if (_stopped)
        {
            Log.Warning("NetworkManager[{ListenEndPoint}]: Ignoring new connection, manager is stopped.",
                LocalEndPoint);
            _ = lowLevelConnection.DisposeAsync();
            return;
        }

        var connectionId = new ConnectionId(Interlocked.Increment(ref _nextConnectionId));
        IPEndPoint? remoteEndPoint = null;
        try
        {
            remoteEndPoint = lowLevelConnection.RemoteEndPoint as IPEndPoint;
            Log.Information(
                "NetworkManager[{ListenEndPoint}]: New incoming connection accepted: {ConnectionId} from {RemoteEndPoint}",
                LocalEndPoint, connectionId, remoteEndPoint ?? (object)"Unknown");
        }
        catch (ObjectDisposedException)
        {
            Log.Warning(
                "NetworkManager[{ListenEndPoint}]: Could not get remote endpoint for incoming connection {ConnectionId} - socket disposed.",
                LocalEndPoint, connectionId);
            _ = lowLevelConnection.DisposeAsync();
            return;
        }
        catch (Exception ex)
        {
            Log.Warning(ex,
                "NetworkManager[{ListenEndPoint}]: Error getting remote endpoint for incoming connection {ConnectionId}.",
                LocalEndPoint, connectionId);
        }

        var managedConnection = new ManagedTcpConnection<TMessage>(
            connectionId,
            lowLevelConnection,
            _serializer,
            _memoryPool,
            _onMessageReceived,
            (closedId, error) => HandleManagedConnectionClosed(closedId, remoteEndPoint, error),
            _onProcessingError
        );

        if (_activeConnections.TryAdd(connectionId, managedConnection))
        {
            managedConnection.StartProcessing();
            Log.Debug("NetworkManager[{ListenEndPoint}]: Started processing for incoming connection {ConnectionId}.",
                LocalEndPoint, connectionId);
        }
        else
        {
            Log.Error(
                "NetworkManager[{ListenEndPoint}]: Failed to add new connection {ConnectionId} to dictionary (duplicate ID?). Disposing.",
                LocalEndPoint, connectionId);
            _ = managedConnection.DisposeAsync();
        }
    }

    public async Task<ConnectionId?> ConnectAsync(IPEndPoint endpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        if (_stopped)
        {
            Log.Warning("NetworkManager[{ListenEndPoint}]: ConnectAsync called while stopped.", LocalEndPoint);
            return null;
        }

        if (_endpointToConnectionIdMap.TryGetValue(endpoint, out var existingId) &&
            _activeConnections.ContainsKey(existingId))
        {
            Log.Information(
                "NetworkManager[{ListenEndPoint}]: Connection to {RemoteEndPoint} already exists with ID {ConnectionId}.",
                LocalEndPoint, endpoint, existingId);
            return existingId;
        }

        _endpointToConnectionIdMap.TryRemove(endpoint, out _);

        Log.Information("NetworkManager[{ListenEndPoint}]: Attempting to connect to {RemoteEndPoint}...", LocalEndPoint,
            endpoint);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        var linkedToken = linkedCts.Token;

        for (int attempt = 1; attempt <= _maxConnectRetries; attempt++)
        {
            if (linkedToken.IsCancellationRequested)
            {
                Log.Information(
                    "NetworkManager[{ListenEndPoint}]: Connection attempt to {RemoteEndPoint} cancelled before attempt {Attempt}.",
                    LocalEndPoint, endpoint, attempt);
                return null;
            }

            Socket? socket = null;
            Connection? lowLevelConnection = null;
            ManagedTcpConnection<TMessage>? managedConnection = null;

            try
            {
                socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                await socket.ConnectAsync(endpoint).WaitAsync(_connectTimeout, linkedToken).ConfigureAwait(false);

                var socketWrapper = new SocketWrapper(socket);
                lowLevelConnection = new Connection(
                    socketWrapper,
                    _senderPool,
                    new Receiver(),
                    _transportScheduler,
                    _applicationScheduler,
                    _memoryPool);

                ConnectionId? connectionId = new ConnectionId(Interlocked.Increment(ref _nextConnectionId));

                managedConnection = new ManagedTcpConnection<TMessage>(
                    connectionId.Value,
                    lowLevelConnection,
                    _serializer,
                    _memoryPool,
                    _onMessageReceived,
                    (closedId, error) => HandleManagedConnectionClosed(closedId, endpoint, error),
                    _onProcessingError
                );

                if (_activeConnections.TryAdd(connectionId.Value, managedConnection))
                {
                    if (_endpointToConnectionIdMap.TryAdd(endpoint, connectionId.Value))
                    {
                        managedConnection.StartProcessing();
                        Log.Information(
                            "NetworkManager[{ListenEndPoint}]: Successfully connected to {RemoteEndPoint} with ID {ConnectionId}.",
                            LocalEndPoint, endpoint, connectionId.Value);
                        return connectionId.Value;
                    }
                    else
                    {
                        _activeConnections.TryRemove(connectionId.Value, out _);
                        Log.Error(
                            "NetworkManager[{ListenEndPoint}]: Failed to add outgoing connection {ConnectionId} to endpoint map. Cleaning up.",
                            LocalEndPoint, connectionId);
                        throw new InvalidOperationException(
                            "Failed to register endpoint mapping for new outgoing connection.");
                    }
                }
                else
                {
                    Log.Error(
                        "NetworkManager[{ListenEndPoint}]: Failed to add outgoing connection {ConnectionId} to active dictionary. Cleaning up.",
                        LocalEndPoint, connectionId);
                    throw new InvalidOperationException(
                        "Failed to register new outgoing connection in active dictionary.");
                }
            }
            catch (Exception ex) when (ex is SocketException || ex is TimeoutException ||
                                       ex is OperationCanceledException || ex is InvalidOperationException)
            {
                Log.Warning(ex,
                    "NetworkManager[{ListenEndPoint}]: Failed to connect to {RemoteEndPoint} (Attempt {Attempt}/{MaxAttempts}).",
                    LocalEndPoint, endpoint, attempt, _maxConnectRetries);

                await (managedConnection?.DisposeAsync() ?? ValueTask.CompletedTask).ConfigureAwait(false);
                if (lowLevelConnection != null && managedConnection == null)
                {
                    await lowLevelConnection.DisposeAsync().ConfigureAwait(false);
                }

                socket?.Dispose();

                if (linkedToken.IsCancellationRequested || attempt == _maxConnectRetries)
                {
                    Log.Error(
                        "NetworkManager[{ListenEndPoint}]: Final connection attempt to {RemoteEndPoint} failed or cancelled.",
                        LocalEndPoint, endpoint);
                    try
                    {
                        await _onConnectionFailed(endpoint, ex).ConfigureAwait(false);
                    }
                    catch (Exception cbEx)
                    {
                        Log.Error(cbEx,
                            "NetworkManager[{ListenEndPoint}]: Error in OnConnectionFailed callback for {RemoteEndPoint}.",
                            LocalEndPoint, endpoint);
                    }

                    return null;
                }

                try
                {
                    var delay = TimeSpan.FromMilliseconds(Math.Min(
                        _initialConnectRetryDelay.TotalMilliseconds * Math.Pow(2, attempt - 1),
                        _maxConnectRetryDelay.TotalMilliseconds));
                    Log.Information(
                        "NetworkManager[{ListenEndPoint}]: Retrying connection to {RemoteEndPoint} in {Delay}...",
                        LocalEndPoint, endpoint, delay);
                    await Task.Delay(delay, linkedToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    Log.Information(
                        "NetworkManager[{ListenEndPoint}]: Connection retry delay cancelled for {RemoteEndPoint}.",
                        LocalEndPoint, endpoint);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    "NetworkManager[{ListenEndPoint}]: Unexpected error connecting to {RemoteEndPoint}. Aborting connection attempt.",
                    LocalEndPoint, endpoint);
                await (managedConnection?.DisposeAsync() ?? ValueTask.CompletedTask).ConfigureAwait(false);
                if (lowLevelConnection != null && managedConnection == null)
                {
                    await lowLevelConnection.DisposeAsync().ConfigureAwait(false);
                }

                socket?.Dispose();
                try
                {
                    await _onConnectionFailed(endpoint, ex).ConfigureAwait(false);
                }
                catch (Exception cbEx)
                {
                    Log.Error(cbEx,
                        "NetworkManager[{ListenEndPoint}]: Error in OnConnectionFailed callback for {RemoteEndPoint}.",
                        LocalEndPoint, endpoint);
                }

                return null;
            }
        }

        return null;
    }

    public async Task DisconnectAsync(ConnectionId connectionId)
    {
        if (_stopped) return;

        Log.Information("NetworkManager[{ListenEndPoint}]: Requesting disconnect for ConnectionId {ConnectionId}.",
            LocalEndPoint, connectionId);
        if (_activeConnections.TryRemove(connectionId, out var managedConnection))
        {
            var endpointKvp = _endpointToConnectionIdMap.FirstOrDefault(kvp => kvp.Value == connectionId);
            if (endpointKvp.Key != null)
            {
                _endpointToConnectionIdMap.TryRemove(endpointKvp);
            }

            await managedConnection.DisposeAsync().ConfigureAwait(false);
            Log.Information(
                "NetworkManager[{ListenEndPoint}]: Initiated disposal for connection {ConnectionId} after explicit disconnect request.",
                LocalEndPoint, connectionId);
        }
        else
        {
            Log.Warning(
                "NetworkManager[{ListenEndPoint}]: Disconnect requested for unknown or already removed ConnectionId {ConnectionId}.",
                LocalEndPoint, connectionId);
        }
    }

    private ValueTask HandleManagedConnectionClosed(ConnectionId connectionId, IPEndPoint? remoteEndPoint,
        Exception? error)
    {
        Log.Information(
            "NetworkManager[{ListenEndPoint}]: Handling closure for ConnectionId {ConnectionId}. Remote: {RemoteEndPoint}, Error: {Error}",
            LocalEndPoint, connectionId, remoteEndPoint ?? (object)"Incoming", error?.Message ?? "Normal");

        _activeConnections.TryRemove(connectionId, out _);
        if (remoteEndPoint != null)
        {
            // Ensure removal only if the ID matches the stored value for that endpoint.
            _endpointToConnectionIdMap.TryRemove(
                new KeyValuePair<IPEndPoint, ConnectionId>(remoteEndPoint, connectionId));
        }

        try
        {
            return _onConnectionClosed(connectionId, remoteEndPoint, error);
        }
        catch (Exception ex)
        {
            Log.Error(ex,
                "NetworkManager[{ListenEndPoint}]: Error executing OnConnectionClosed callback for {ConnectionId}.",
                LocalEndPoint, connectionId);
            return ValueTask.CompletedTask;
        }
    }

    public async ValueTask SendAsync(ConnectionId connectionId, TMessage message)
    {
        if (_stopped)
        {
            throw new InvalidOperationException("NetworkManager is stopped.");
        }

        if (_activeConnections.TryGetValue(connectionId, out var managedConnection))
        {
            try
            {
                await managedConnection.SendMessageAsync(message).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                Log.Warning(
                    "NetworkManager[{ListenEndPoint}]: SendAsync failed, connection {ConnectionId} was disposed during send.",
                    LocalEndPoint, connectionId);
                throw;
            }
            catch (InvalidOperationException ex)
            {
                Log.Warning(ex, "NetworkManager[{ListenEndPoint}]: SendAsync failed for connection {ConnectionId}.",
                    LocalEndPoint, connectionId);
                throw;
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    "NetworkManager[{ListenEndPoint}]: Unexpected error sending message to ConnectionId {ConnectionId}.",
                    LocalEndPoint, connectionId);
                throw;
            }
        }
        else
        {
            Log.Warning("NetworkManager[{ListenEndPoint}]: SendAsync failed, ConnectionId not found: {ConnId}",
                LocalEndPoint, connectionId);
            throw new KeyNotFoundException($"ConnectionId {connectionId} not found.");
        }
    }

    public async ValueTask SendBytesAsync(ConnectionId connectionId, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var token = linkedCts.Token;

        if (_stopped) throw new InvalidOperationException("NetworkManager is stopped.");

        if (_activeConnections.TryGetValue(connectionId, out var managedConnection))
        {
            try
            {
                await managedConnection.SendRawBytesAsync(data, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                Log.Warning(
                    "NetworkManager[{ListenEndPoint}]: SendBytesAsync cancelled for ConnectionId {ConnectionId}.",
                    LocalEndPoint, connectionId);
                throw;
            }
            catch (ObjectDisposedException)
            {
                Log.Warning(
                    "NetworkManager[{ListenEndPoint}]: SendBytesAsync failed, connection {ConnectionId} was disposed during send.",
                    LocalEndPoint, connectionId);
                throw;
            }
            catch (InvalidOperationException ex)
            {
                Log.Warning(ex,
                    "NetworkManager[{ListenEndPoint}]: SendBytesAsync failed for connection {ConnectionId}.",
                    LocalEndPoint, connectionId);
                throw;
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    "NetworkManager[{ListenEndPoint}]: Unexpected error sending raw bytes to ConnectionId {ConnectionId}.",
                    LocalEndPoint, connectionId);
                throw;
            }
        }
        else
        {
            Log.Warning("NetworkManager[{ListenEndPoint}]: SendBytesAsync failed, ConnectionId not found: {ConnId}",
                LocalEndPoint, connectionId);
            throw new KeyNotFoundException($"ConnectionId {connectionId} not found.");
        }
    }

    public async ValueTask BroadcastAsync(IEnumerable<ConnectionId> connectionIds, TMessage message)
    {
        ArgumentNullException.ThrowIfNull(connectionIds);
        if (_stopped) return;

        var targets = connectionIds
            .Select(id => _activeConnections.GetValueOrDefault(id))
            .Where(conn => conn != null && !conn.IsClosed)
            .ToList();

        if (!targets.Any())
        {
            Log.Debug("NetworkManager[{ListenEndPoint}]: Broadcast requested but no active target connections found.",
                LocalEndPoint);
            return;
        }

        Log.Debug("NetworkManager[{ListenEndPoint}]: Broadcasting message to {Count} connections.", LocalEndPoint,
            targets.Count);

        var sendTasks = targets.Select(conn => SendAsync(conn!.Id, message).AsTask()).ToList();

        try
        {
            await Task.WhenAll(sendTasks).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "NetworkManager[{ListenEndPoint}]: At least one error occurred during broadcast send.",
                LocalEndPoint);
        }
    }

    public async ValueTask BroadcastBytesAsync(IEnumerable<ConnectionId> connectionIds, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connectionIds);
        if (_stopped) return;

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var token = linkedCts.Token;

        var targets = connectionIds
            .Select(id => _activeConnections.GetValueOrDefault(id))
            .Where(conn => conn != null && !conn.IsClosed)
            .ToList();

        if (!targets.Any())
        {
            Log.Debug(
                "NetworkManager[{ListenEndPoint}]: BroadcastBytes requested but no active target connections found.",
                LocalEndPoint);
            return;
        }

        Log.Debug(
            "NetworkManager[{ListenEndPoint}]: Broadcasting raw data ({DataLength} bytes) to {Count} connections.",
            LocalEndPoint, data.Length, targets.Count);

        var sendTasks = targets.Select(conn => conn!.SendRawBytesAsync(data, token).AsTask()).ToList();

        try
        {
            await Task.WhenAll(sendTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            Log.Warning("NetworkManager[{ListenEndPoint}]: BroadcastBytesAsync cancelled.", LocalEndPoint);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "NetworkManager[{ListenEndPoint}]: At least one error occurred during BroadcastBytes send.",
                LocalEndPoint);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    async ValueTask INetworkManager.SendAsync(ConnectionId connectionId, ReadOnlyMemory<byte> data)
    {
        await SendBytesAsync(connectionId, data).ConfigureAwait(false);
    }

    async ValueTask INetworkManager.BroadcastAsync(IEnumerable<ConnectionId> connectionIds, ReadOnlyMemory<byte> data)
    {
        await BroadcastBytesAsync(connectionIds, data).ConfigureAwait(false);
    }

    public List<ConnectionId> GetOtherReplicasConnectionIds()
    {
        return _endpointToConnectionIdMap.Values.ToList();
    }

    public ConnectionId? GetConnectionIdForReplica(byte replicaId)
    {
        if (!_replicaIdToEndPointMap.TryGetValue(replicaId, out var endpoint))
        {
            Log.Warning("NetworkManager[{LocalEndPoint}]: No endpoint configured for Replica ID {ReplicaId}",
                LocalEndPoint, replicaId);
            return null;
        }

        if (_endpointToConnectionIdMap.TryGetValue(endpoint, out var connectionId))
        {
            if (_activeConnections.ContainsKey(connectionId))
            {
                return connectionId;
            }

            Log.Warning(
                "NetworkManager[{LocalEndPoint}]: Found ConnectionId {ConnId} for Replica {ReplicaId} ({EndPoint}) but connection is not active. Removing stale map entry.",
                LocalEndPoint, connectionId, replicaId, endpoint);
            _endpointToConnectionIdMap.TryRemove(
                new KeyValuePair<IPEndPoint, ConnectionId>(endpoint, connectionId));
            return null;
        }

        Log.Debug(
            "NetworkManager[{LocalEndPoint}]: No active connection found for Replica ID {ReplicaId} ({EndPoint})",
            LocalEndPoint, replicaId, endpoint);
        return null;
    }
}