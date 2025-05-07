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
    // Core components
    private readonly TcpServer _tcpServer;
    private readonly SenderPool _senderPool;
    private readonly IoQueue _transportScheduler;
    private readonly PipeScheduler _applicationScheduler;
    private readonly PinnedBlockMemoryPool _memoryPool;
    private readonly INetworkMessageSerializer<TMessage> _serializer;
    private readonly ReplicaConnectionManager _replicaManager;
    private readonly NetworkMessageProcessor<TMessage> _messageProcessor;

    // Connection management
    private readonly ConcurrentDictionary<ConnectionId, Connection> _connections = new();
    private readonly ConcurrentDictionary<ConnectionId, IPEndPoint> _pendingConnections = new();
    private readonly Dictionary<IPEndPoint, byte> _replicaEndPointToIdMap;
    private readonly Dictionary<byte, IPEndPoint> _replicaIdToEndPointMap;
    private readonly byte _localReplicaId;

    // Lifecycle management
    private readonly CancellationTokenSource _lifetimeCts = new();
    private volatile bool _isStarted;
    private volatile bool _isDisposed;

    // Event handlers
    private readonly Func<ConnectionId, TMessage, ValueTask> _onMessageReceived;
    private readonly Func<ConnectionId, Exception, ValueTask> _onProcessingError;
    private readonly Func<ConnectionId, IPEndPoint?, bool, Exception?, ValueTask> _onConnectionClosed;
    private readonly Func<IPEndPoint, Exception, ValueTask> _onConnectionFailed;

    // Connection ID counter
    private long _connectionIdCounter;

    public IPEndPoint? LocalEndPoint { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RobustTcpNetworkManager{TMessage}"/> class.
    /// </summary>
    public TcpNetworkManager(
        IPEndPoint listenEndPoint,
        byte localReplicaId,
        Dictionary<byte, IPEndPoint> replicaIdToEndPointMap,
        SenderPool senderPool,
        IoQueue transportScheduler,
        PipeScheduler applicationScheduler,
        PinnedBlockMemoryPool memoryPool,
        INetworkMessageSerializer<TMessage> serializer,
        Func<ConnectionId, TMessage, ValueTask> onMessageReceived,
        Func<ConnectionId, Exception, ValueTask> onProcessingError,
        Func<ConnectionId, IPEndPoint?, bool, Exception?, ValueTask> onConnectionClosed,
        Func<IPEndPoint, Exception, ValueTask> onConnectionFailed)
    {
        LocalEndPoint = listenEndPoint ?? throw new ArgumentNullException(nameof(listenEndPoint));
        _localReplicaId = localReplicaId;
        _replicaIdToEndPointMap =
            replicaIdToEndPointMap ?? throw new ArgumentNullException(nameof(replicaIdToEndPointMap));
        _senderPool = senderPool ?? throw new ArgumentNullException(nameof(senderPool));
        _transportScheduler = transportScheduler ?? throw new ArgumentNullException(nameof(transportScheduler));
        _applicationScheduler = applicationScheduler ?? throw new ArgumentNullException(nameof(applicationScheduler));
        _memoryPool = memoryPool ?? throw new ArgumentNullException(nameof(memoryPool));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _onMessageReceived = onMessageReceived ?? throw new ArgumentNullException(nameof(onMessageReceived));
        _onProcessingError = onProcessingError ?? throw new ArgumentNullException(nameof(onProcessingError));
        _onConnectionClosed = onConnectionClosed ?? throw new ArgumentNullException(nameof(onConnectionClosed));
        _onConnectionFailed = onConnectionFailed ?? throw new ArgumentNullException(nameof(onConnectionFailed));

        // Create endpoint to ID mapping
        _replicaEndPointToIdMap = _replicaIdToEndPointMap.ToDictionary(kvp => kvp.Value, kvp => kvp.Key);

        // Create TCP server
        _tcpServer = new TcpServer(
            LocalEndPoint,
            OnNewConnectionAccepted,
            _senderPool,
            _transportScheduler,
            _applicationScheduler,
            _memoryPool);

        // Create message processor
        _messageProcessor = new NetworkMessageProcessor<TMessage>(
            _serializer,
            _onMessageReceived,
            _memoryPool);

        // Create replica connection manager
        _replicaManager = new ReplicaConnectionManager(
            _localReplicaId,
            _replicaIdToEndPointMap,
            ConnectToReplicaAsync,
            DisconnectAsync);
    }

    private Task<ConnectionId?> ConnectToReplicaAsync(IPEndPoint endPoint, bool isReplica, byte replicaId,
        CancellationToken cancellationToken)
    {
        return ConnectToEndpointAsync(endPoint, isReplica, replicaId, cancellationToken);
    }

    /// <summary>
    /// Starts the network manager, including the TCP server and initiating connections to other replicas.
    /// </summary>
    public async Task StartAsync()
    {
        if (_isStarted)
        {
            Log.Warning("NetworkManager already started");
            return;
        }

        Log.Information("Starting NetworkManager on {Endpoint}", LocalEndPoint);
        _tcpServer.Start();
        _isStarted = true;

        // Connect to all replicas
        await _replicaManager.ConnectToAllReplicasAsync();

        Log.Information("NetworkManager started successfully");
    }

    /// <summary>
    /// Stops the network manager and disconnects all connections.
    /// </summary>
    public async Task StopAsync()
    {
        if (!_isStarted)
        {
            return;
        }

        Log.Information("Stopping NetworkManager");
        await _lifetimeCts.CancelAsync();
        _isStarted = false;

        // Disconnect from all replicas
        await _replicaManager.DisconnectAllAsync();

        // Disconnect all client connections
        var clientConnections = _connections.Keys
            .Where(id => !GetOtherReplicasConnectionIds().Contains(id))
            .ToList();

        foreach (var connectionId in clientConnections)
        {
            await DisconnectAsync(connectionId);
        }

        await _tcpServer.DisposeAsync();
        Log.Information("NetworkManager stopped");
    }

    /// <summary>
    /// Establishes a connection to the specified endpoint.
    /// </summary>
    public async Task<ConnectionId?> ConnectAsync(IPEndPoint endpoint, CancellationToken cancellationToken = default)
    {
        if (!_isStarted)
        {
            throw new InvalidOperationException("NetworkManager not started");
        }

        // If connecting to a replica, use the replica connection manager
        if (_replicaEndPointToIdMap.TryGetValue(endpoint, out var replicaId))
        {
            return await _replicaManager.ConnectToReplicaAsync(replicaId);
        }

        // Handle client connections
        return await ConnectToEndpointAsync(endpoint, isReplica: false, replicaId: null, cancellationToken);
    }


    /// <summary>
    /// Disconnects the specified connection.
    /// </summary>
    public async Task DisconnectAsync(ConnectionId connectionId)
    {
        if (_connections.TryRemove(connectionId, out var connection))
        {
            // Check if this is a replica connection
            var replicaId = _replicaManager.GetReplicaIdForConnection(connectionId);

            try
            {
                connection.Shutdown();
                await connection.DisposeAsync();

                // Emit the connection closed event
                if (connection.RemoteEndPoint is IPEndPoint remoteEndPoint)
                {
                    await _onConnectionClosed(connectionId, remoteEndPoint, replicaId.HasValue, null);
                }
                else
                {
                    await _onConnectionClosed(connectionId, null, replicaId.HasValue, null);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error while disconnecting {ConnectionId}", connectionId);

                // Still emit the connection closed event
                if (connection.RemoteEndPoint is IPEndPoint remoteEndPoint)
                {
                    await _onConnectionClosed(connectionId, remoteEndPoint, replicaId.HasValue, ex);
                }
                else
                {
                    await _onConnectionClosed(connectionId, null, replicaId.HasValue, ex);
                }
            }
        }
    }

    /// <summary>
    /// Sends data to a specific connection.
    /// </summary>
    public async ValueTask SendAsync(ConnectionId connectionId, ReadOnlyMemory<byte> data)
    {
        if (!_connections.TryGetValue(connectionId, out var connection))
        {
            throw new ArgumentException($"Connection {connectionId} not found", nameof(connectionId));
        }

        try
        {
            // Copy data to the writer
            await connection.Output.WriteAsync(data);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error sending data to {ConnectionId}", connectionId);
            throw;
        }
    }

    /// <summary>
    /// Broadcasts data to multiple connections.
    /// </summary>
    public async ValueTask BroadcastAsync(IEnumerable<ConnectionId> connectionIds, ReadOnlyMemory<byte> data)
    {
        var tasks = new List<ValueTask>();
        foreach (var connectionId in connectionIds)
        {
            tasks.Add(SendAsync(connectionId, data));
        }

        foreach (var task in tasks)
        {
            await task;
        }
    }

    /// <summary>
    /// Gets a list of connection IDs for all connected replicas.
    /// </summary>
    public List<ConnectionId> GetOtherReplicasConnectionIds()
    {
        return _replicaManager.GetConnectedReplicaIds();
    }

    /// <summary>
    /// Gets the connection ID for a specific replica.
    /// </summary>
    public ConnectionId? GetConnectionIdForReplica(byte replicaId)
    {
        return _replicaManager.GetConnectionIdForReplica(replicaId);
    }

    /// <summary>
    /// Disposes the network manager.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
        {
            return;
        }

        _isDisposed = true;

        try
        {
            await StopAsync();
            await _replicaManager.DisposeAsync();
        }
        finally
        {
            _lifetimeCts.Dispose();
            _senderPool.Dispose();
        }
    }

    #region Private Methods

    /// <summary>
    /// Handles a new connection that has been accepted by the TCP server.
    /// </summary>
    private void OnNewConnectionAccepted(Connection connection)
    {
        if (_isDisposed)
        {
            connection.Shutdown();
            return;
        }

        var connectionId = new ConnectionId(Interlocked.Increment(ref _connectionIdCounter));

        // Store the connection
        _connections[connectionId] = connection;

        // Determine if this is a replica connection
        var remoteEndPoint = connection.RemoteEndPoint as IPEndPoint;

        if (remoteEndPoint != null && _replicaEndPointToIdMap.TryGetValue(remoteEndPoint, out var connectedReplicaId))
        {
            Log.Information("Accepted connection from replica {ReplicaId} at {Endpoint}", connectedReplicaId,
                remoteEndPoint);
        }
        else
        {
            Log.Information("Accepted client connection from {Endpoint}", remoteEndPoint);
        }

        // Set up event handlers
        connection.OnClosed += () => HandleConnectionClosed(connectionId, connection);
        connection.OnError += (ex) => HandleConnectionError(connectionId, connection, ex);

        // Start the connection
        connection.Start();

        // Start processing messages
        _ = Task.Run(async () =>
        {
            try
            {
                await _messageProcessor.ProcessMessagesAsync(connectionId, connection.Input, _lifetimeCts.Token);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error in message processing for {ConnectionId}", connectionId);
                await _onProcessingError(connectionId, ex);
                await DisconnectAsync(connectionId);
            }
        });
    }

    /// <summary>
    /// Handles errors on a connection.
    /// </summary>
    private async void HandleConnectionError(ConnectionId connectionId, Connection connection, Exception exception)
    {
        Log.Error(exception, "Error on connection {ConnectionId}", connectionId);
        await _onProcessingError(connectionId, exception);
    }

    /// <summary>
    /// Handles connection closure.
    /// </summary>
    private async void HandleConnectionClosed(ConnectionId connectionId, Connection connection)
    {
        try
        {
            // Remove from active connections
            if (_connections.TryRemove(connectionId, out _))
            {
                var remoteEndPoint = connection.RemoteEndPoint as IPEndPoint;
                bool isReplica = false;
                byte? replicaId = null;

                // Check if this was a replica connection
                if (remoteEndPoint != null &&
                    _replicaEndPointToIdMap.TryGetValue(remoteEndPoint, out var connectedReplicaId))
                {
                    isReplica = true;
                    replicaId = connectedReplicaId;

                    // Handle replica disconnection
                    _replicaManager.HandleReplicaConnectionClosed(connectionId, connectedReplicaId);
                }

                // Notify listeners
                await _onConnectionClosed(connectionId, remoteEndPoint, isReplica, null);
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error handling connection closure for {ConnectionId}", connectionId);
        }
    }

    /// <summary>
    /// Connects to an endpoint.
    /// </summary>
    private async Task<ConnectionId?> ConnectToEndpointAsync(IPEndPoint endpoint, bool isReplica, byte? replicaId,
        CancellationToken cancellationToken)
    {
        Log.Information("Connecting to {Endpoint} (Replica: {IsReplica}, ReplicaId: {ReplicaId})",
            endpoint, isReplica, replicaId);

        var connectionId = new ConnectionId(Interlocked.Increment(ref _connectionIdCounter));
        _pendingConnections[connectionId] = endpoint;

        Socket? socket = null;
        try
        {
            // Create and configure the socket
            socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;

            // Connect to the endpoint
            await socket.ConnectAsync(endpoint, cancellationToken);

            // Create connection
            var socketWrapper = new SocketWrapper(socket);
            var connection = new Connection(
                socketWrapper,
                _senderPool,
                new Receiver(),
                _transportScheduler,
                _applicationScheduler,
                _memoryPool);

            // Store the connection
            _connections[connectionId] = connection;
            _pendingConnections.TryRemove(connectionId, out _);

            // Set up event handlers
            connection.OnClosed += () => HandleConnectionClosed(connectionId, connection);
            connection.OnError += (ex) => HandleConnectionError(connectionId, connection, ex);

            // Start the connection
            connection.Start();

            // Start processing messages
            _ = Task.Run(async () =>
            {
                try
                {
                    await _messageProcessor.ProcessMessagesAsync(connectionId, connection.Input, _lifetimeCts.Token);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in message processing for {ConnectionId}", connectionId);
                    await _onProcessingError(connectionId, ex);
                    await DisconnectAsync(connectionId);
                }
            });

            return connectionId;
        }
        catch (Exception ex)
        {
            _pendingConnections.TryRemove(connectionId, out _);
            socket?.Dispose();
            Log.Error(ex, "Failed to connect to {Endpoint}", endpoint);
            await _onConnectionFailed(endpoint, ex);
            throw;
        }
    }

    #endregion
}