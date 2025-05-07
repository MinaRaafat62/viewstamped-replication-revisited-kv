using System.Collections.Concurrent;
using System.Net;
using Serilog;

namespace VsrReplica.Networking;

public class ReplicaConnectionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<byte, ReplicaState> _replicaConnections = new();
    private readonly byte _localReplicaId;
    private readonly Dictionary<byte, IPEndPoint> _replicaEndpoints;
    private readonly Func<IPEndPoint, bool, byte, CancellationToken, Task<ConnectionId?>> _connectFunc;
    private readonly Func<ConnectionId, Task> _disconnectFunc;
    private readonly CancellationTokenSource _lifetimeCts = new();
    private readonly SemaphoreSlim _reconnectSemaphore = new(1, 1);
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplicaConnectionManager"/> class.
    /// </summary>
    /// <param name="localReplicaId">The local replica ID.</param>
    /// <param name="replicaEndpoints">A dictionary mapping replica IDs to their endpoints.</param>
    /// <param name="connectFunc">Function to connect to an endpoint.</param>
    /// <param name="disconnectFunc">Function to disconnect a connection.</param>
    public ReplicaConnectionManager(
        byte localReplicaId,
        Dictionary<byte, IPEndPoint> replicaEndpoints,
        Func<IPEndPoint, bool, byte, CancellationToken, Task<ConnectionId?>> connectFunc,
        Func<ConnectionId, Task> disconnectFunc)
    {
        _localReplicaId = localReplicaId;
        _replicaEndpoints = replicaEndpoints ?? throw new ArgumentNullException(nameof(replicaEndpoints));
        _connectFunc = connectFunc ?? throw new ArgumentNullException(nameof(connectFunc));
        _disconnectFunc = disconnectFunc ?? throw new ArgumentNullException(nameof(disconnectFunc));

        // Initialize replica state tracking
        foreach (var (replicaId, endpoint) in _replicaEndpoints)
        {
            if (replicaId != _localReplicaId)
            {
                _replicaConnections[replicaId] = new ReplicaState(endpoint);
            }
        }
    }

    /// <summary>
    /// Gets all currently connected replica connection IDs.
    /// </summary>
    public List<ConnectionId> GetConnectedReplicaIds()
    {
        return _replicaConnections.Values
            .Where(rs => rs.ConnectionId.HasValue)
            .Select(rs => rs.ConnectionId!.Value)
            .ToList();
    }

    /// <summary>
    /// Gets the connection ID for a specific replica if connected.
    /// </summary>
    public ConnectionId? GetConnectionIdForReplica(byte replicaId)
    {
        if (replicaId == _localReplicaId)
        {
            return null;
        }

        return _replicaConnections.TryGetValue(replicaId, out var state) ? state.ConnectionId : null;
    }

    /// <summary>
    /// Gets the replica ID for a connection ID, if known.
    /// </summary>
    public byte? GetReplicaIdForConnection(ConnectionId connectionId)
    {
        foreach (var (replicaId, state) in _replicaConnections)
        {
            if (state.ConnectionId.HasValue && state.ConnectionId.Value.Equals(connectionId))
            {
                return replicaId;
            }
        }

        return null;
    }

    /// <summary>
    /// Establishes connections to all replicas.
    /// </summary>
    public async Task ConnectToAllReplicasAsync()
    {
        var tasks = new List<Task>();
        foreach (var replicaId in _replicaEndpoints.Keys.Where(id => id != _localReplicaId))
        {
            tasks.Add(ConnectToReplicaAsync(replicaId));
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Establishes a connection to a specific replica.
    /// </summary>
    public async Task<ConnectionId?> ConnectToReplicaAsync(byte replicaId)
    {
        if (replicaId == _localReplicaId)
        {
            return null;
        }

        if (!_replicaConnections.TryGetValue(replicaId, out var state))
        {
            Log.Warning("Attempted to connect to unknown replica {ReplicaId}", replicaId);
            return null;
        }

        // Don't try to connect if already connected
        if (state.ConnectionId.HasValue)
        {
            return state.ConnectionId;
        }

        // Try to connect
        try
        {
            var connectionId = await _connectFunc(state.Endpoint, true, replicaId, _lifetimeCts.Token);
            if (connectionId.HasValue)
            {
                state.ConnectionId = connectionId;
                state.LastConnectAttempt = DateTime.UtcNow;
                state.ConsecutiveFailures = 0;
                Log.Information("Successfully connected to replica {ReplicaId} at {Endpoint}", replicaId,
                    state.Endpoint);
                return connectionId;
            }
        }
        catch (Exception ex)
        {
            state.ConsecutiveFailures++;
            state.LastConnectAttempt = DateTime.UtcNow;
            Log.Error(ex, "Failed to connect to replica {ReplicaId} at {Endpoint} (Attempt {Attempts})",
                replicaId, state.Endpoint, state.ConsecutiveFailures);

            // Schedule reconnection
            _ = Task.Run(() => ReconnectWithBackoffAsync(replicaId));
        }

        return null;
    }

    /// <summary>
    /// Handles a connection closure for a replica.
    /// </summary>
    public void HandleReplicaConnectionClosed(ConnectionId connectionId, byte replicaId)
    {
        if (_replicaConnections.TryGetValue(replicaId, out var state) &&
            state.ConnectionId.HasValue && state.ConnectionId.Value.Equals(connectionId))
        {
            state.ConnectionId = null;
            Log.Information("Connection to replica {ReplicaId} closed, scheduling reconnection", replicaId);

            // Schedule reconnection
            _ = Task.Run(() => ReconnectWithBackoffAsync(replicaId));
        }
    }

    /// <summary>
    /// Reconnects to a replica with exponential backoff.
    /// </summary>
    private async Task ReconnectWithBackoffAsync(byte replicaId)
    {
        if (!_replicaConnections.TryGetValue(replicaId, out var state))
        {
            return;
        }

        // Prevent multiple concurrent reconnection attempts to the same replica
        if (!await _reconnectSemaphore.WaitAsync(0))
        {
            return;
        }

        try
        {
            // Calculate delay based on consecutive failures
            int delayMs = CalculateBackoffDelayMs(state.ConsecutiveFailures);

            Log.Information("Scheduling reconnection to replica {ReplicaId} in {DelayMs}ms (Attempt {Attempts})",
                replicaId, delayMs, state.ConsecutiveFailures + 1);

            // Wait before attempting reconnection
            await Task.Delay(delayMs, _lifetimeCts.Token);

            if (_isDisposed || _lifetimeCts.IsCancellationRequested)
            {
                return;
            }

            // Attempt reconnection
            await ConnectToReplicaAsync(replicaId);
        }
        catch (OperationCanceledException) when (_lifetimeCts.IsCancellationRequested)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error during reconnection attempt to replica {ReplicaId}", replicaId);

            // Increment failure count and schedule another attempt
            if (_replicaConnections.TryGetValue(replicaId, out state))
            {
                state.ConsecutiveFailures++;
                _ = Task.Run(() => ReconnectWithBackoffAsync(replicaId));
            }
        }
        finally
        {
            _reconnectSemaphore.Release();
        }
    }

    /// <summary>
    /// Calculates a backoff delay based on consecutive failures.
    /// </summary>
    private int CalculateBackoffDelayMs(int consecutiveFailures)
    {
        // Starting with 100ms
        int baseDelay = 100;

        // Exponential backoff with randomization
        int maxDelay = Math.Min(30000, baseDelay * (int)Math.Pow(1.5, Math.Min(20, consecutiveFailures)));

        // Add jitter (±20%)
        double jitterFactor = 0.8 + (new Random().NextDouble() * 0.4);

        return (int)(maxDelay * jitterFactor);
    }

    /// <summary>
    /// Disconnects all replica connections.
    /// </summary>
    public async Task DisconnectAllAsync()
    {
        var tasks = new List<Task>();
        foreach (var state in _replicaConnections.Values)
        {
            if (state.ConnectionId.HasValue)
            {
                tasks.Add(_disconnectFunc(state.ConnectionId.Value));
                state.ConnectionId = null;
            }
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }
    }

    /// <summary>
    /// Disposes resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
        {
            return;
        }

        _isDisposed = true;
        await _lifetimeCts.CancelAsync();
        await DisconnectAllAsync();
        _reconnectSemaphore.Dispose();
        _lifetimeCts.Dispose();
    }

    /// <summary>
    /// Represents the state of a connection to a replica.
    /// </summary>
    private class ReplicaState
    {
        public IPEndPoint Endpoint { get; }
        public ConnectionId? ConnectionId { get; set; }
        public DateTime LastConnectAttempt { get; set; } = DateTime.MinValue;
        public int ConsecutiveFailures { get; set; }

        public ReplicaState(IPEndPoint endpoint)
        {
            Endpoint = endpoint;
        }
    }
}