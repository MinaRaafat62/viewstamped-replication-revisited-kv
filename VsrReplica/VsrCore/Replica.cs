using System.IO.Pipelines;
using System.Net;
using Serilog;
using VsrReplica.Networking;
using VsrReplica.Networking.Interfaces;
using VsrReplica.Networking.MemoryPool;
using VsrReplica.Networking.Transport;
using VsrReplica.VsrCore.Application;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Timers;

namespace VsrReplica.VsrCore;

public class Replica : IDisposable
{
    private readonly INetworkManager _networkManager;
    private readonly ApplicationPipeline<VsrMessage> _applicationPipeline;
    private readonly ReplicaState _state;
    private readonly Dictionary<byte, IPEndPoint> _replicaMap;
    private readonly IReplicaTimer _primaryMonitorTimer;
    private readonly IReplicaTimer _primaryIdleCommitTimer;
    private readonly CancellationTokenSource _cts = new();
    private readonly ReplicaLifecycleManager _lifecycleManager;
    private readonly IReplicaContext _replicaContext;


    public Replica(NetworkProtocol protocol, Dictionary<byte, IPEndPoint> replicaMap)
    {
        if (protocol == NetworkProtocol.Udp)
        {
            throw new NotImplementedException("UDP protocol is not implemented yet.");
        }

        _applicationPipeline = new ApplicationPipeline<VsrMessage>();
        ReplicaHandlers.RegisterCommandHandlers();
        var memoryPool = new PinnedBlockMemoryPool();
        var senderPool = new SenderPool(() => new Sender(), 200); // Factory for Sender
        var transportScheduler = new IoQueue();
        var applicationScheduler = PipeScheduler.ThreadPool;
        var serializer = new VsrMessageSerializer();
        byte? selfReplicaId = null;
        foreach (var kvp in replicaMap.Where(kvp => Equals(kvp.Value, NetworkConfig.Replica)))
        {
            selfReplicaId = kvp.Key;
            break;
        }

        if (!selfReplicaId.HasValue)
        {
            throw new InvalidOperationException(
                $"Could not find own endpoint {NetworkConfig.Replica} in the provided replica map.");
        }


        _replicaMap = replicaMap;

        _networkManager = new TcpNetworkManager<VsrMessage>(
            NetworkConfig.Replica,
            senderPool,
            transportScheduler,
            applicationScheduler,
            memoryPool,
            serializer,
            HandleMessageReceived,
            HandleProcessingError,
            HandleConnectionClosed,
            HandleConnectionFailed,
            replicaMap
        );
        _state = new ReplicaState(
            replica: selfReplicaId.Value,
            totalReplicas: (byte)replicaMap.Count,
            new KvStore(),
            memoryPool);

        _replicaContext = new ReplicaContext(
            _state,
            _applicationPipeline,
            _networkManager);

        Action<InternalEventPipelineItem> enqueueAction = (pipelineMsg) =>
            _applicationPipeline.EnqueueInternalEventAsync(pipelineMsg.Type).AsTask().Wait();

        _primaryMonitorTimer = new PrimaryMonitorTimer(_state.Replica, enqueueAction);
        _primaryIdleCommitTimer = new PrimaryIdleCommitTimer(_state.Replica, enqueueAction);
        _lifecycleManager = new ReplicaLifecycleManager(
            _state,
            _primaryMonitorTimer,
            _primaryIdleCommitTimer,
            _replicaContext
        );
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var message = await _applicationPipeline.DequeueItemAsync(cancellationToken);
            try
            {
                bool statePotentiallyChanged;
                switch (message.Type)
                {
                    case PipelineMessageType.NetworkMessage:
                    {
                        var networkMessage = ((NetworkPipelineItem)message);
                        var connectionId = networkMessage.ConnectionId;
                        var vsrMessage = networkMessage.Message;
                        var handler = ReplicaHandlers.GetCommandHandler(vsrMessage.Header.Command);
                        await handler.HandleCommandAsync(vsrMessage, connectionId, _replicaContext);
                        _lifecycleManager.NotifyActivity(networkMessage.Message.Header.Replica,
                            networkMessage.Message.Header.Command);
                        statePotentiallyChanged = true;
                        break;
                    }
                    case PipelineMessageType.PrimaryTimeout:
                    {
                        //await _lifecycleManager.HandlePrimaryTimeout();
                        statePotentiallyChanged = true;
                        break;
                    }
                    case PipelineMessageType.SendIdleCommit:
                    {
                        await _lifecycleManager.HandleSendIdleCommit();
                        statePotentiallyChanged = false;
                        break;
                    }
                    case PipelineMessageType.PrepareSent:
                    {
                        statePotentiallyChanged = false;
                        break;
                    }
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (statePotentiallyChanged)
                {
                    _lifecycleManager.UpdateTimerStates();
                }
            }
            finally
            {
                message?.Dispose();
            }
        }
    }

    private async ValueTask HandleMessageReceived(ConnectionId connectionId, VsrMessage message)
    {
        Log.Debug("Replica Received a new Message {Message}", message);
        await _applicationPipeline.EnqueueNetworkMessageAsync(message, connectionId);
    }

    private ValueTask HandleProcessingError(ConnectionId connectionId, Exception ex)
    {
        Log.Error(ex, "App: Processing error on connection {ConnId}", connectionId);
        return ValueTask.CompletedTask;
    }

    private ValueTask HandleConnectionClosed(ConnectionId connectionId, IPEndPoint? endpoint, Exception? ex)
    {
        Log.Error(ex, "App: Connection error on {EndPoint}", endpoint);
        _lifecycleManager.UpdateTimerStates();
        return ValueTask.CompletedTask;
    }

    private ValueTask HandleConnectionFailed(IPEndPoint endpoint, Exception exception)
    {
        Log.Information("App: Connection established to {EndPoint}", endpoint);
        _lifecycleManager.UpdateTimerStates();
        return ValueTask.CompletedTask;
    }


    public void Start(CancellationToken cancellationToken = default)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var token = linkedCts.Token;
        _ = _networkManager.StartAsync();

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(1000, token);

                if (_state.TotalReplicas > 1)
                {
                    Log.Information("Replica {ReplicaId}: Connecting to other replicas...", _state.Replica);
                    var connectTasks = new List<Task>();
                    if (connectTasks == null) throw new ArgumentNullException(nameof(connectTasks));
                    foreach (var kvp in _replicaMap.Where(kvp => kvp.Key != _state.Replica))
                    {
                        if (token.IsCancellationRequested) break;
                        Log.Debug("Replica {ReplicaId}: Attempting connection to Replica {TargetId} at {EndPoint}",
                            _state.Replica, kvp.Key, kvp.Value);
                        connectTasks.Add(_networkManager.ConnectAsync(kvp.Value, token));
                    }
                }

                // Initial timer state update after attempting connections
                _lifecycleManager.UpdateTimerStates();
            }
            catch (OperationCanceledException)
            {
                Log.Information("Replica {ReplicaId}: Connection attempts cancelled.", _state.Replica);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Replica {ReplicaId}: Error during initial connection attempts.", _state.Replica);
            }
        }, token);


        _ = ProcessMessagesAsync(token);
    }

    public void Dispose()
    {
        Log.Information("Replica {ReplicaId}: Disposing...", _state.Replica);
        if (!_cts.IsCancellationRequested)
        {
            _cts.Cancel();
        }

        _lifecycleManager?.Dispose();
        _networkManager?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(5));
        _applicationPipeline?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(1));
        GC.SuppressFinalize(this);
        _cts.Dispose();
        Log.Information("Replica {ReplicaId}: Disposal complete.", _state.Replica);
        GC.SuppressFinalize(this);
    }
}

public enum NetworkProtocol
{
    Tcp,
    Udp
}