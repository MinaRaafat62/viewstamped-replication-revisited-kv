using Serilog;
using VsrReplica.Networking;
using VsrReplica.Networking.Interfaces;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.ReplicaInternals;

public class ReplicaContext(
    ReplicaState state,
    ApplicationPipeline<VsrMessage> applicationPipeline,
    INetworkManager manager,
    Func<Task> initiateRecoveryCallback)
    : IReplicaContext
{
    public ReplicaState State { get; set; } = state;

    public async Task SendAsync(ConnectionId connectionId, SerializedMessage message)
    {
        await manager.SendAsync(connectionId, message.Memory).ConfigureAwait(false);
    }

    public async Task BroadcastAsync(SerializedMessage message)
    {
        var currentReplicaConnections = manager.GetOtherReplicasConnectionIds();
        if (currentReplicaConnections.Count > 0)
        {
            await manager.BroadcastAsync(currentReplicaConnections, message.Memory).ConfigureAwait(false);
        }
        else
        {
            Log.Debug("ReplicaContext: Broadcast requested, but no replica connections found by NetworkManager.");
        }
    }

    public async Task EnqueueInternalMessageAsync(PipelineMessageType type)
    {
        await applicationPipeline.EnqueueInternalEventAsync(type).ConfigureAwait(false);
    }

    public ConnectionId? GetConnectionIdForReplica(byte replicaId)
    {
        return manager.GetConnectionIdForReplica(replicaId);
    }

    public async Task InitiateRecoveryAsync()
    {
        await initiateRecoveryCallback();
    }
}