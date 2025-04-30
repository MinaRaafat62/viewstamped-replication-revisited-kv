using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.ReplicaInternals;

public interface IReplicaContext
{
    public ReplicaState State { get; }
    public Task SendAsync(ConnectionId connectionId, SerializedMessage message);
    public Task BroadcastAsync(SerializedMessage message);
    public Task EnqueueInternalMessageAsync(PipelineMessageType type);
    public ConnectionId? GetConnectionIdForReplica(byte replicaId);
}