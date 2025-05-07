using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;

namespace VsrReplica.VsrCore.Handlers;

public class PingHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        if (!connectionId.HasValue)
        {
            Log.Error("Replica {ReplicaId}: Received Request but ConnectionId is null. Cannot process.", context.State.Replica);
            return false;
        }
        var pongMessage = new VsrMessage(header: new VsrHeader(
                parent: message.Header.Parent, client: message.Header.Client,
                context: message.Header.Context, bodySize: 0, request: message.Header.Request,
                epoch: context.State.Epoch, view: context.State.View, op: context.State.Op,
                commit: context.State.Commit,
                cluster: context.State.Cluster, command: Command.Pong, operation: Operation.Reserved, offset: 0,
                replica: context.State.Replica, version: GlobalConfig.CurrentVersion),
            payload: Memory<byte>.Empty);

        var serializedMessage = VsrMessageSerializer.SerializeMessage(pongMessage, context.State.MemoryPool);
        await context.SendAsync(connectionId.Value, serializedMessage)
            .ConfigureAwait(false);
        return true;
    }
}