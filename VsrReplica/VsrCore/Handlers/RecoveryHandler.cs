using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;

namespace VsrReplica.VsrCore.Handlers;

public class RecoveryHandler : IVsrCommandHandler
{
    public Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        Log.Information("Replica {ReplicaId} received Recovery command from {SenderReplica}",
            context.State.Replica, message.Header.Replica);
        throw new NotImplementedException();
    }
}