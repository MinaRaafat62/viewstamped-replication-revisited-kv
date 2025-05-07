using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;

namespace VsrReplica.VsrCore.Handlers;

public class RecoveryResponseHandler : IVsrCommandHandler
{
    public Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        throw new NotImplementedException();
    }
}