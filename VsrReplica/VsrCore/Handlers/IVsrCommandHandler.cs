using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;

namespace VsrReplica.VsrCore.Handlers;

public interface IVsrCommandHandler
{
    Task<bool> HandleCommandAsync(VsrMessage message,ConnectionId? connectionId, IReplicaContext context);
}