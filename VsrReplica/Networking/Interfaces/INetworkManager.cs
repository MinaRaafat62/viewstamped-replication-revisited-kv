using System.Net;

namespace VsrReplica.Networking.Interfaces;

public interface INetworkManager : IAsyncDisposable
{
    Task StartAsync();
    Task StopAsync();
    Task<ConnectionId?> ConnectAsync(IPEndPoint endpoint, CancellationToken cancellationToken = default);
    Task DisconnectAsync(ConnectionId connectionId);
    ValueTask SendAsync(ConnectionId connectionId, ReadOnlyMemory<byte> data);
    ValueTask BroadcastAsync(IEnumerable<ConnectionId> connectionIds, ReadOnlyMemory<byte> data);
    IPEndPoint? LocalEndPoint { get; }
    List<ConnectionId> GetOtherReplicasConnectionIds();
    ConnectionId? GetConnectionIdForReplica(byte replicaId);
}