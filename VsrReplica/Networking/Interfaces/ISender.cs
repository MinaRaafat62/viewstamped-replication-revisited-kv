using System.Buffers;
using System.Net.Sockets;

namespace VsrReplica.Networking.Interfaces;

public interface ISender : IDisposable
{
    // Methods required by Connection (or other consumers)
    ValueTask<int> SendAsync(ISocket socket, in ReadOnlyMemory<byte> data);
    ValueTask<int> SendAsync(ISocket socket, in ReadOnlySequence<byte> data);

    // Method required by SenderPool
    void Reset();
    
}