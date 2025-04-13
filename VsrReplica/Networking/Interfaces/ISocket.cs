using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;

namespace VsrReplica.Networking.Interfaces;

public interface ISocket : IDisposable
{
    bool SendAsync(SocketAsyncEventArgs e);
    bool ReceiveAsync([NotNull] SocketAsyncEventArgs e);
    void Shutdown(SocketShutdown how);
}