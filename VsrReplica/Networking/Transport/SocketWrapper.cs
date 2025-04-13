using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.Networking.Transport;

public class SocketWrapper(Socket socket) : ISocket
{
    private readonly Socket _socket = socket ?? throw new ArgumentNullException(nameof(socket));

    public bool SendAsync(SocketAsyncEventArgs e)
    {
        return _socket.SendAsync(e);
    }

    public void Dispose()
    {
        _socket.Dispose();
        GC.SuppressFinalize(this);
    }

    public bool ReceiveAsync([NotNull] SocketAsyncEventArgs e)
    {
        // Delegate to the wrapped socket's ReceiveAsync
        return _socket.ReceiveAsync(e);
    }

    public void Shutdown(SocketShutdown how)
    {
        try
        {
            _socket.Shutdown(how);
        }
        catch (ObjectDisposedException)
        {
            // Ignore if already disposed, Shutdown in Connection handles the lock
        }
        catch (SocketException ex) when (ex.SocketErrorCode == SocketError.NotConnected)
        {
            // Ignore if not connected (already effectively shut down)
        }
    }
}