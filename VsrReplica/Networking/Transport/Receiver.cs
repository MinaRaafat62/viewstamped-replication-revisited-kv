using System.Net.Sockets;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.Networking.Transport;

public class Receiver : AwaitableEventArgs, IReceiver
{
    private short _token;

    public ValueTask<int> ReceiveAsync(ISocket socket, Memory<byte> memory)
    {
        SetBuffer(memory);
        if (socket.ReceiveAsync(this))
        {
            return new ValueTask<int>(this, _token++);
        }

        var transferred = BytesTransferred;
        var err = SocketError;
        return err == SocketError.Success
            ? new ValueTask<int>(transferred)
            : ValueTask.FromException<int>(new SocketException((int)err));
    }
}