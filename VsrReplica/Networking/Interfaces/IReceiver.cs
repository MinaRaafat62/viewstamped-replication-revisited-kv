namespace VsrReplica.Networking.Interfaces;

public interface IReceiver : IDisposable
{
    ValueTask<int> ReceiveAsync(ISocket socket, Memory<byte> memory);
}