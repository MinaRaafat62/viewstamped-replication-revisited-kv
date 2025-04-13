using System.Collections.Concurrent;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.Networking.Transport;

public class SenderPool(Func<ISender> senderFactory, int maxNumberOfSenders = 128) : ISenderPool
{
    private int _count;
    private readonly ConcurrentQueue<ISender> _senders = new();
    private bool _disposed;

    public ISender Rent()
    {
        if (_senders.TryDequeue(out var sender))
        {
            Interlocked.Decrement(ref _count);
            return sender;
        }

        return senderFactory();
    }

    public void Return(ISender sender)
    {
        if (_disposed || _count >= maxNumberOfSenders)
        {
            sender.Dispose();
        }
        else
        {
            Interlocked.Increment(ref _count);
            sender.Reset();
            _senders.Enqueue(sender);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        while (_senders.TryDequeue(out var sender))
        {
            sender.Dispose();
        }
    }
}