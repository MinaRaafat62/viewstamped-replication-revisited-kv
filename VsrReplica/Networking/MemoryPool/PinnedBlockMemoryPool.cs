using System.Buffers;
using System.Collections.Concurrent;

namespace VsrReplica.Networking.MemoryPool;

public class PinnedBlockMemoryPool : MemoryPool<byte>
{
    public const int BlockSize = 4096;
    public override int MaxBufferSize { get; } = BlockSize;

    private readonly ConcurrentQueue<MemoryPoolBlock> _blocks = new();

    private bool _isDisposed; // To detect redundant calls

    private readonly object _disposeSync = new();


    private const int AnySize = -1;

    public override IMemoryOwner<byte> Rent(int size = AnySize)
    {
        return _blocks.TryDequeue(out var block)
            ?
            // block successfully taken from the stack - return it
            block
            : new MemoryPoolBlock(this, BlockSize);
    }

    public virtual void Return(MemoryPoolBlock block)
    {
        if (!_isDisposed)
        {
            _blocks.Enqueue(block);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_isDisposed)
        {
            return;
        }

        lock (_disposeSync)
        {
            _isDisposed = true;

            if (!disposing) return;
            // Discard blocks in pool
            while (_blocks.TryDequeue(out _))
            {
            }
        }
    }
}