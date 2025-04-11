using VsrReplica.Networking.MemoryPool;

namespace VsrTests.VsrReplicaTests.NetworkingTests.MemoryPoolTests;

internal class TestablePinnedBlockMemoryPool : PinnedBlockMemoryPool
{
    public MemoryPoolBlock ReturnedBlock { get; private set; } = null!;
    public int ReturnCallCount { get; private set; }
    private bool IsDisposed { get; set; } // Track disposal

    public override void Return(MemoryPoolBlock block)
    {
        if (!IsDisposed) // Only track if not disposed
        {
            ReturnedBlock = block;
            ReturnCallCount++;
        }
    }

    protected override void Dispose(bool disposing)
    {
        IsDisposed = true; // Mark as disposed
    }
}