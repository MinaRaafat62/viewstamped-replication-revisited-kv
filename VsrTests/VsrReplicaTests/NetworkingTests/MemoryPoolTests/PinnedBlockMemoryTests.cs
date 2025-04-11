using VsrReplica.Networking.MemoryPool;

namespace VsrTests.VsrReplicaTests.NetworkingTests.MemoryPoolTests;

public class PinnedBlockMemoryTests : IDisposable
{
    private readonly PinnedBlockMemoryPool _pool = new();

    public void Dispose()
    {
        _pool.Dispose();
    }

    [Fact]
    public void BlockSize_ShouldReturnCorrectConstant()
    {
        const int expectedBlockSize = 4096;
        var actualBlockSize = PinnedBlockMemoryPool.BlockSize;
        Assert.Equal(expectedBlockSize, actualBlockSize);
    }

    [Fact]
    public void MaxBufferSize_ShouldEqualBlockSize()
    {
        // Assert
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, _pool.MaxBufferSize);
    }

    [Fact]
    public void Rent_WhenPoolIsEmpty_ShouldCreateNewBlock()
    {
        // Act
        using var owner = _pool.Rent();

        // Assert
        Assert.NotNull(owner);
        Assert.IsType<MemoryPoolBlock>(owner); // Ensure it's the correct type
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner.Memory.Length);

        var block = (MemoryPoolBlock)owner;
        Assert.Same(_pool, block.Pool); // Ensure the block knows its pool
    }

    [Fact]
    public void Rent_WithSizeHint_ShouldReturnBlockOfBlockSize()
    {
        // Act
        using var owner = _pool.Rent(100); // Request a smaller size

        // Assert
        Assert.NotNull(owner);
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner.Memory.Length); // Should still get the standard block size
    }

    [Fact]
    public void Return_ShouldMakeBlockAvailableForRent()
    {
        // Arrange
        var owner1 = _pool.Rent();
        var block1 = (MemoryPoolBlock)owner1; // Keep reference for comparison
        var memory1 = owner1.Memory; // Get memory segment

        // Act
        owner1.Dispose(); // Returns the block to the pool
        using var owner2 = _pool.Rent(); // Rent again, should get the returned block

        // Assert
        Assert.NotNull(owner2);
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner2.Memory.Length);
        Assert.Same(block1, owner2); // Should be the exact same MemoryPoolBlock instance
        // Optional: Check if the underlying memory buffer is the same (it should be)
        Assert.True(memory1.Span == ((MemoryPoolBlock)owner2).Memory.Span);
    }

    [Fact]
    public void Rent_MultipleTimes_ShouldReturnDistinctBlocksWhenPoolEmpty()
    {
        // Act
        using var owner1 = _pool.Rent();
        using var owner2 = _pool.Rent();

        // Assert
        Assert.NotNull(owner1);
        Assert.NotNull(owner2);
        Assert.NotSame(owner1, owner2); // Should be different block instances
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner1.Memory.Length);
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner2.Memory.Length);
    }

    [Fact]
    public void Dispose_ShouldPreventBlocksFromBeingReturned()
    {
        // Arrange
        var owner1 = _pool.Rent();
        var block1 = (MemoryPoolBlock)owner1;

        // Act
        _pool.Dispose(); // Dispose the pool FIRST
        owner1.Dispose(); // Now return the block (should be ignored by the disposed pool)

        // Re-create pool to test Rent (original pool is disposed)
        using var pool2 = new PinnedBlockMemoryPool();
        using var owner2 = pool2.Rent(); // Rent from a new pool

        // Assert
        Assert.NotNull(owner2);
        Assert.NotSame(block1, owner2); // Should be a new block, not the one returned to the disposed pool
    }

    [Fact]
    public void Dispose_MultipleTimes_ShouldNotThrow()
    {
        // Act
        _pool.Dispose();
        var exception = Record.Exception(() => _pool.Dispose());

        // Assert
        Assert.Null(exception); // Should not throw on second dispose
    }

    [Fact]
    public void Rent_AfterDispose_ShouldStillAllocateNewBlock()
    {
        // Arrange
        _pool.Dispose();

        // Act
        // Rent doesn't check _isDisposed before creating a new block if queue is empty
        using var owner = _pool.Rent();

        // Assert
        Assert.NotNull(owner);
        Assert.IsType<MemoryPoolBlock>(owner);
        Assert.Equal(PinnedBlockMemoryPool.BlockSize, owner.Memory.Length);
        // The block's Pool property will point to the disposed pool instance
        Assert.Same(_pool, ((MemoryPoolBlock)owner).Pool);
    }
}