using VsrReplica.Networking.MemoryPool;

namespace VsrTests.VsrReplicaTests.NetworkingTests.MemoryPoolTests;

public class MemoryPoolBlockTests
{
    [Fact]
    public void Constructor_InitializesPropertiesAndAllocatesPinnedMemory()
    {
        // Arrange
        using var testPool = new TestablePinnedBlockMemoryPool();
        const int expectedLength = PinnedBlockMemoryPool.BlockSize; // Using the standard size

        // Act
        var block = new MemoryPoolBlock(testPool, expectedLength);

        // Assert
        Assert.Same(testPool, block.Pool); // Pool reference should be stored
        Assert.Equal(expectedLength, block.Memory.Length); // Length should match request


        // Example Check: Write and read to confirm usability
        block.Memory.Span[0] = 123;
        Assert.Equal(123, block.Memory.Span[0]);
        block.Memory.Span[expectedLength - 1] = 255;
        Assert.Equal(255, block.Memory.Span[expectedLength - 1]);

        // Cleanup (calls Dispose -> Pool.Return)
        block.Dispose();
    }

    [Fact]
    public void Dispose_CallsPoolReturnWithItself()
    {
        // Arrange
        using var testPool = new TestablePinnedBlockMemoryPool();
        // Create the block directly for this test, associating it with our test pool
        var block = new MemoryPoolBlock(testPool, PinnedBlockMemoryPool.BlockSize);

        // Act
        block.Dispose();

        // Assert
        Assert.Equal(1, testPool.ReturnCallCount); // Verify Pool.Return was called exactly once
        Assert.Same(block, testPool.ReturnedBlock); // Verify the correct block instance was passed to Return
    }

    [Fact]
    public void Memory_ProvidesAccessToUnderlyingPinnedArray()
    {
        // Arrange
        using var testPool = new TestablePinnedBlockMemoryPool();
        var block = new MemoryPoolBlock(testPool, PinnedBlockMemoryPool.BlockSize);
        var memory = block.Memory;

        // Act
        // Modify memory through the Memory<byte>
        for (var i = 0; i < memory.Length; i++)
        {
            memory.Span[i] = (byte)(i % 256);
        }

        // Assert
        // Verify the changes are reflected
        Assert.Equal(0, memory.Span[0]);
        Assert.Equal(1, memory.Span[1]);
        Assert.Equal(255, memory.Span[255]);
        Assert.Equal(0, memory.Span[256]); // Wrap around check

        // Cleanup
        block.Dispose();
    }
}