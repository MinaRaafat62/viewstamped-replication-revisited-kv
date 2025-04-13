using Moq;
using VsrReplica.Networking.Interfaces;
using VsrReplica.Networking.Transport;

namespace VsrTests.VsrReplicaTests.NetworkingTests.Transport;

public class SenderPoolTests : IDisposable
{
    private readonly Mock<Func<ISender>> _mockSenderFactory;
    private readonly SenderPool _pool;
    private const int MaxPoolSize = 2;

    public SenderPoolTests()
    {
        // Create a mock factory
        _mockSenderFactory = new Mock<Func<ISender>>();

        // Set up the factory to return new mock senders when called
        _mockSenderFactory.Setup(f => f()).Returns(() => new Mock<ISender>().Object);

        // Create the pool with the mock factory and a small size for testing
        _pool = new SenderPool(_mockSenderFactory.Object, MaxPoolSize);
    }


    [Fact]
    public void Rent_WhenPoolIsEmpty_CreatesNewSender()
    {
        var sender = _pool.Rent();
        Assert.NotNull(sender);
    }

    [Fact]
    public void Return_And_Rent_ReusesSender()
    {
        var sender1 = _pool.Rent(); // Creates a new one
        _pool.Return(sender1);
        var sender2 = _pool.Rent(); // Should reuse sender1

        // Verify Reset was called upon return
        Mock.Get(sender1).Verify(s => s.Reset(), Times.Once);
        Assert.Same(sender1, sender2);
    }

    [Fact]
    public void Rent_WhenPoolIsEmpty_CallsFactoryAndReturnsNewSender()
    {
        // Arrange
        var mockSender = new Mock<ISender>();
        _mockSenderFactory.Setup(f => f()).Returns(mockSender.Object); // Make factory return this specific mock

        // Act
        var sender = _pool.Rent();

        // Assert
        Assert.NotNull(sender);
        Assert.Same(mockSender.Object, sender); // Ensure it's the one from the factory
        _mockSenderFactory.Verify(f => f(), Times.Once); // Verify the factory was called exactly once
    }

    [Fact]
    public void Return_And_Rent_ReusesSenderWithoutCallingFactory()
    {
        // Arrange
        var mockSenderToReturn = new Mock<ISender>(); // Create a mock sender manually

        // Act
        _pool.Return(mockSenderToReturn.Object); // Return the mock sender
        var rentedSender = _pool.Rent(); // Rent it back out

        // Assert
        Assert.Same(mockSenderToReturn.Object, rentedSender); // Should be the same instance
        mockSenderToReturn.Verify(s => s.Reset(), Times.Once); // Verify Reset was called on return
        _mockSenderFactory.Verify(f => f(), Times.Never); // Factory should NOT have been called
    }

    [Fact]
    public void Return_WhenPoolIsFull_DisposesSender()
    {
        // Arrange
        var sender1 = new Mock<ISender>();
        var sender2 = new Mock<ISender>();
        var sender3 = new Mock<ISender>(); // The one that will be disposed

        // Act
        _pool.Return(sender1.Object); // Fill the pool (max size 2)
        _pool.Return(sender2.Object);
        _pool.Return(sender3.Object); // Return one more than capacity

        // Assert
        sender1.Verify(s => s.Reset(), Times.Once);
        sender1.Verify(s => s.Dispose(), Times.Never); // Should not be disposed

        sender2.Verify(s => s.Reset(), Times.Once);
        sender2.Verify(s => s.Dispose(), Times.Never); // Should not be disposed

        sender3.Verify(s => s.Reset(), Times.Never); // Should not be reset if immediately disposed
        sender3.Verify(s => s.Dispose(), Times.Once); // Should be disposed as pool is full
    }

    [Fact]
    public void Return_WhenPoolIsDisposed_DisposesSender()
    {
        // Arrange
        var senderToReturn = new Mock<ISender>();
        _pool.Dispose(); // Dispose the pool first

        // Act
        _pool.Return(senderToReturn.Object);

        // Assert
        senderToReturn.Verify(s => s.Reset(), Times.Never);
        senderToReturn.Verify(s => s.Dispose(), Times.Once); // Disposed because pool is disposed
    }

    [Fact]
    public void Dispose_DisposesPooledSenders()
    {
        // Arrange
        var sender1 = new Mock<ISender>();
        var sender2 = new Mock<ISender>();
        _pool.Return(sender1.Object);
        _pool.Return(sender2.Object);

        // Act
        _pool.Dispose();

        // Assert
        sender1.Verify(s => s.Dispose(), Times.Once);
        sender2.Verify(s => s.Dispose(), Times.Once);
    }


    public void Dispose()
    {
        _pool.Dispose();
    }
}