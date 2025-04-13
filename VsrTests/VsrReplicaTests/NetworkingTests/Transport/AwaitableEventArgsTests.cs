using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks.Sources;
using VsrReplica.Networking.Transport;

namespace VsrTests.VsrReplicaTests.NetworkingTests.Transport;

public class AwaitableEventArgsTests
{
    private class TestAwaitableEventArgs : AwaitableEventArgs
    {
        public SocketError TestSocketError { get; set; } = SocketError.Success;

        // Store the internal source for token access
        private ManualResetValueTaskSourceCore<int> GetSource()
        {
            var field = typeof(AwaitableEventArgs).GetField("_source", BindingFlags.NonPublic | BindingFlags.Instance);
            if (field == null) throw new InvalidOperationException("Could not find _source field.");
            return (ManualResetValueTaskSourceCore<int>)field.GetValue(this)!;
        }

        // Override OnCompleted to set SocketError just before base logic runs
        protected override void OnCompleted(SocketAsyncEventArgs args)
        {
            // Set the base property from our test value
            this.SocketError = this.TestSocketError;
            

            // Now call the base implementation which reads SocketError and BytesTransferred
            base.OnCompleted(args);
        }

        // Method to simulate completion trigger
        public void SimulateCompletion() => this.OnCompleted(this);
        

        // Expose internal source version for testing GetStatus/GetResult
        public short GetCurrentToken() => GetSource().Version;
    }

    private readonly TestAwaitableEventArgs _args = new();


    [Fact]
    public void GetResult_AfterSuccessfulCompletion_SucceedsWithoutThrowing()
    {
        // Arrange
        _args.TestSocketError = SocketError.Success;
        
        var token = _args.GetCurrentToken();

        // Act
        _args.SimulateCompletion(); // Trigger OnCompleted logic using staged SocketError
        var status = _args.GetStatus(token);
        var exception = Record.Exception(() => _args.GetResult(token)); // Get result and capture exception if any

        // Assert
        Assert.Equal(ValueTaskSourceStatus.Succeeded, status);
        Assert.Null(exception); // Crucially, assert that GetResult did NOT throw
    }

    [Fact]
    public void GetResult_AfterErrorCompletion_ThrowsSocketException()
    {
        // Arrange
        _args.TestSocketError = SocketError.NetworkUnreachable;
        var token = _args.GetCurrentToken();

        // Act
        _args.SimulateCompletion();
        var status = _args.GetStatus(token);

        // Assert
        Assert.Equal(ValueTaskSourceStatus.Faulted, status);
        var ex = Assert.Throws<SocketException>((Action)Act);
        Assert.Equal(SocketError.NetworkUnreachable, ex.SocketErrorCode);
        return;

        void Act() => _args.GetResult(token);
    }

    [Fact]
    public void GetResult_ResetsSource_ThrowsOnSecondCallWithSameToken()
    {
        // Arrange
        _args.TestSocketError = SocketError.Success; // Use success path for this test
        var token = _args.GetCurrentToken();
        _args.SimulateCompletion();

        // Act
        _args.GetResult(token); // First call resets (doesn't throw in success case)

        // Assert
        Assert.Throws<InvalidOperationException>((Action)Act);
        return;

        void Act() => _args.GetResult(token);
    }

    [Fact]
    public void GetStatus_BeforeCompletion_ReturnsPending()
    {
        // Arrange
        var token = _args.GetCurrentToken();

        // Act
        var status = _args.GetStatus(token);

        // Assert
        Assert.Equal(ValueTaskSourceStatus.Pending, status);
    }

    [Fact]
    public void OnCompleted_Callback_IsInvokedAfterCompletion()
    {
        // Arrange
        _args.TestSocketError = SocketError.Success; // Doesn't matter for this test if success/fail
        var token = _args.GetCurrentToken();
        var callbackExecuted = false;

        // Act
        _args.OnCompleted(Continuation, null, token, ValueTaskSourceOnCompletedFlags.None);
        Assert.False(callbackExecuted); // Callback should not run yet

        _args.SimulateCompletion(); // Now trigger completion

        // Assert
        Assert.True(callbackExecuted); // Callback should have run
        return;

        void Continuation(object? state) => callbackExecuted = true;
    }
}