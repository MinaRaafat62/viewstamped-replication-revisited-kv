using System.Net.Sockets;
using System.Threading.Tasks.Sources;

namespace VsrReplica.Networking.Transport;

public class AwaitableEventArgs()
    : SocketAsyncEventArgs(unsafeSuppressExecutionContextFlow: true), IValueTaskSource<int>
{
    protected ManualResetValueTaskSourceCore<int> _source;

    protected override void OnCompleted(SocketAsyncEventArgs args)
    {
        if (SocketError != SocketError.Success)
        {
            _source.SetException(new SocketException((int)SocketError));
        }
        else
        {
            _source.SetResult(BytesTransferred);
        }
    }

    public int GetResult(short token)
    {
        var result = _source.GetResult(token);
        _source.Reset();
        return result;
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
        return _source.GetStatus(token);
    }

    public void OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _source.OnCompleted(continuation, state, token, flags);
    }
}