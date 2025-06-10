using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.Application;

public interface IStateMachine
{
    byte[] Apply(Operation operation, ReadOnlySpan<byte> data);
    byte[] GetState();
    void SetState(ReadOnlySpan<byte> state);
}