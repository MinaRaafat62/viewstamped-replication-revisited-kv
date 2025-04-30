using VsrReplica.Networking;

namespace VsrReplica.VsrCore.Messages;

public abstract class PipelineItem : IDisposable
{
    public abstract PipelineMessageType Type { get; }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}

public sealed class NetworkPipelineItem(VsrMessage message, ConnectionId connectionId) : PipelineItem
{
    public override PipelineMessageType Type => PipelineMessageType.NetworkMessage;

    public VsrMessage Message { get; } = message ?? throw new ArgumentNullException(nameof(message));
    public ConnectionId ConnectionId { get; } = connectionId;

    public override void Dispose()
    {
        Message?.Dispose(); // VsrMessage itself should implement IDisposable
        base.Dispose();
    }
}

public sealed class InternalEventPipelineItem : PipelineItem
{
    public override PipelineMessageType Type { get; }

    public InternalEventPipelineItem(PipelineMessageType eventType)
    {
        if (eventType == PipelineMessageType.NetworkMessage)
        {
            throw new ArgumentException("Use NetworkPipelineItem for network messages.", nameof(eventType));
        }

        Type = eventType;
    }
}

public enum PipelineMessageType
{
    NetworkMessage,
    PrimaryTimeout,
    SendIdleCommit,
    PrepareSent,
}