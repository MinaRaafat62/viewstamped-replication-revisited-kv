using System.Threading.Channels;
using VsrReplica.Networking;
using VsrReplica.Networking.Interfaces;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore;

public class ApplicationPipeline<TMessage> : IAsyncDisposable where TMessage : INetworkMessage
{
    private readonly Channel<PipelineItem> _channel;
    private readonly ChannelWriter<PipelineItem> _writer;
    private ChannelReader<PipelineItem> Reader { get; }

    public ApplicationPipeline()
    {
        var options = new UnboundedChannelOptions()
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        };
        _channel = Channel.CreateUnbounded<PipelineItem>(options);
        _writer = _channel.Writer;
        Reader = _channel.Reader;
    }

    public async Task EnqueueNetworkMessageAsync(VsrMessage message, ConnectionId connectionId)
    {
        var pipelineItem = new NetworkPipelineItem(message, connectionId);
        await _writer.WriteAsync(pipelineItem);
    }

    public async ValueTask EnqueueInternalEventAsync(PipelineMessageType eventType)
    {
        var pipelineItem = new InternalEventPipelineItem(eventType);
        await _writer.WriteAsync(pipelineItem);
    }

    public async Task<PipelineItem> DequeueItemAsync(CancellationToken cancellationToken = default)
    {
        return await Reader.ReadAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _writer.Complete();
        while (_channel.Reader.TryRead(out var item))
        {
            item?.Dispose();
        }

        return ValueTask.CompletedTask;
    }
}