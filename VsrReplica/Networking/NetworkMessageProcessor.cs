using System.Buffers;
using System.IO.Pipelines;
using Serilog;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.Networking;

public class NetworkMessageProcessor<TMessage> where TMessage : INetworkMessage
{
    private readonly INetworkMessageSerializer<TMessage> _serializer;
    private readonly Func<ConnectionId, TMessage, ValueTask> _messageHandler;
    private readonly MemoryPool<byte> _memoryPool;

    /// <summary>
    /// Initializes a new instance of the <see cref="NetworkMessageProcessor{TMessage}"/> class.
    /// </summary>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="messageHandler">The message handler callback.</param>
    /// <param name="memoryPool">The memory pool to use.</param>
    public NetworkMessageProcessor(
        INetworkMessageSerializer<TMessage> serializer,
        Func<ConnectionId, TMessage, ValueTask> messageHandler,
        MemoryPool<byte> memoryPool)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        _memoryPool = memoryPool ?? throw new ArgumentNullException(nameof(memoryPool));
    }

    /// <summary>
    /// Processes messages from a connection pipe.
    /// </summary>
    /// <param name="connectionId">The connection ID.</param>
    /// <param name="reader">The pipe reader to read messages from.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async Task ProcessMessagesAsync(
        ConnectionId connectionId,
        PipeReader reader,
        System.Threading.CancellationToken cancellationToken = default)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult result;
                try
                {
                    result = await reader.ReadAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Graceful cancellation
                    break;
                }

                var buffer = result.Buffer;
                var consumed = buffer.Start;

                try
                {
                    if (buffer.IsEmpty && result.IsCompleted)
                    {
                        break;
                    }

                    // Process all complete messages in the buffer
                    while (_serializer.TryReadMessageSize(buffer, out var messageSize))
                    {
                        if (buffer.Length < messageSize)
                        {
                            // Not enough data for a complete message
                            break;
                        }

                        // Extract the message data
                        var messageSlice = buffer.Slice(0, messageSize);

                        // Copy to owned memory so we can process it after advancing the pipe
                        var memoryOwner = _memoryPool.Rent(messageSize);
                        messageSlice.CopyTo(memoryOwner.Memory.Span);

                        try
                        {
                            // Deserialize the message
                            var message = _serializer.Deserialize(
                                memoryOwner.Memory.Slice(0, messageSize),
                                memoryOwner);

                            // Process the message
                            await _messageHandler(connectionId, message);
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex, "Error processing message from {ConnectionId}", connectionId);
                            // Continue processing other messages
                        }

                        // Advance past this message
                        consumed = messageSlice.End;
                        buffer = buffer.Slice(messageSize);
                    }
                }
                finally
                {
                    // Tell the pipe how much we consumed and examined
                    reader.AdvanceTo(consumed, buffer.End);
                }

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Fatal error in message processing loop for {ConnectionId}", connectionId);
            throw;
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }
}