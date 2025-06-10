using System.Buffers;
using Serilog;
using VsrReplica.Networking;
using VsrReplica.Networking.Interfaces;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class VsrMessageSerializer : INetworkMessageSerializer<VsrMessage>
{
    public SerializedMessage Serialize(VsrMessage message, MemoryPool<byte> pool)
    {
        message.Header.SetBodyChecksum(message.Payload.Span);
        message.Header.SetHeaderChecksum();
        var size = GlobalConfig.HeaderSize + message.Payload.Length;
        var owner = pool.Rent(size);
        var buffer = owner.Memory;

        if (buffer.Length < size)
        {
            owner.Dispose(); // Dispose the inadequate buffer
            throw new ArgumentException($"Rented buffer too small. Needed: {size}, Got: {buffer.Length}");
        }

        var targetSpan = buffer.Span[..size];

        VsrHeaderSerializer.Serialize(message.Header, targetSpan[..GlobalConfig.HeaderSize]);
        message.Payload.Span.CopyTo(targetSpan[GlobalConfig.HeaderSize..]);
        return new SerializedMessage(buffer[..size], owner);
    }

    public VsrMessage Deserialize(ReadOnlyMemory<byte> data, IMemoryOwner<byte> owner)
    {
        if (data.Length < GlobalConfig.HeaderSize)
        {
            owner.Dispose();
            throw new ArgumentException("Data too small for a valid message");
        }

        Log.Verbose("Serializer: Received Data Hex: {DataHex}", Convert.ToHexString(data.Span));
        VsrHeader header;
        try
        {
            header = VsrHeaderSerializer.Deserialize(data.Span[..GlobalConfig.HeaderSize]);
            Log.Verbose("Serializer: Deserialized Header Body Checksum: {Checksum}", header.ChecksumBody);
        }
        catch (Exception ex)
        {
            owner.Dispose();
            Log.Error(ex, "Failed to deserialize VSR header.");
            throw new InvalidDataException("Failed to deserialize VSR header.", ex);
        }

        var payloadSlice = data[GlobalConfig.HeaderSize..];
        Log.Verbose("Serializer: Payload Slice Hex (Receiver): {PayloadHex}", Convert.ToHexString(payloadSlice.Span));
        var calculatedBodyChecksum = VsrHeader.CalculateBodyChecksum(payloadSlice.Span);
        Log.Verbose("Serializer: Recalculated Body Checksum (Receiver): {Checksum}", calculatedBodyChecksum);

        if (header.ChecksumBody != calculatedBodyChecksum)
        {
            Log.Error(
                "Body Checksum Mismatch. Header Checksum: {HeaderChecksum}, Calculated Checksum: {CalculatedChecksum}, Payload Length: {PayloadLength}",
                header.ChecksumBody, calculatedBodyChecksum, payloadSlice.Length);
            owner.Dispose();
            throw new InvalidOperationException("Invalid body checksum");
        }

        var calculatedHeaderChecksum = VsrHeader.CalculateChecksum(header);

        if (header.Checksum != calculatedHeaderChecksum)
        {
            Log.Error(
                "Header Checksum Mismatch. Header Checksum: {HeaderChecksum}, Calculated Checksum: {CalculatedChecksum}",
                header.Checksum, calculatedHeaderChecksum);
            owner.Dispose();
            throw new InvalidOperationException("Header checksum verification failed!");
        }

        var payloadMemory = owner.Memory.Slice(GlobalConfig.HeaderSize, payloadSlice.Length);
        return new VsrMessage(header, owner, payloadMemory);
    }

    public bool TryReadMessageSize(ReadOnlySequence<byte> buffer, out int size)
    {
        size = 0;

        if (buffer.Length < GlobalConfig.HeaderSize)
        {
            return false;
        }

        var sizeSpan = buffer.Slice(120, 4); // Size is at offset 120
        size = (int)BinaryUtils.ReadUInt32BigEndian(sizeSpan);
        // TODO: we need to check the maximum size of the memory pool so the message does not exceed the maximum size
        return size >= GlobalConfig.HeaderSize;
    }

    public static SerializedMessage SerializeMessage(VsrMessage message, MemoryPool<byte> pool)
    {
        message.Header.SetBodyChecksum(message.Payload.Span);
        Log.Verbose("Serializer: Calculated Body Checksum (Sender): {Checksum}", message.Header.ChecksumBody);
        message.Header.SetHeaderChecksum();
        var size = GlobalConfig.HeaderSize + message.Payload.Length;
        var owner = pool.Rent(size);
        var buffer = owner.Memory;

        if (buffer.Length < size)
        {
            owner.Dispose();
            throw new ArgumentException($"Rented buffer too small. Needed: {size}, Got: {buffer.Length}");
        }

        var targetSpan = buffer.Span[..size];

        VsrHeaderSerializer.Serialize(message.Header, targetSpan[..GlobalConfig.HeaderSize]);
        ReadOnlySpan<byte> sourcePayloadSpan = message.Payload.Span;

        var destinationPayloadSpan = targetSpan[GlobalConfig.HeaderSize..];
        Log.Verbose(
            "Serializer: Copying Payload. Source ({SourceLen} bytes): {SourceHex}, Dest Slice Start Index: {DestIndex}",
            sourcePayloadSpan.Length, Convert.ToHexString(sourcePayloadSpan), GlobalConfig.HeaderSize);

        sourcePayloadSpan.CopyTo(destinationPayloadSpan);

        Log.Verbose("Serializer: Copied Payload. Resulting Dest Slice ({DestLen} bytes): {DestHex}",
            destinationPayloadSpan.Length, Convert.ToHexString(destinationPayloadSpan));
        Log.Verbose("Serializer: Full buffer after copy ({FullLen} bytes): {FullHex}",
            targetSpan.Length, Convert.ToHexString(targetSpan));

        return new SerializedMessage(buffer[..size], owner);
    }
}