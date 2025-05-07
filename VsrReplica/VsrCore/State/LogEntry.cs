using Serilog;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.State;

public class LogEntry(
    ulong op,
    uint view,
    Operation operation,
    UInt128 client,
    uint request,
    byte[] payload
)
{
    public ulong Op { get; } = op;
    public uint View { get; } = view;
    public Operation Operation { get; } = operation;
    public UInt128 Client { get; } = client;
    public uint Request { get; } = request;
    public byte[] Payload { get; } = payload;

    public ReadOnlySpan<byte> PayloadSpan => Payload.AsSpan();

    private const int
        FixedSize = sizeof(ulong) + sizeof(uint) + sizeof(byte) + (16) + sizeof(uint) +
                    sizeof(uint); // 16 bytes for UInt128 and last uint for size of payload

    public static int CalculateSerializedSize(LogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return FixedSize + entry.Payload.Length;
    }

    public static int Serialize(LogEntry entry, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var requiredSize = CalculateSerializedSize(entry);
        if (destination.Length < requiredSize)
        {
            throw new ArgumentException(
                $"Destination span too small for LogEntry. Need {requiredSize}, got {destination.Length}.",
                nameof(destination));
        }

        var writer = new BinaryUtils.SpanWriter(destination);
        writer.WriteUInt64BigEndian(entry.Op);
        writer.WriteUInt32BigEndian(entry.View);
        writer.WriteByte((byte)entry.Operation);
        writer.WriteUInt128BigEndian(entry.Client);
        writer.WriteUInt32BigEndian(entry.Request);
        writer.WriteUInt32BigEndian((uint)entry.Payload.Length);
        writer.WriteBytes(entry.PayloadSpan);
        return writer.BytesWritten;
    }

    public static LogEntry Deserialize(ReadOnlySpan<byte> source, out int bytesRead)
    {
        if (source.Length < FixedSize)
        {
            throw new ArgumentException(
                $"Source span too small for fixed LogEntry fields. Need at least {FixedSize}, got {source.Length}.",
                nameof(source));
        }
        var reader = new BinaryUtils.SpanReader(source);
        var op = reader.ReadUInt64BigEndian();
        var view = reader.ReadUInt32BigEndian();
        var operation = (Operation)reader.ReadByte();
        var client = reader.ReadUInt128BigEndian();
        var request = reader.ReadUInt32BigEndian();
        var payloadLength = reader.ReadUInt32BigEndian();
        if (reader.Remaining < payloadLength)
        {
            throw new ArgumentException(
                $"Source span too small for payload. Need {payloadLength}, got {reader.Remaining}.", nameof(source));
        }
        var payload = reader.ReadBytes((int)payloadLength).ToArray();

        bytesRead = reader.BytesRead;
        
        if (bytesRead != FixedSize + payloadLength)
        {
            Log.Warning("LogEntry.Deserialize: Bytes read ({BytesRead}) does not match expected size ({ExpectedSize}).",
                bytesRead, FixedSize + payloadLength);
        }

        return new LogEntry(op, view, operation, client, request, payload);
    }


    public override string ToString()
    {
        return $"Op={Op}, View={View}, OpType={Operation}, Client={Client}, Req={Request}, PayloadLen={Payload.Length}";
    }
}