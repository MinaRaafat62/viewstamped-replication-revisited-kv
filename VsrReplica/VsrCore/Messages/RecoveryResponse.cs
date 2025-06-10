using Serilog;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class RecoveryResponse(UInt128 nonce, bool isPrimary, List<LogEntry> logs)
{
    public UInt128 Nonce { get; } = nonce;
    public bool IsPrimary { get; } = isPrimary;
    public List<LogEntry> Logs { get; } = logs;

    private const int FixedSizePart = 16 + 1 + 4;


    public static int CalculateSerializedSize(RecoveryResponse payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return FixedSizePart + payload.Logs.Sum(LogEntry.CalculateSerializedSize);
    }

    public static int Serialize(RecoveryResponse payload, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var requiredSize = CalculateSerializedSize(payload);
        if (destination.Length < requiredSize)
        {
            throw new ArgumentException($"Destination span too small. Need {requiredSize}, got {destination.Length}.",
                nameof(destination));
        }

        var writer = new BinaryUtils.SpanWriter(destination);

        // Write fixed fields
        writer.WriteUInt128BigEndian(payload.Nonce);
        writer.WriteByte(payload.IsPrimary ? (byte)1 : (byte)0);
        writer.WriteUInt32BigEndian((uint)payload.Logs.Count);

        // Write log entries
        foreach (var entry in payload.Logs)
        {
            var entrySlice = writer.RemainingSpan;
            var bytesWrittenForEntry = LogEntry.Serialize(entry, entrySlice);
            writer.Advance(bytesWrittenForEntry);
        }

        if (writer.BytesWritten != requiredSize)
        {
            Log.Warning(
                "RecoveryResponsePayload.Serialize: Bytes written ({Written}) does not match expected size ({Expected}).",
                writer.BytesWritten, requiredSize);
        }

        return writer.BytesWritten;
    }

    public static RecoveryResponse Deserialize(ReadOnlySpan<byte> source, out int bytesRead)
    {
        if (source.Length < FixedSizePart)
        {
            throw new ArgumentException(
                $"Source span too small for fixed fields. Need {FixedSizePart}, got {source.Length}.", nameof(source));
        }

        var reader = new BinaryUtils.SpanReader(source);

        var nonce = reader.ReadUInt128BigEndian();
        var isPrimaryByte = reader.ReadByte();
        var isPrimary = isPrimaryByte == 1;
        var logEntryCount = reader.ReadUInt32BigEndian();

        var logs = new List<LogEntry>((int)logEntryCount);
        var totalExpectedLogBytes = 0;
        for (var i = 0; i < logEntryCount; i++)
        {
            if (reader.Remaining == 0 && i < logEntryCount)
            {
                throw new ArgumentException(
                    $"Source span ended prematurely while reading log entries. Expected {logEntryCount}, read {i}.",
                    nameof(source));
            }

            var entrySlice = reader.RemainingSpan;
            var entry = LogEntry.Deserialize(entrySlice, out var bytesReadForEntry);
            logs.Add(entry);
            reader.Advance(bytesReadForEntry);
            totalExpectedLogBytes += bytesReadForEntry;
        }

        bytesRead = reader.BytesRead;
        var expectedTotalSize = FixedSizePart + totalExpectedLogBytes;
        if (bytesRead != expectedTotalSize)
        {
            Log.Warning(
                "RecoveryResponsePayload.Deserialize: Bytes read ({Read}) does not match expected size ({Expected}).",
                bytesRead, expectedTotalSize);
        }

        if (reader.Remaining > 0)
        {
            Log.Warning("RecoveryResponsePayload.Deserialize: Extra {RemainingBytes} bytes left in buffer.",
                reader.Remaining);
        }

        return new RecoveryResponse(nonce, isPrimary, logs);
    }

    public override string ToString()
    {
        return $"Nonce={Nonce}, IsPrimary={IsPrimary}, LogEntries={Logs.Count}";
    }
}