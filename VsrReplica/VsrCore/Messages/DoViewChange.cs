using Serilog;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class DoViewChange(uint latestView, List<LogEntry> logSuffix)
{
    public uint LatestView { get; } = latestView;
    public List<LogEntry> LogSuffix { get; } = logSuffix;

    public static int CalculateSerializedSize(DoViewChange payload)
    {
        return sizeof(uint) + sizeof(uint) + payload.LogSuffix.Sum(LogEntry.CalculateSerializedSize);
    }

    public static void Serialize(DoViewChange payload, Span<byte> destination)
    {
        var writer = new BinaryUtils.SpanWriter(destination);
        writer.WriteUInt32BigEndian(payload.LatestView);
        writer.WriteUInt32BigEndian((uint)payload.LogSuffix.Count);
        foreach (var entry in payload.LogSuffix)
        {
            var entrySlice = writer.RemainingSpan;
            var bytesWritten = LogEntry.Serialize(entry, entrySlice);
            writer.Advance(bytesWritten);
        }
    }

    public static DoViewChange Deserialize(ReadOnlySpan<byte> source)
    {
        var reader = new BinaryUtils.SpanReader(source);
        var latestView = reader.ReadUInt32BigEndian();
        var logEntryCount = reader.ReadUInt32BigEndian();
        var logSuffix = new List<LogEntry>((int)logEntryCount);
        for (var i = 0; i < logEntryCount; i++)
        {
            var entrySlice = reader.RemainingSpan;
            var entry = LogEntry.Deserialize(entrySlice, out var bytesRead);
            logSuffix.Add(entry);
            reader.Advance(bytesRead);
        }
        if (reader.Remaining > 0)
        {
            Log.Warning("DoViewChangePayload.Deserialize: Extra {RemainingBytes} bytes left in buffer.",
                reader.Remaining);
        }
        return new DoViewChange(latestView, logSuffix);
    }
}