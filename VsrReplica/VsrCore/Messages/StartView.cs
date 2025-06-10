using Serilog;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class StartView(List<LogEntry> logSuffix)
{
    public List<LogEntry> LogSuffix { get; } = logSuffix;

    public static int CalculateSerializedSize(StartView payload)
    {
        return sizeof(uint) + payload.LogSuffix.Sum(LogEntry.CalculateSerializedSize);
    }

    public static void Serialize(StartView payload, Span<byte> destination)
    {
        var writer = new BinaryUtils.SpanWriter(destination);

        writer.WriteUInt32BigEndian((uint)payload.LogSuffix.Count);

        foreach (var entry in payload.LogSuffix)
        {
            var entrySlice = writer.RemainingSpan;
            var bytesWritten = LogEntry.Serialize(entry, entrySlice);
            writer.Advance(bytesWritten);
        }
    }

    public static StartView Deserialize(ReadOnlySpan<byte> source)
    {
        var reader = new BinaryUtils.SpanReader(source);
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
            Log.Warning("StartViewPayload.Deserialize: Extra {RemainingBytes} bytes left in buffer.", reader.Remaining);
        }

        return new StartView(logSuffix);
    }
}