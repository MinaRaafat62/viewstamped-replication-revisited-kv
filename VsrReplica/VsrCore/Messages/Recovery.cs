using Serilog;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class Recovery(UInt128 nonce)
{
    public UInt128 Nonce { get; } = nonce;

    private const int FixedSize = 16;

    public static int CalculateSerializedSize(Recovery recovery) => FixedSize;

    public static int Serialize(Recovery payload, Span<byte> destination)
    {
        if (destination.Length < FixedSize)
        {
            throw new ArgumentException($"Destination span too small. Need {FixedSize}, got {destination.Length}.",
                nameof(destination));
        }

        var writer = new BinaryUtils.SpanWriter(destination);
        writer.WriteUInt128BigEndian(payload.Nonce);

        if (writer.BytesWritten != FixedSize)
        {
            Log.Warning(
                "RecoveryPayload.Serialize: Bytes written ({Written}) does not match expected size ({Expected}).",
                writer.BytesWritten, FixedSize);
        }

        return writer.BytesWritten;
    }

    public static Recovery Deserialize(ReadOnlySpan<byte> source, out int bytesRead)
    {
        if (source.Length < FixedSize)
        {
            throw new ArgumentException($"Source span too small. Need {FixedSize}, got {source.Length}.",
                nameof(source));
        }

        var reader = new BinaryUtils.SpanReader(source);
        var nonce = reader.ReadUInt128BigEndian();

        bytesRead = reader.BytesRead;
        if (bytesRead != FixedSize)
        {
            Log.Warning("RecoveryPayload.Deserialize: Bytes read ({Read}) does not match expected size ({Expected}).",
                bytesRead, FixedSize);
        }

        if (reader.Remaining > 0)
        {
            Log.Warning("RecoveryPayload.Deserialize: Extra {RemainingBytes} bytes left in buffer.", reader.Remaining);
        }

        return new Recovery(nonce);
    }

    public override string ToString()
    {
        return $"Nonce={Nonce}";
    }
}