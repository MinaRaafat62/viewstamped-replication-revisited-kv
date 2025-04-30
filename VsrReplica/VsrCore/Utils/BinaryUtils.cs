using System.Buffers;
using System.Buffers.Binary;

namespace VsrReplica.VsrCore.Utils;

public class BinaryUtils
{
    public static UInt128 BytesToUInt128BigEndian(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 16)
            throw new ArgumentException("Byte array must be at least 16 bytes.", nameof(bytes));
        var high = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        var low = BinaryPrimitives.ReadUInt64BigEndian(bytes[8..]);
        return new UInt128(high, low);
    }

    public static uint ReadUInt32BigEndian(ReadOnlySequence<byte> source)
    {
        if (source.IsSingleSegment)
        {
            return BinaryPrimitives.ReadUInt32BigEndian(source.FirstSpan);
        }

        Span<byte> buffer = stackalloc byte[4];
        source.CopyTo(buffer);
        return BinaryPrimitives.ReadUInt32BigEndian(buffer);
    }

    public static UInt128 NewGuidUInt128()
    {
        var guid = Guid.NewGuid().ToByteArray();
        ulong high = BitConverter.ToUInt64(guid, 0);
        ulong low = BitConverter.ToUInt64(guid, 8);
        return new UInt128(high, low);
    }
}