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
        var high = BitConverter.ToUInt64(guid, 0);
        var low = BitConverter.ToUInt64(guid, 8);
        return new UInt128(high, low);
    }

    internal ref struct SpanWriter(Span<byte> span)
    {
        private readonly Span<byte> _span = span;
        public int BytesWritten { get; private set; } = 0;
        public Span<byte> RemainingSpan => _span[BytesWritten..];

        public void Advance(int count)
        {
            if (count < 0 || count > RemainingSpan.Length) throw new ArgumentOutOfRangeException(nameof(count));
            BytesWritten += count;
        }

        public void WriteByte(byte value)
        {
            RemainingSpan[0] = value;
            Advance(1);
        }

        public void WriteBytes(ReadOnlySpan<byte> value)
        {
            value.CopyTo(RemainingSpan);
            Advance(value.Length);
        }

        public void WriteUInt32BigEndian(uint value)
        {
            BinaryPrimitives.WriteUInt32BigEndian(RemainingSpan, value);
            Advance(sizeof(uint));
        }

        public void WriteUInt64BigEndian(ulong value)
        {
            BinaryPrimitives.WriteUInt64BigEndian(RemainingSpan, value);
            Advance(sizeof(ulong));
        }

        public void WriteUInt128BigEndian(UInt128 value)
        {
            BinaryPrimitives.WriteUInt128BigEndian(RemainingSpan, value);
            Advance(16); // sizeof(UInt128)
        }
    }

    internal ref struct SpanReader(ReadOnlySpan<byte> span)
    {
        private readonly ReadOnlySpan<byte> _span = span;
        public int BytesRead { get; private set; } = 0;
        public ReadOnlySpan<byte> RemainingSpan => _span.Slice(BytesRead);
        public int Remaining => RemainingSpan.Length;

        public void Advance(int count)
        {
            if (count < 0 || count > RemainingSpan.Length) throw new ArgumentOutOfRangeException(nameof(count));
            BytesRead += count;
        }

        public byte ReadByte()
        {
            var value = RemainingSpan[0];
            Advance(1);
            return value;
        }

        public ReadOnlySpan<byte> ReadBytes(int length)
        {
            var value = RemainingSpan[..length];
            Advance(length);
            return value;
        }

        public uint ReadUInt32BigEndian()
        {
            var value = BinaryPrimitives.ReadUInt32BigEndian(RemainingSpan);
            Advance(sizeof(uint));
            return value;
        }

        public ulong ReadUInt64BigEndian()
        {
            var value = BinaryPrimitives.ReadUInt64BigEndian(RemainingSpan);
            Advance(sizeof(ulong));
            return value;
        }

        public UInt128 ReadUInt128BigEndian()
        {
            var value = BinaryPrimitives.ReadUInt128BigEndian(RemainingSpan);
            Advance(16); // sizeof(UInt128)
            return value;
        }
    }
}