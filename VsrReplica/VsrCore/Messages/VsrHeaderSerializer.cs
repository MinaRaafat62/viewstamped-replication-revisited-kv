using System.Buffers.Binary;

namespace VsrReplica.VsrCore.Messages;

public class VsrHeaderSerializer
{
    public static void Serialize(VsrHeader header, Span<byte> destination)
    {
        if (destination.Length < 128)
            throw new ArgumentException("Destination buffer too small for header");

        if (header.ChecksumBody == 0)
        {
            throw new InvalidOperationException("ChecksumBody must be set");
        }

        var position = 0;

        // Write all fields in order
        BinaryPrimitives.WriteUInt128BigEndian(destination.Slice(position, 16), header.Checksum);
        position += 16;

        BinaryPrimitives.WriteUInt128BigEndian(destination.Slice(position, 16), header.ChecksumBody);
        position += 16;

        BinaryPrimitives.WriteUInt128BigEndian(destination.Slice(position, 16), header.Parent);
        position += 16;

        BinaryPrimitives.WriteUInt128BigEndian(destination.Slice(position, 16), header.Client);
        position += 16;

        BinaryPrimitives.WriteUInt128BigEndian(destination.Slice(position, 16), header.Context);
        position += 16;

        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(position, 4), header.Request);
        position += 4;

        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(position, 4), header.Cluster);
        position += 4;

        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(position, 4), header.Epoch);
        position += 4;

        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(position, 4), header.View);
        position += 4;

        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(position, 8), header.Op);
        position += 8;

        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(position, 8), header.Commit);
        position += 8;

        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(position, 8), header.Offset);
        position += 8;

        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(position, 4), header.Size);
        position += 4;

        destination[position++] = header.Replica;
        destination[position++] = (byte)header.Command;
        destination[position++] = (byte)header.Operation;
        destination[position] = header.Version;
    }

    public static VsrHeader Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 128)
            throw new ArgumentException("Source buffer too small for header");

        var position = 0;

        var checksum = BinaryPrimitives.ReadUInt128BigEndian(source.Slice(position, 16));
        position += 16;

        var checksumBody = BinaryPrimitives.ReadUInt128BigEndian(source.Slice(position, 16));
        position += 16;

        var parent = BinaryPrimitives.ReadUInt128BigEndian(source.Slice(position, 16));
        position += 16;

        var client = BinaryPrimitives.ReadUInt128BigEndian(source.Slice(position, 16));
        position += 16;

        var context = BinaryPrimitives.ReadUInt128BigEndian(source.Slice(position, 16));
        position += 16;

        var request = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(position, 4));
        position += 4;

        var cluster = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(position, 4));
        position += 4;

        var epoch = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(position, 4));
        position += 4;

        var view = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(position, 4));
        position += 4;

        var op = BinaryPrimitives.ReadUInt64BigEndian(source.Slice(position, 8));
        position += 8;

        var commit = BinaryPrimitives.ReadUInt64BigEndian(source.Slice(position, 8));
        position += 8;

        var offset = BinaryPrimitives.ReadUInt64BigEndian(source.Slice(position, 8));
        position += 8;

        var sizeBytesSpan = source.Slice(position, 4);
        var size = BinaryPrimitives.ReadUInt32BigEndian(sizeBytesSpan);
        position += 4;


        var replica = source[position++];
        var command = (Command)source[position++];
        var operation = (Operation)source[position++];
        var version = source[position];

        var header = new VsrHeader(
            parent, client, context, bodySize: size - GlobalConfig.HeaderSize, request, cluster, epoch, view, op,
            commit, offset, replica,
            command, operation, version);

        header.SetHeaderChecksum(checksum);
        header.SetBodyChecksum(checksumBody);


        return header;
    }
}