using System.Buffers.Binary;
using Serilog;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public static class VsrHeaderSerializer
{
    public static void Serialize(VsrHeader header, Span<byte> destination)
    {
        if (destination.Length < 128)
            throw new ArgumentException("Destination buffer too small for header");

        if (header.ChecksumBody == 0 || header.Checksum == 0)
        {
            throw new InvalidOperationException("ChecksumBody must be set");
        }

        var writer = new BinaryUtils.SpanWriter(destination);
        writer.WriteUInt128BigEndian(header.Checksum);
        writer.WriteUInt128BigEndian(header.ChecksumBody);
        writer.WriteUInt128BigEndian(header.Parent);
        writer.WriteUInt128BigEndian(header.Client);
        writer.WriteUInt128BigEndian(header.Context);
        writer.WriteUInt32BigEndian(header.Request);
        writer.WriteUInt32BigEndian(header.Cluster);
        writer.WriteUInt32BigEndian(header.Epoch);
        writer.WriteUInt32BigEndian(header.View);
        writer.WriteUInt64BigEndian(header.Op);
        writer.WriteUInt64BigEndian(header.Commit);
        writer.WriteUInt64BigEndian(header.Offset);
        writer.WriteUInt32BigEndian(header.Size); // Size includes header + payload
        writer.WriteByte(header.Replica);
        writer.WriteByte((byte)header.Command);
        writer.WriteByte((byte)header.Operation);
        writer.WriteByte(header.Version);

        // (sanity check) ensure that the number of bytes written is correct
        if (writer.BytesWritten != GlobalConfig.HeaderSize)
        {
            Log.Error(
                "VsrHeaderSerializer.Serialize: Incorrect number of bytes written. Expected {Expected}, wrote {Actual}",
                GlobalConfig.HeaderSize, writer.BytesWritten);
        }
    }

    public static VsrHeader Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < GlobalConfig.HeaderSize)
            throw new ArgumentException("Source buffer too small for header");

        var reader = new BinaryUtils.SpanReader(source);
        var checksum = reader.ReadUInt128BigEndian();
        var checksumBody = reader.ReadUInt128BigEndian();
        var parent = reader.ReadUInt128BigEndian();
        var client = reader.ReadUInt128BigEndian();
        var context = reader.ReadUInt128BigEndian();
        var request = reader.ReadUInt32BigEndian();
        var cluster = reader.ReadUInt32BigEndian();
        var epoch = reader.ReadUInt32BigEndian();
        var view = reader.ReadUInt32BigEndian();
        var op = reader.ReadUInt64BigEndian();
        var commit = reader.ReadUInt64BigEndian();
        var offset = reader.ReadUInt64BigEndian();
        var size = reader.ReadUInt32BigEndian(); // Size includes header + payload
        var replica = reader.ReadByte();
        var command = (Command)reader.ReadByte();
        var operation = (Operation)reader.ReadByte();
        var version = reader.ReadByte();


        if (reader.BytesRead != GlobalConfig.HeaderSize)
        {
            Log.Error(
                "VsrHeaderSerializer.Deserialize: Incorrect number of bytes read. Expected {Expected}, read {Actual}",
                GlobalConfig.HeaderSize, reader.BytesRead);
        }

        var header = new VsrHeader(
            parent, client, context, bodySize: size - GlobalConfig.HeaderSize, request, cluster, epoch, view, op,
            commit, offset, replica,
            command, operation, version);

        header.SetHeaderChecksum(checksum);
        header.SetBodyChecksum(checksumBody);


        return header;
    }
}