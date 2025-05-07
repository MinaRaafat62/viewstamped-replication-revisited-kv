using System.Buffers.Binary;
using Blake3;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Messages;

public class VsrHeader(
    UInt128 parent, //still not used in my implementation
    UInt128 client,
    UInt128 context, // not used to its full potential.
    uint bodySize,
    uint request,
    uint cluster,// still not used in my implementation
    uint epoch, // still not used in my implementation
    uint view,
    ulong op,
    ulong commit,
    ulong offset,// still not used in my implementation
    byte replica,
    Command command,
    Operation operation,
    byte version)
{
    public UInt128 Checksum { get; private set; }
    public UInt128 ChecksumBody { get; private set; }
    public UInt128 Parent { get; set; } = parent;
    public UInt128 Client { get; set; } = client;
    public UInt128 Context { get; set; } = context;
    public uint Request { get; set; } = request;
    public uint Cluster { get; set; } = cluster;
    public uint Epoch { get; set; } = epoch;
    public uint View { get; set; } = view;
    public ulong Op { get; set; } = op;
    public ulong Commit { get; set; } = commit;
    public ulong Offset { get; set; } = offset;
    public uint Size { get; set; } = (GlobalConfig.HeaderSize + bodySize);
    public byte Replica { get; set; } = replica;
    public Command Command { get; set; } = command;
    public Operation Operation { get; set; } = operation;
    public byte Version { get; set; } = version;


    public static UInt128 CalculateChecksum(VsrHeader header)
    {
        if (header.ChecksumBody == 0)
        {
            throw new InvalidOperationException("ChecksumBody must be set before calculating checksum");
        }
        var hasher = Hasher.New();
        Span<byte> buffer = stackalloc byte[16];
        BinaryPrimitives.WriteUInt128BigEndian(buffer, header.ChecksumBody);
        hasher.Update(buffer);
        BinaryPrimitives.WriteUInt128BigEndian(buffer, header.Parent);
        hasher.Update(buffer);
        BinaryPrimitives.WriteUInt128BigEndian(buffer, header.Client);
        hasher.Update(buffer);
        BinaryPrimitives.WriteUInt128BigEndian(buffer, header.Context);
        hasher.Update(buffer);
        Span<byte> smallBuffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(smallBuffer[..4], header.Request);
        hasher.Update(smallBuffer[..4]);
        BinaryPrimitives.WriteUInt32BigEndian(smallBuffer[..4], header.Cluster);
        hasher.Update(smallBuffer[..4]);
        BinaryPrimitives.WriteUInt32BigEndian(smallBuffer[..4], header.Epoch);
        hasher.Update(smallBuffer[..4]);
        BinaryPrimitives.WriteUInt32BigEndian(smallBuffer[..4], header.View);
        hasher.Update(smallBuffer[..4]);
        BinaryPrimitives.WriteUInt64BigEndian(smallBuffer, header.Op);
        hasher.Update(smallBuffer);
        BinaryPrimitives.WriteUInt64BigEndian(smallBuffer, header.Commit);
        hasher.Update(smallBuffer);
        BinaryPrimitives.WriteUInt64BigEndian(smallBuffer, header.Offset);
        hasher.Update(smallBuffer);
        BinaryPrimitives.WriteUInt32BigEndian(smallBuffer[..4], header.Size);
        hasher.Update(smallBuffer[..4]);
        hasher.Update(new[] { header.Replica });
        hasher.Update(new[] { (byte)header.Command });
        hasher.Update(new[] { (byte)header.Operation });
        hasher.Update(new[] { header.Version });
        var hash = hasher.Finalize();
        return BinaryPrimitives.ReadUInt128BigEndian(hash.AsSpan()[..16]);
    }


    public void SetHeaderChecksum(UInt128 checksum)
    {
        Checksum = checksum;
    }

    public void SetHeaderChecksum()
    {
        Checksum = CalculateChecksum(this);
    }

    public void SetBodyChecksum(UInt128 checksum)
    {
        ChecksumBody = checksum;
    }

    public void SetBodyChecksum(ReadOnlySpan<byte> body)
    {
        var hash = Hasher.Hash(body);
        ChecksumBody = BinaryUtils.BytesToUInt128BigEndian(hash.AsSpan()[..16]);
    }

    public static UInt128 CalculateBodyChecksum(ReadOnlySpan<byte> body)
    {
        var hash = Hasher.Hash(body);
        var checksum = BinaryUtils.BytesToUInt128BigEndian(hash.AsSpan()[..16]);
        return checksum;
    }
    
    public override string ToString()
    {
        return $"Request: {Request}, View: {View}, Op: {Op}, Commit: {Commit}, " +
               $"Size: {Size}, Replica: {Replica}, Command: {Command}, Operation: {Operation}";
    }
}

public enum Command : byte
{
    Reserved,
    Ping,
    Pong,
    Request,
    Prepare,
    PrepareOk,
    Reply,
    Commit,
    StartViewChange,
    DoViewChange,
    StartView,
    Recovery,
    RecoveryResponse,
}

public enum Operation : byte
{
    Reserved,
    Set,
    Update,
    Get
}