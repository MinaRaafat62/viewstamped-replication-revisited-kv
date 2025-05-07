using System.Buffers.Binary;
using System.Text;
using VsrReplica.VsrCore;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.Utils;

namespace VsrTests.VsrReplicaTests.VsrCoreTests.MessagesTests;

public class VsrHeaderTests
{
    private VsrHeader CreateSampleHeader1(uint bodySize = 10, byte replicaId = 1)
    {
        return new VsrHeader(
            parent: BinaryUtils.NewGuidUInt128(),
            client: BinaryUtils.NewGuidUInt128(),
            context: BinaryUtils.NewGuidUInt128(),
            bodySize: bodySize,
            request: 12345u,
            cluster: 1u,
            epoch: 2u,
            view: 3u,
            op: 100ul,
            commit: 90ul,
            offset: 0ul,
            replica: replicaId,
            command: Command.Prepare,
            operation: Operation.Set,
            version: GlobalConfig.CurrentVersion
        );
    }

    private VsrHeader CreateSampleHeader2(uint bodySize = 10, byte replicaId = 1, UInt128 parent = default,
        UInt128 client = default, UInt128 context = default)
    {
        return new VsrHeader(
            parent: parent == default ? BinaryUtils.NewGuidUInt128() : parent,
            client: client == default ? BinaryUtils.NewGuidUInt128() : client,
            context: context == default ? BinaryUtils.NewGuidUInt128() : context,
            bodySize: bodySize,
            request: 12345u,
            cluster: 1u,
            epoch: 2u,
            view: 3u,
            op: 100ul,
            commit: 90ul,
            offset: 0ul,
            replica: replicaId,
            command: Command.Prepare,
            operation: Operation.Set,
            version: GlobalConfig.CurrentVersion
        );
    }

    [Fact]
    public void CalculateBodyChecksum_ShouldProduceConsistentResult()
    {
        var payload1 = Encoding.UTF8.GetBytes("test payload");
        var payload2 = Encoding.UTF8.GetBytes("test payload");
        var payload3 = Encoding.UTF8.GetBytes("different payload");

        var checksum1 = VsrHeader.CalculateBodyChecksum(payload1);
        var checksum2 = VsrHeader.CalculateBodyChecksum(payload2);
        var checksum3 = VsrHeader.CalculateBodyChecksum(payload3);

        Assert.NotEqual(UInt128.Zero, checksum1);
        Assert.Equal(checksum1, checksum2);
        Assert.NotEqual(checksum1, checksum3);
    }

    [Fact]
    public void SetBodyChecksum_Span_SetsCorrectChecksum()
    {
        var header = CreateSampleHeader1();
        var payload = Encoding.UTF8.GetBytes("payload data");
        var expectedChecksum = VsrHeader.CalculateBodyChecksum(payload);

        header.SetBodyChecksum(payload);

        Assert.Equal(expectedChecksum, header.ChecksumBody);
    }

    [Fact]
    public void SetBodyChecksum_UInt128_SetsChecksum()
    {
        var header = CreateSampleHeader1();
        UInt128 checksum = 12345;

        header.SetBodyChecksum(checksum);

        Assert.Equal(checksum, header.ChecksumBody);
    }

    [Fact]
    public void CalculateChecksum_Throws_WhenBodyChecksumNotSet()
    {
        var header = CreateSampleHeader1();
        // header.ChecksumBody is 0 initially
        Assert.Throws<InvalidOperationException>(() => VsrHeader.CalculateChecksum(header));
    }

    [Fact]
    public void CalculateChecksum_ShouldProduceConsistentResult()
    {
        var parent = BinaryUtils.NewGuidUInt128();
        var client = BinaryUtils.NewGuidUInt128();
        var context = BinaryUtils.NewGuidUInt128();

        var header1 = CreateSampleHeader2(replicaId: 1, bodySize: 10, parent: parent, client: client, context: context);
        var header2 =
            CreateSampleHeader2(replicaId: 1, bodySize: 10, parent: parent, client: client,
                context: context); // Same replica ID
        var header3 =
            CreateSampleHeader2(replicaId: 2, bodySize: 10, parent: parent, client: client,
                context: context); // Different replica ID

        var payload = Encoding.UTF8.GetBytes("common payload");
        header1.SetBodyChecksum(payload);
        header2.SetBodyChecksum(payload);
        header3.SetBodyChecksum(payload);

        var checksum1 = VsrHeader.CalculateChecksum(header1);
        var checksum2 = VsrHeader.CalculateChecksum(header2);
        var checksum3 = VsrHeader.CalculateChecksum(header3);

        Assert.NotEqual(UInt128.Zero, checksum1);
        Assert.Equal(checksum1, checksum2);
        Assert.NotEqual(checksum1, checksum3);
    }

    [Fact]
    public void SetHeaderChecksum_Calculated_SetsCorrectChecksum()
    {
        var header = CreateSampleHeader1();
        header.SetBodyChecksum(Encoding.UTF8.GetBytes("data")); // Need non-zero body checksum
        var expectedChecksum = VsrHeader.CalculateChecksum(header);

        header.SetHeaderChecksum(); // Calculate and set

        Assert.Equal(expectedChecksum, header.Checksum);
    }

    [Fact]
    public void SetHeaderChecksum_UInt128_SetsChecksum()
    {
        var header = CreateSampleHeader1();
        UInt128 checksum = 98765;

        header.SetHeaderChecksum(checksum);

        Assert.Equal(checksum, header.Checksum);
    }

    // --- VsrHeaderSerializer Tests ---

    [Fact]
    public void SerializeDeserialize_Header_ShouldRoundtripCorrectly()
    {
        // Arrange
        var originalHeader = CreateSampleHeader1(bodySize: 50, replicaId: 2);
        originalHeader.SetBodyChecksum(Encoding.UTF8.GetBytes("dummy payload for checksum"));
        originalHeader.SetHeaderChecksum(); // Calculate based on other fields

        var buffer = new byte[GlobalConfig.HeaderSize];

        // Act
        VsrHeaderSerializer.Serialize(originalHeader, buffer.AsSpan());
        var deserializedHeader = VsrHeaderSerializer.Deserialize(buffer.AsSpan());

        // Assert
        // Check all fields individually
        Assert.Equal(originalHeader.Checksum, deserializedHeader.Checksum);
        Assert.Equal(originalHeader.ChecksumBody, deserializedHeader.ChecksumBody);
        Assert.Equal(originalHeader.Parent, deserializedHeader.Parent);
        Assert.Equal(originalHeader.Client, deserializedHeader.Client);
        Assert.Equal(originalHeader.Context, deserializedHeader.Context);
        Assert.Equal(originalHeader.Request, deserializedHeader.Request);
        Assert.Equal(originalHeader.Cluster, deserializedHeader.Cluster);
        Assert.Equal(originalHeader.Epoch, deserializedHeader.Epoch);
        Assert.Equal(originalHeader.View, deserializedHeader.View);
        Assert.Equal(originalHeader.Op, deserializedHeader.Op);
        Assert.Equal(originalHeader.Commit, deserializedHeader.Commit);
        Assert.Equal(originalHeader.Offset, deserializedHeader.Offset);
        Assert.Equal(originalHeader.Size, deserializedHeader.Size);
        Assert.Equal(originalHeader.Replica, deserializedHeader.Replica);
        Assert.Equal(originalHeader.Command, deserializedHeader.Command);
        Assert.Equal(originalHeader.Operation, deserializedHeader.Operation);
        Assert.Equal(originalHeader.Version, deserializedHeader.Version);
    }

    [Fact]
    public void Serialize_ThrowsInvalidOperation_WhenChecksumsNotSet()
    {
        var header = CreateSampleHeader1();
        var buffer = new byte[GlobalConfig.HeaderSize];

        // Body checksum not set
        Assert.Throws<InvalidOperationException>(() => VsrHeaderSerializer.Serialize(header, buffer.AsSpan()));

        // Body set, but header not set
        header.SetBodyChecksum(Encoding.UTF8.GetBytes("data"));
        Assert.Throws<InvalidOperationException>(() => VsrHeaderSerializer.Serialize(header, buffer.AsSpan()));
    }

    [Fact]
    public void Serialize_ThrowsArgumentException_WhenBufferTooSmall()
    {
        var header = CreateSampleHeader1();
        header.SetBodyChecksum(Encoding.UTF8.GetBytes("data"));
        header.SetHeaderChecksum();
        var buffer = new byte[GlobalConfig.HeaderSize - 1]; // Too small

        Assert.Throws<ArgumentException>(() => VsrHeaderSerializer.Serialize(header, buffer.AsSpan()));
    }

    [Fact]
    public void Deserialize_ThrowsArgumentException_WhenBufferTooSmall()
    {
        var buffer = new byte[GlobalConfig.HeaderSize - 1]; // Too small
        Assert.Throws<ArgumentException>(() => VsrHeaderSerializer.Deserialize(buffer.AsSpan()));
    }

    [Fact]
    public void Serialize_Header_ProducesCorrectByteSequence()
    {
        // Arrange
        var header = new VsrHeader(
            parent: new UInt128(1, 2),
            client: new UInt128(3, 4),
            context: new UInt128(5, 6),
            bodySize: 10u, // Size will be 128 + 10 = 138
            request: 0xAABBCCDD,
            cluster: 0x11223344,
            epoch: 0x55667788,
            view: 0x99AABBCC,
            op: 0x0102030405060708,
            commit: 0x090A0B0C0D0E0F00,
            offset: 0xFFEEDDCCBBAA9988,
            replica: 0xFE,
            command: Command.PrepareOk, // Example command
            operation: Operation.Get, // Example operation
            version: 0x01
        );

        // Set checksums (essential for serialization)
        // Use a predictable body checksum for the test
        var bodyChecksum = new UInt128(0xABCDEF0123456789, 0x9876543210FEDCBA);
        header.SetBodyChecksum(bodyChecksum);
        header.SetHeaderChecksum(); // Calculate the header checksum based on all fields

        var actualBuffer = new byte[GlobalConfig.HeaderSize];

        // Define the EXACT expected byte sequence (Big Endian)
        var expectedBufferList = new List<byte>();
        var temp16 = new byte[16];
        var temp8 = new byte[8];
        var temp4 = new byte[4];

        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.Checksum); // Use the calculated checksum
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.ChecksumBody); // Use the set body checksum
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.Parent);
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.Client);
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.Context);
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, header.Request);
        expectedBufferList.AddRange(temp4);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, header.Cluster);
        expectedBufferList.AddRange(temp4);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, header.Epoch);
        expectedBufferList.AddRange(temp4);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, header.View);
        expectedBufferList.AddRange(temp4);
        BinaryPrimitives.WriteUInt64BigEndian(temp8, header.Op);
        expectedBufferList.AddRange(temp8);
        BinaryPrimitives.WriteUInt64BigEndian(temp8, header.Commit);
        expectedBufferList.AddRange(temp8);
        BinaryPrimitives.WriteUInt64BigEndian(temp8, header.Offset);
        expectedBufferList.AddRange(temp8);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, header.Size);
        expectedBufferList.AddRange(temp4);
        expectedBufferList.Add(header.Replica);
        expectedBufferList.Add((byte)header.Command);
        expectedBufferList.Add((byte)header.Operation);
        expectedBufferList.Add(header.Version);

        var expectedBuffer = expectedBufferList.ToArray();

        // Act
        VsrHeaderSerializer.Serialize(header, actualBuffer.AsSpan());

        // Assert
        Assert.Equal(GlobalConfig.HeaderSize, expectedBuffer.Length); // Sanity check expected size
        Assert.Equal<byte>(expectedBuffer, actualBuffer); // Compare raw bytes
    }
}