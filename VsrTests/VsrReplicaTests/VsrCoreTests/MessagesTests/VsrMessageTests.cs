using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using VsrReplica.Networking;
using VsrReplica.VsrCore;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.Utils;

namespace VsrTests.VsrReplicaTests.VsrCoreTests.MessagesTests;

public class VsrMessageTests
{
    private readonly VsrMessageSerializer _serializer = new();
    private readonly MemoryPool<byte> _memoryPool = MemoryPool<byte>.Shared;

    private VsrHeader CreateSampleHeader(uint bodySize = 10, byte replicaId = 1)
    {
        // Use the same helper as in VsrHeaderTests
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

    [Fact]
    public void SerializeDeserialize_Message_ShouldRoundtripCorrectly()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("This is the message payload.");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        // Create message without owner for serialization test simplicity
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());

        // Act I: Serialize
        SerializedMessage serializedMessage;
        using (serializedMessage = _serializer.Serialize(originalMessage, _memoryPool))
        {
            Assert.False(serializedMessage.Memory.IsEmpty);
            Assert.Equal(GlobalConfig.HeaderSize + payloadBytes.Length, serializedMessage.Memory.Length);

            // Act II: Deserialize (using the memory and owner from serialization)
            // We need to pass the owner to Deserialize
            var deserializedMessage = _serializer.Deserialize(serializedMessage.Memory, serializedMessage.Owner);

            // Assert
            Assert.NotNull(deserializedMessage);
            Assert.NotNull(deserializedMessage.Header);
            Assert.False(deserializedMessage.Payload.IsEmpty);

            // Compare Headers (basic check, could compare all fields)
            Assert.Equal(originalHeader.Command, deserializedMessage.Header.Command);
            Assert.Equal(originalHeader.Operation, deserializedMessage.Header.Operation);
            Assert.Equal(originalHeader.View, deserializedMessage.Header.View);
            Assert.Equal(originalHeader.Op, deserializedMessage.Header.Op);
            Assert.Equal(originalHeader.Commit, deserializedMessage.Header.Commit);
            Assert.Equal(originalHeader.Replica, deserializedMessage.Header.Replica);
            Assert.Equal(originalHeader.Size, deserializedMessage.Header.Size);
            Assert.Equal(originalHeader.Client, deserializedMessage.Header.Client);
            Assert.Equal(originalHeader.Request, deserializedMessage.Header.Request);

            // Compare Payloads
            Assert.True(originalMessage.Payload.Span.SequenceEqual(deserializedMessage.Payload.Span));

            // Dispose the deserialized message (which handles the owner)
            deserializedMessage.Dispose();
        } // serializedMessage is disposed here (which would normally dispose the owner if not passed on)
    }

    [Fact]
    public void SerializeDeserialize_Message_WithEmptyPayload_ShouldRoundtripCorrectly()
    {
        // Arrange
        var payloadBytes = Array.Empty<byte>();
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());

        // Act & Assert
        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);
        Assert.Equal(GlobalConfig.HeaderSize, serializedMessage.Memory.Length);

        var deserializedMessage = _serializer.Deserialize(serializedMessage.Memory, serializedMessage.Owner);

        Assert.NotNull(deserializedMessage);
        Assert.NotNull(deserializedMessage.Header);
        Assert.True(deserializedMessage.Payload.IsEmpty);
        Assert.Equal(originalHeader.Size, deserializedMessage.Header.Size);
        Assert.Equal(originalHeader.Command, deserializedMessage.Header.Command);

        deserializedMessage.Dispose();
    }


    [Fact]
    public void Deserialize_ThrowsInvalidOperation_WhenBodyChecksumMismatch()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Original Payload");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());

        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);

        // Tamper with the payload *after* serialization
        var tamperedMemory = serializedMessage.Memory.ToArray(); // Copy
        tamperedMemory[GlobalConfig.HeaderSize + 1]++; // Change one byte of payload

        using var tamperedOwner = new TestMemoryOwner(tamperedMemory);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _serializer.Deserialize(tamperedOwner.Memory, tamperedOwner));
        Assert.Contains("Invalid body checksum", ex.Message);
    }

    [Fact]
    public void Deserialize_ThrowsInvalidOperation_WhenHeaderChecksumMismatch()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Payload");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());

        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);

        // Tamper with the header (e.g., view number) *after* serialization
        var tamperedMemory = serializedMessage.Memory.ToArray(); // Copy
        // Offset for View: Checksum(16)+ChecksumBody(16)+Parent(16)+Client(16)+Context(16)+Request(4)+Cluster(4)+Epoch(4) = 108
        tamperedMemory[108]++; // Increment first byte of View

        using var tamperedOwner = new TestMemoryOwner(tamperedMemory);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _serializer.Deserialize(tamperedOwner.Memory, tamperedOwner));
        Assert.Contains("Header checksum verification failed", ex.Message);
    }

    [Fact]
    public void Deserialize_ThrowsArgumentException_WhenDataTooSmallForHeader()
    {
        var buffer = new byte[GlobalConfig.HeaderSize - 1];
        using var owner = new TestMemoryOwner(buffer);
        Assert.Throws<ArgumentException>(() => _serializer.Deserialize(owner.Memory, owner));
    }

    [Fact]
    public void TryReadMessageSize_ReturnsFalse_WhenBufferTooSmall()
    {
        var buffer = new ReadOnlySequence<byte>(new byte[GlobalConfig.HeaderSize - 1]);
        var result = _serializer.TryReadMessageSize(buffer, out var size);

        Assert.False(result);
        Assert.Equal(0, size);
    }

    [Fact]
    public void TryReadMessageSize_ReturnsTrueAndCorrectSize_WhenBufferSufficient()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Payload");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());
        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);
        var buffer = new ReadOnlySequence<byte>(serializedMessage.Memory);

        // Act
        var result = _serializer.TryReadMessageSize(buffer, out var size);

        // Assert
        Assert.True(result);
        Assert.Equal((int)originalHeader.Size, size);
        Assert.Equal(GlobalConfig.HeaderSize + payloadBytes.Length, size);
    }

    [Fact]
    public void TryReadMessageSize_ReturnsTrue_WhenBufferHasMoreThanNeeded()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Payload");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());
        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);

        // Create a larger buffer containing the message + extra data
        var largerBufferArray = serializedMessage.Memory.ToArray().Concat(new byte[] { 1, 2, 3 }).ToArray();
        var buffer = new ReadOnlySequence<byte>(largerBufferArray);

        // Act
        var result = _serializer.TryReadMessageSize(buffer, out var size);

        // Assert
        Assert.True(result);
        Assert.Equal((int)originalHeader.Size, size);
    }

    [Fact]
    public void TryReadMessageSize_HandlesMultiSegmentBuffer()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Payload");
        var originalHeader = CreateSampleHeader((uint)payloadBytes.Length);
        var originalMessage = new VsrMessage(originalHeader, payloadBytes.AsMemory());
        using var serializedMessage = _serializer.Serialize(originalMessage, _memoryPool);

        // Create a multi-segment sequence
        var segment1 = serializedMessage.Memory.Slice(0, 60);
        var segment2 = serializedMessage.Memory.Slice(60);
        var buffer = CreateSequence(segment1, segment2);


        // Act
        var result = _serializer.TryReadMessageSize(buffer, out var size);

        // Assert
        Assert.True(result);
        Assert.Equal((int)originalHeader.Size, size);
    }

    // Helper to create a ReadOnlySequence from multiple segments
    private static ReadOnlySequence<byte> CreateSequence(params ReadOnlyMemory<byte>[] segments)
    {
        Segment? startSegment = null;
        Segment? endSegment = null;

        foreach (var segmentMemory in segments)
        {
            var currentSegment = new Segment(segmentMemory);
            if (startSegment == null)
            {
                startSegment = currentSegment;
            }
            else
            {
                endSegment!.SetNext(currentSegment);
            }

            endSegment = currentSegment;
        }

        return new ReadOnlySequence<byte>(startSegment!, 0, endSegment!, endSegment!.Memory.Length);
    }

    [Fact]
    public void Serialize_Message_ProducesCorrectByteSequence()
    {
        // Arrange
        var payloadBytes = Encoding.UTF8.GetBytes("Test Payload Data");
        var header = CreateSampleHeader((uint)payloadBytes.Length, replicaId: 0xFD);
        var message = new VsrMessage(header, payloadBytes.AsMemory()); // No owner needed for serialization test

        // Act: Serialize the message
        // Note: Serialize calculates and sets checksums internally
        using var serializedMessage = _serializer.Serialize(message, _memoryPool);
        var actualBuffer = serializedMessage.Memory.ToArray(); // Get the actual bytes

        // Assert I: Check total size
        Assert.Equal(GlobalConfig.HeaderSize + payloadBytes.Length, actualBuffer.Length);

        // Assert II: Construct the expected bytes manually
        var expectedBufferList = new List<byte>();
        byte[] temp16 = new byte[16];
        byte[] temp8 = new byte[8];
        byte[] temp4 = new byte[4];

        // Manually serialize the header part (checksums are now set on 'header' by Serialize)
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.Checksum);
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, header.ChecksumBody);
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

        // Append the payload bytes
        expectedBufferList.AddRange(payloadBytes);

        var expectedBuffer = expectedBufferList.ToArray();

        // Assert III: Compare the raw bytes
        Assert.Equal<byte>(expectedBuffer, actualBuffer);
    }

    private class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public void SetNext(Segment nextSegment)
        {
            Next = nextSegment;
            RunningIndex = nextSegment.RunningIndex - Memory.Length;
        }
    }
}

public class TestMemoryOwner : IMemoryOwner<byte>
{
    private readonly byte[] _buffer;
    public Memory<byte> Memory { get; }

    public TestMemoryOwner(ReadOnlyMemory<byte> source)
    {
        _buffer = source.ToArray();
        Memory = _buffer.AsMemory();
    }

    public void Dispose()
    {
        /* No-op for this simple test owner */
        GC.SuppressFinalize(this);
    }
}