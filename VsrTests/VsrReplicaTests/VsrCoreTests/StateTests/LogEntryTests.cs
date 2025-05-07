using System.Buffers.Binary;
using System.Text;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrTests.VsrReplicaTests.VsrCoreTests.StateTests;

public class LogEntryTests
{
    private static UInt128 NewTestClientId() => BinaryUtils.NewGuidUInt128();

    private LogEntry CreateSampleLogEntry(ulong op = 123, uint view = 5, string payloadStr = "test-payload",
        Operation operation = Operation.Set, uint request = 456u)
    {
        var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
        var clientId = NewTestClientId();
        return new LogEntry(op, view, operation, clientId, request, payloadBytes);
    }

    private const int
        LogEntryFixedSize =
            sizeof(ulong) + sizeof(uint) + sizeof(byte) + 16 + sizeof(uint) + sizeof(uint); // 8+4+1+16+4+4 = 37 bytes

    [Fact]
    public void CalculateSerializedSize_ShouldBeCorrect()
    {
        const string payloadStr = "some data";
        var entry = CreateSampleLogEntry(payloadStr: payloadStr);
        var expectedPayloadSize = Encoding.UTF8.GetByteCount(payloadStr);

        var calculatedSize = LogEntry.CalculateSerializedSize(entry);

        Assert.Equal(LogEntryFixedSize + expectedPayloadSize, calculatedSize);
    }

    [Fact]
    public void CalculateSerializedSize_WithEmptyPayload_ShouldBeCorrect()
    {
        var entry = CreateSampleLogEntry(payloadStr: "");
        var calculatedSize = LogEntry.CalculateSerializedSize(entry);
        Assert.Equal(LogEntryFixedSize, calculatedSize);
    }


    [Fact]
    public void SerializeDeserialize_LogEntry_ShouldRoundtripCorrectly()
    {
        // Arrange
        var originalEntry = CreateSampleLogEntry(op: 999, view: 10, payloadStr: "hello world 123",
            operation: Operation.Get, request: 1001);
        var bufferSize = LogEntry.CalculateSerializedSize(originalEntry);
        var buffer = new byte[bufferSize];
        var bufferSpan = buffer.AsSpan(); // Get span once

        // Act
        var bytesWritten = LogEntry.Serialize(originalEntry, bufferSpan);
        var deserializedEntry = LogEntry.Deserialize(bufferSpan, out var bytesRead);

        // Assert
        Assert.Equal(bufferSize, bytesWritten);
        Assert.Equal(bufferSize, bytesRead);

        Assert.Equal(originalEntry.Op, deserializedEntry.Op);
        Assert.Equal(originalEntry.View, deserializedEntry.View);
        Assert.Equal(originalEntry.Operation, deserializedEntry.Operation);
        Assert.Equal(originalEntry.Client, deserializedEntry.Client);
        Assert.Equal(originalEntry.Request, deserializedEntry.Request);
        Assert.Equal(originalEntry.Payload, deserializedEntry.Payload); // Compares byte arrays
    }

    [Fact]
    public void SerializeDeserialize_LogEntry_WithEmptyPayload_ShouldRoundtripCorrectly()
    {
        // Arrange
        var originalEntry = CreateSampleLogEntry(payloadStr: "");
        var bufferSize = LogEntry.CalculateSerializedSize(originalEntry);
        var buffer = new byte[bufferSize];
        var bufferSpan = buffer.AsSpan(); // Get span once

        // Act
        var bytesWritten = LogEntry.Serialize(originalEntry, bufferSpan);
        var deserializedEntry = LogEntry.Deserialize(bufferSpan, out var bytesRead);

        // Assert
        Assert.Equal(bufferSize, bytesWritten);
        Assert.Equal(bufferSize, bytesRead);

        Assert.Equal(originalEntry.Op, deserializedEntry.Op);
        Assert.Equal(originalEntry.View, deserializedEntry.View);
        Assert.Equal(originalEntry.Operation, deserializedEntry.Operation);
        Assert.Equal(originalEntry.Client, deserializedEntry.Client);
        Assert.Equal(originalEntry.Request, deserializedEntry.Request);
        Assert.Empty(deserializedEntry.Payload);
        Assert.Equal(originalEntry.Payload, deserializedEntry.Payload);
    }

    [Fact]
    public void Serialize_ThrowsArgumentException_WhenBufferTooSmall()
    {
        var entry = CreateSampleLogEntry();
        var requiredSize = LogEntry.CalculateSerializedSize(entry);
        var buffer = new byte[requiredSize - 1]; // One byte too small

        // Create the span inside the lambda
        Assert.Throws<ArgumentException>(() => LogEntry.Serialize(entry, buffer.AsSpan()));
    }

    [Fact]
    public void Deserialize_ThrowsArgumentException_WhenBufferTooSmallForFixedPart()
    {
        var entry = CreateSampleLogEntry();
        var requiredSize = LogEntry.CalculateSerializedSize(entry);
        var buffer = new byte[requiredSize];
        LogEntry.Serialize(entry, buffer.AsSpan());

        // Define the size needed, but create the span inside the lambda
        const int insufficientSize = LogEntryFixedSize - 1;

        Assert.Throws<ArgumentException>(() =>
        {
            // Create the too-small span *inside* the lambda
            var smallSpan = buffer.AsSpan(0, insufficientSize);
            LogEntry.Deserialize(smallSpan, out _);
        });
    }

    [Fact]
    public void Deserialize_ThrowsArgumentException_WhenBufferTooSmallForPayload()
    {
        var entry = CreateSampleLogEntry(payloadStr: "non-empty");
        var requiredSize = LogEntry.CalculateSerializedSize(entry);
        var buffer = new byte[requiredSize];
        LogEntry.Serialize(entry, buffer.AsSpan());

        // Define the size needed, but create the span inside the lambda
        var insufficientSize = requiredSize - 1; // Has fixed part, but payload truncated

        Assert.Throws<ArgumentException>(() =>
        {
            // Create the too-small span *inside* the lambda
            var smallSpan = buffer.AsSpan(0, insufficientSize);
            LogEntry.Deserialize(smallSpan, out _);
        });
    }

    [Fact]
    public void Serialize_ProducesCorrectByteSequence()
    {
        // Arrange
        const ulong op = 0x0102030405060708;
        const uint view = 0xAABBCCDD;
        const Operation operation = Operation.Set; // Assuming Operation.Set maps to byte 0x01
        var client = new UInt128(0x1122334455667788, 0x99AABBCCDDEEFF00);
        const uint request = 0xEEEEFFFF;
        byte[] payload = [0xAA, 0xBB, 0xCC];

        var entry = new LogEntry(op, view, operation, client, request, payload);
        var bufferSize = LogEntry.CalculateSerializedSize(entry);
        var actualBuffer = new byte[bufferSize];

        // Define the EXACT expected byte sequence (Big Endian)
        var expectedBufferList = new List<byte>();
        var temp8 = new byte[8];
        var temp4 = new byte[4];
        var temp16 = new byte[16];

        // Op (ulong)
        BinaryPrimitives.WriteUInt64BigEndian(temp8, op);
        expectedBufferList.AddRange(temp8);
        // View (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, view);
        expectedBufferList.AddRange(temp4);
        // Operation (byte)
        expectedBufferList.Add((byte)operation);
        // Client (UInt128)
        BinaryPrimitives.WriteUInt128BigEndian(temp16, client);
        expectedBufferList.AddRange(temp16);
        // Request (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, request);
        expectedBufferList.AddRange(temp4);
        // Payload Length (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, (uint)payload.Length);
        expectedBufferList.AddRange(temp4);
        // Payload (byte[])
        expectedBufferList.AddRange(payload);

        var expectedBuffer = expectedBufferList.ToArray();

        // Act
        var bytesWritten = LogEntry.Serialize(entry, actualBuffer.AsSpan());

        // Assert
        Assert.Equal(expectedBuffer.Length, bytesWritten);
        Assert.Equal(expectedBuffer.Length, actualBuffer.Length);
        Assert.Equal<byte>(expectedBuffer, actualBuffer);
    }
}