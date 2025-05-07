using System.Buffers.Binary;
using System.Text;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrTests.VsrReplicaTests.VsrCoreTests.MessagesTests;

public class ViewChangePayloadTests
{
    private LogEntry CreateSampleLogEntry(ulong op, uint view, string payloadStr = "log-payload")
    {
        var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
        var clientId = BinaryUtils.NewGuidUInt128();
        return new LogEntry(op, view, Operation.Update, clientId, (uint)op * 10, payloadBytes);
    }

    private byte[] GetExpectedLogEntryBytes(LogEntry entry)
    {
        var expectedBufferList = new List<byte>();
        var temp8 = new byte[8];
        var temp4 = new byte[4];
        var temp16 = new byte[16];

        BinaryPrimitives.WriteUInt64BigEndian(temp8, entry.Op);
        expectedBufferList.AddRange(temp8);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, entry.View);
        expectedBufferList.AddRange(temp4);
        expectedBufferList.Add((byte)entry.Operation);
        BinaryPrimitives.WriteUInt128BigEndian(temp16, entry.Client);
        expectedBufferList.AddRange(temp16);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, entry.Request);
        expectedBufferList.AddRange(temp4);
        BinaryPrimitives.WriteUInt32BigEndian(temp4, (uint)entry.Payload.Length);
        expectedBufferList.AddRange(temp4);
        expectedBufferList.AddRange(entry.Payload);

        return expectedBufferList.ToArray();
    }

    [Fact]
    public void SerializeDeserialize_DoViewChange_EmptyLog_ShouldRoundtrip()
    {
        // Arrange
        var originalPayload = new DoViewChange(5u, new List<LogEntry>());
        var bufferSize = DoViewChange.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];

        // Act
        DoViewChange.Serialize(originalPayload, buffer.AsSpan());
        var deserializedPayload = DoViewChange.Deserialize(buffer.AsSpan());

        // Assert
        Assert.Equal(originalPayload.LatestView, deserializedPayload.LatestView);
        Assert.NotNull(deserializedPayload.LogSuffix);
        Assert.Empty(deserializedPayload.LogSuffix);
    }

    [Fact]
    public void SerializeDeserialize_DoViewChange_WithLog_ShouldRoundtrip()
    {
        // Arrange
        var logEntries = new List<LogEntry>
        {
            CreateSampleLogEntry(101, 4, "entry1"),
            CreateSampleLogEntry(102, 4, "entry2-longer")
        };
        var originalPayload = new DoViewChange(4u, logEntries);
        var bufferSize = DoViewChange.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];

        // Act
        DoViewChange.Serialize(originalPayload, buffer.AsSpan());
        var deserializedPayload = DoViewChange.Deserialize(buffer.AsSpan());

        // Assert
        Assert.Equal(originalPayload.LatestView, deserializedPayload.LatestView);
        Assert.NotNull(deserializedPayload.LogSuffix);
        Assert.Equal(logEntries.Count, deserializedPayload.LogSuffix.Count);

        for (int i = 0; i < logEntries.Count; i++)
        {
            var original = logEntries[i];
            var deserialized = deserializedPayload.LogSuffix[i];
            Assert.Equal(original.Op, deserialized.Op);
            Assert.Equal(original.View, deserialized.View);
            Assert.Equal(original.Operation, deserialized.Operation);
            Assert.Equal(original.Client, deserialized.Client);
            Assert.Equal(original.Request, deserialized.Request);
            Assert.Equal(original.Payload, deserialized.Payload);
        }
    }

    [Fact]
    public void Deserialize_DoViewChange_Throws_WhenBufferTooSmall()
    {
        var logEntries = new List<LogEntry> { CreateSampleLogEntry(101, 4, "entry1") };
        var originalPayload = new DoViewChange(4u, logEntries);
        var bufferSize = DoViewChange.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];
        DoViewChange.Serialize(originalPayload, buffer.AsSpan());

        // Test various truncations
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DoViewChange.Deserialize(buffer.AsSpan(0, 1))); // Too small for view
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DoViewChange.Deserialize(buffer.AsSpan(0, 5))); // Too small for count
        Assert.Throws<ArgumentException>(() =>
            DoViewChange.Deserialize(buffer.AsSpan(0, bufferSize - 1))); // Truncated log entry
    }

    [Fact]
    public void Serialize_DoViewChange_ProducesCorrectByteSequence()
    {
        // Arrange
        uint latestView = 0xDEADBEEF;
        var logEntry1 = CreateSampleLogEntry(101, 4, "entry1");
        var logEntry2 = CreateSampleLogEntry(102, 4, "entry2-longer");
        var logEntries = new List<LogEntry> { logEntry1, logEntry2 };

        var payload = new DoViewChange(latestView, logEntries);
        var bufferSize = DoViewChange.CalculateSerializedSize(payload);
        var actualBuffer = new byte[bufferSize];

        // Construct expected bytes
        var expectedBufferList = new List<byte>();
        var temp4 = new byte[4];

        // LatestView (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, latestView);
        expectedBufferList.AddRange(temp4);
        // LogSuffix.Count (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, (uint)logEntries.Count);
        expectedBufferList.AddRange(temp4);
        // Log Entries
        expectedBufferList.AddRange(GetExpectedLogEntryBytes(logEntry1));
        expectedBufferList.AddRange(GetExpectedLogEntryBytes(logEntry2));

        var expectedBuffer = expectedBufferList.ToArray();

        // Act
        DoViewChange.Serialize(payload, actualBuffer.AsSpan());

        // Assert
        Assert.Equal(expectedBuffer.Length, actualBuffer.Length);
        Assert.Equal<byte>(expectedBuffer, actualBuffer);
    }


    // --- StartView Tests ---

    [Fact]
    public void SerializeDeserialize_StartView_EmptyLog_ShouldRoundtrip()
    {
        // Arrange
        var originalPayload = new StartView(new List<LogEntry>());
        var bufferSize = StartView.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];

        // Act
        StartView.Serialize(originalPayload, buffer.AsSpan());
        var deserializedPayload = StartView.Deserialize(buffer.AsSpan());

        // Assert
        Assert.NotNull(deserializedPayload.LogSuffix);
        Assert.Empty(deserializedPayload.LogSuffix);
    }

    [Fact]
    public void SerializeDeserialize_StartView_WithLog_ShouldRoundtrip()
    {
        // Arrange
        var logEntries = new List<LogEntry>
        {
            CreateSampleLogEntry(201, 6, "sv_entry1"),
            CreateSampleLogEntry(202, 6, "sv_entry2_more_data")
        };
        var originalPayload = new StartView(logEntries);
        var bufferSize = StartView.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];

        // Act
        StartView.Serialize(originalPayload, buffer.AsSpan());
        var deserializedPayload = StartView.Deserialize(buffer.AsSpan());

        // Assert
        Assert.NotNull(deserializedPayload.LogSuffix);
        Assert.Equal(logEntries.Count, deserializedPayload.LogSuffix.Count);

        for (int i = 0; i < logEntries.Count; i++)
        {
            var original = logEntries[i];
            var deserialized = deserializedPayload.LogSuffix[i];
            Assert.Equal(original.Op, deserialized.Op);
            Assert.Equal(original.View, deserialized.View);
            Assert.Equal(original.Operation, deserialized.Operation);
            Assert.Equal(original.Client, deserialized.Client);
            Assert.Equal(original.Request, deserialized.Request);
            Assert.Equal(original.Payload, deserialized.Payload);
        }
    }

    [Fact]
    public void Deserialize_StartView_Throws_WhenBufferTooSmall()
    {
        var logEntries = new List<LogEntry> { CreateSampleLogEntry(201, 6, "sv_entry1") };
        var originalPayload = new StartView(logEntries);
        var bufferSize = StartView.CalculateSerializedSize(originalPayload);
        var buffer = new byte[bufferSize];
        StartView.Serialize(originalPayload, buffer.AsSpan());

        // Test various truncations
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            StartView.Deserialize(buffer.AsSpan(0, 1))); // Too small for count
        Assert.Throws<ArgumentException>(() =>
            StartView.Deserialize(buffer.AsSpan(0, bufferSize - 1))); // Truncated log entry
    }

    [Fact]
    public void Serialize_StartView_ProducesCorrectByteSequence()
    {
        // Arrange
        var logEntry1 = CreateSampleLogEntry(201, 6, "sv_entry1");
        var logEntry2 = CreateSampleLogEntry(202, 6, "sv_entry2_more_data");
        var logEntries = new List<LogEntry> { logEntry1, logEntry2 };

        var payload = new StartView(logEntries);
        var bufferSize = StartView.CalculateSerializedSize(payload);
        var actualBuffer = new byte[bufferSize];

        // Construct expected bytes
        var expectedBufferList = new List<byte>();
        var temp4 = new byte[4];

        // LogSuffix.Count (uint)
        BinaryPrimitives.WriteUInt32BigEndian(temp4, (uint)logEntries.Count);
        expectedBufferList.AddRange(temp4);
        // Log Entries
        expectedBufferList.AddRange(GetExpectedLogEntryBytes(logEntry1));
        expectedBufferList.AddRange(GetExpectedLogEntryBytes(logEntry2));

        var expectedBuffer = expectedBufferList.ToArray();

        // Act
        StartView.Serialize(payload, actualBuffer.AsSpan());

        // Assert
        Assert.Equal(expectedBuffer.Length, actualBuffer.Length);
        Assert.Equal<byte>(expectedBuffer, actualBuffer);
    }
}