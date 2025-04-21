using System.Buffers;

namespace VsrReplica.Networking.Interfaces;

public interface INetworkMessageSerializer<T> where T : INetworkMessage
{
    SerializedMessage Serialize(T message, MemoryPool<byte> pool);
    T Deserialize(ReadOnlyMemory<byte> memory,IMemoryOwner<byte> owner);
    bool TryReadMessageSize(ReadOnlySequence<byte> memory, out int size);
}