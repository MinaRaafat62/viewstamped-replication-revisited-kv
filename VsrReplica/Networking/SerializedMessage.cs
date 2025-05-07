using System.Buffers;

namespace VsrReplica.Networking;

public readonly struct SerializedMessage(ReadOnlyMemory<byte> memory, IMemoryOwner<byte> owner) : IDisposable
{
    public ReadOnlyMemory<byte> Memory { get; } = memory;
    private readonly IMemoryOwner<byte> _owner = owner;

    public readonly IMemoryOwner<byte> Owner => _owner;

    public void Dispose()
    {
        _owner?.Dispose();
    }
}