using System.Buffers;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.VsrCore.Messages;

public class VsrMessage : INetworkMessage
{
    public VsrHeader Header { get; set; }
    public Memory<byte> Payload { get; set; }
    private IMemoryOwner<byte>? _memoryOwner;


    public VsrMessage(VsrHeader header, IMemoryOwner<byte> memoryOwner, Memory<byte> payload)
    {
        Header = header;
        _memoryOwner = memoryOwner;
        Payload = payload;
    }
    
    public VsrMessage(VsrHeader header, Memory<byte> payload)
    {
        Header = header;
        Payload = payload;
        _memoryOwner = null;
    }

    public override string ToString()
    {
        return $"Header: {Header}, Payload: {Payload.Length} bytes";
    }

    public void Dispose()
    {
        if (_memoryOwner == null) return;
        _memoryOwner.Dispose();
        _memoryOwner = null;
        GC.SuppressFinalize(this);
    }
}