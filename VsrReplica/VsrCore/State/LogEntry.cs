using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.State;

public readonly record struct LogEntry(
    ulong Op,
    uint View,
    Operation Operation,
    UInt128 Client,
    uint Request,
    byte[] Payload
)
{
    public ulong Op { get; } = Op;
    public uint View { get; } = View;
    public Operation Operation { get; } = Operation;
    public UInt128 Client { get; } = Client;
    public uint Request { get; } = Request;
    public byte[] Payload { get; } = Payload;
    
    public ReadOnlySpan<byte> PayloadSpan => Payload.AsSpan();

    public override string ToString()
    {
        return $"Op={Op}, View={View}, OpType={Operation}, Client={Client}, Req={Request}, PayloadLen={Payload.Length}";
    }
}