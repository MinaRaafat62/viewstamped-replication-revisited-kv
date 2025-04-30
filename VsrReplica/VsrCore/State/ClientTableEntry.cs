using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.State;

public class ClientTableEntry
{
    public uint Request { get; set; }
    public VsrMessage? Response { get; set; }
    public bool Executed { get; set; }
    public byte[]? Result { get; set; }
}