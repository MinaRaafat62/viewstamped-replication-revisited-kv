namespace VsrReplica.VsrCore.State;

public enum ReplicaStatus : byte
{
    Normal,
    ViewChange,
    Recovering
}