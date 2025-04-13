namespace VsrReplica.Networking.Interfaces;

public interface ISenderPool : IDisposable
{
    ISender Rent();
    void Return(ISender sender);
}