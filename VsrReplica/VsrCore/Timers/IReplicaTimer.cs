namespace VsrReplica.VsrCore.Timers;

public interface IReplicaTimer : IDisposable
{
    void Start(TimeSpan duration);
    void Stop();
    void ActivityDetected();
}