namespace VsrReplica.VsrCore;

public class GlobalConfig
{
    public const int HeaderSize = 128;
    public const byte CurrentVersion = 1;
    public static readonly TimeSpan BackupPrimaryTimeoutDuration = TimeSpan.FromMilliseconds(300);
    public static readonly TimeSpan PrimaryIdleCommitInterval = TimeSpan.FromMilliseconds(100);
}