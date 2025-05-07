using System.Net;

namespace VsrReplica.Networking;

public class NetworkConfig
{
    public static List<IPEndPoint> Replicas { get; set; } = [];
    public static IPEndPoint Replica { get; private set; } = new IPEndPoint(IPAddress.Loopback, 5000);

    public static void SetThisReplica(IPEndPoint replica)
    {
        Replica = replica;
    }
}