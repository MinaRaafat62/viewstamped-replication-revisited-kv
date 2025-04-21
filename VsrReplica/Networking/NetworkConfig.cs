using System.Net;

namespace VsrReplica.Networking;

public class NetworkConfig
{
    public static List<IPEndPoint> Replicas { get; set; } = [];
    public static IPEndPoint Replica { get; set; } = new IPEndPoint(IPAddress.Loopback, 0);
}