using System.Net;
using Serilog;
using Serilog.Exceptions;
using VsrReplica.Networking;
using VsrReplica.VsrCore;

namespace VsrReplica;

public class Program
{
    // Keep logging configuration separate for clarity
    static void ConfigureLogging()
    {
        Log.Logger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .Enrich.WithExceptionDetails()
            .WriteTo.Console(outputTemplate:
                "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}")
            .MinimumLevel.Verbose() 
            .CreateLogger();
    }

    // Main entry point
    public static async Task Main(string[] args)
    {
        ConfigureLogging();

        // --- Argument Parsing ---
        if (args.Length != 3 || !args[0].Equals("replica", StringComparison.InvariantCultureIgnoreCase))
        {
            PrintUsage();
            return;
        }

        if (!int.TryParse(args[1], out var replicaIndex) || replicaIndex < 0)
        {
            Log.Error("Invalid replica index '{Index}'. Must be a non-negative integer.", args[1]);
            PrintUsage();
            return;
        }

        string endpointsString = args[2];
        var endpointStrings =
            endpointsString.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (endpointStrings.Length == 0)
        {
            Log.Error("No replica endpoints provided.");
            PrintUsage();
            return;
        }

        var replicaEndpoints = new List<IPEndPoint>();
        foreach (var epString in endpointStrings)
        {
            if (IPEndPoint.TryParse(epString, out var endpoint))
            {
                replicaEndpoints.Add(endpoint!);
            }
            else
            {
                Log.Error(
                    "Invalid endpoint format: '{EndpointString}'. Expected format IP:Port (e.g., 127.0.0.1:9000 or [::1]:9000).",
                    epString);
                PrintUsage();
                return;
            }
        }

        if (replicaIndex >= replicaEndpoints.Count)
        {
            Log.Error("Replica index {Index} is out of range. Must be less than the number of endpoints ({Count}).",
                replicaIndex, replicaEndpoints.Count);
            PrintUsage();
            return;
        }
        // --- End Argument Parsing ---


        // --- Configuration from Parsed Arguments ---
        var selfEndpoint = replicaEndpoints[replicaIndex];
        NetworkConfig.SetThisReplica(selfEndpoint); // Set the static config for potential other uses

        // Create the Replica ID -> Endpoint map
        var replicaMap = new Dictionary<byte, IPEndPoint>();
        for (int i = 0; i < replicaEndpoints.Count; i++)
        {
            // Use the index in the list as the replica ID (byte)
            replicaMap.Add((byte)i, replicaEndpoints[i]);
        }
        // --- End Configuration ---


        Log.Information("Starting Replica {ReplicaId} at {EndPoint}...", (byte)replicaIndex, selfEndpoint);
        Log.Information("Total Replicas: {Count}. Full List: {List}", replicaMap.Count,
            string.Join(", ", replicaEndpoints));

        Replica? replica = null;
        try
        {
            // Keep the application running until shutdown is requested
            var cts = new CancellationTokenSource();

            // Pass the map to the Replica constructor
            replica = new Replica(NetworkProtocol.Tcp, replicaMap);
            replica.Start(cts.Token); // Start listening and connecting

            Log.Information("Replica started. Press Ctrl+C to exit...");


            Console.CancelKeyPress += (sender, e) =>
            {
                if (!cts.IsCancellationRequested)
                {
                    Log.Information("Shutdown requested via Ctrl+C...");
                    e.Cancel = true; // Prevent default OS termination
                    cts.Cancel(); // Signal cancellation to the application
                }
                else
                {
                    Log.Warning("Shutdown already in progress.");
                }
            };

            // Wait for cancellation signal
            await Task.Delay(Timeout.Infinite, cts.Token)
                .ContinueWith(_ => { },
                    cts.Token); // Use ContinueWith to prevent TaskCanceledException propagation here

            Log.Information("Shutdown signal received.");
        }
        catch (OperationCanceledException)
        {
            // Expected when cts.Cancel() is called by Ctrl+C handler
            Log.Information("Shutdown process initiated.");
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Replica {ReplicaId} failed to start or encountered a fatal error during runtime.",
                (byte)replicaIndex);
        }
        finally
        {
            if (replica != null)
            {
                Log.Information("Disposing replica {ReplicaId}...", (byte)replicaIndex);
                try
                {
                    replica.Dispose(); // Ensure Dispose is called to clean up network resources
                    Log.Information("Replica {ReplicaId} disposed successfully.", (byte)replicaIndex);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error during replica disposal for Replica {ReplicaId}.", (byte)replicaIndex);
                }
            }

            await Log.CloseAndFlushAsync(); // Ensure all logs are written
        }
    }

    static void PrintUsage()
    {
        Console.WriteLine("\nUsage:");
        Console.WriteLine("  dotnet run -- replica <index> <endpoints>");
        Console.WriteLine("\nArguments:");
        Console.WriteLine("  <index>      The zero-based index of this replica instance.");
        Console.WriteLine("  <endpoints>  A comma-separated list of all replica endpoints in IP:Port format.");
        Console.WriteLine("\nExample:");
        Console.WriteLine("  dotnet run -- replica 0 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002");
        Console.WriteLine("  dotnet run -- replica 1 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002");
    }
}