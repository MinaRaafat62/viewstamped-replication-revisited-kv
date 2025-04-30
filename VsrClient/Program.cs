using System.Net;
using Serilog;
using Serilog.Exceptions;

namespace VsrClient;

class Program
{
    static async Task Main(string[] args)
    {
        // Configure logging
        ConfigureLogging();

        try
        {
            // Parse cluster endpoints
            Log.Information("Starting VSR client");
            var clusterPorts = args.Length > 0
                ? args[0].Split(',').Select(int.Parse).ToList()
                : new List<int> { 9000, 9001, 9002 }; // Default to a few ports if none specified

            var clusterEndpoints = clusterPorts
                .Select(p => new IPEndPoint(IPAddress.Loopback, p))
                .ToList();

            Log.Information("Connecting to replicas: {Endpoints}",
                string.Join(", ", clusterEndpoints.Select(e => e.ToString())));

            // Create and connect client using the new VsrClient
            await using var client = new global::VsrClient.VsrClient(clusterEndpoints);
            await client.ConnectAsync();

            // *** CHANGE IS HERE: Call the new PingAsync method ***
            await client.PingAsync(); // Changed from PingReplicasAsync

            // Start interactive CLI
            await RunCliAsync(client);
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Fatal error in client application");
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
    }

    // This test function remains compatible
    private static async Task TestMultipleRequests(global::VsrClient.VsrClient client)
    {
        try
        {
            Console.WriteLine(await client.SetAsync("key1", "value1"));
            Console.WriteLine(await client.SetAsync("key2", "value2"));
            Console.WriteLine(await client.GetAsync("key1"));
            Console.WriteLine(await client.GetAsync("key2"));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during test: {ex.Message}");
            Log.Error(ex, "Error during TestMultipleRequests");
        }
    }


    static async Task RunCliAsync(global::VsrClient.VsrClient client)
    {
        Console.WriteLine("VSR Client CLI");
        Console.WriteLine("Available commands: get <key>, set <key> <value>, update <key> <value>, ping, test, exit");

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(line))
                continue;

            var parts = line.Trim().Split(' ', 3); // Split into command, key, and value (max 3 parts)
            var command = parts[0].ToLowerInvariant();

            try
            {
                switch (command)
                {
                    case "get" when parts.Length >= 2:
                        var result = await client.GetAsync(parts[1]);
                        Console.WriteLine($"Result: {result}");
                        break;

                    case "set" when parts.Length >= 3:
                        var setResult = await client.SetAsync(parts[1], parts[2]);
                        Console.WriteLine(setResult);
                        break;

                    case "update" when parts.Length >= 3:
                        var updateResult = await client.UpdateAsync(parts[1], parts[2]);
                        Console.WriteLine(updateResult);
                        break;

                    case "test":
                        Console.WriteLine("Running test sequence...");
                        await TestMultipleRequests(client);
                        Console.WriteLine("Test sequence finished.");
                        break;

                    case "ping":
                        await client.PingAsync(); // This call matches the new client method
                        Console.WriteLine("Ping command sent.");
                        break;

                    case "exit":
                    case "quit":
                        return;

                    default:
                        Console.WriteLine(
                            "Invalid command or arguments. Available: get <key>, set <key> <value>, update <key> <value>, ping, test, exit");
                        break;
                }
            }
            catch (OperationCanceledException ex) // Catch timeouts specifically
            {
                Console.WriteLine($"Error: Request timed out or cancelled. {ex.Message}");
                Log.Warning(ex, "Request timed out or cancelled for command: {Command}", command);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Log.Error(ex, "Error executing command: {Command}", command);
                // Consider if certain errors mean the client should try reconnecting or selecting a new primary
            }
        }
    }

    static void ConfigureLogging()
    {
        // Basic console logger, adjust as needed
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug() // Set to Debug to see connection/message details
            .Enrich.WithExceptionDetails()
            .WriteTo.Console(
                outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger();
    }
}