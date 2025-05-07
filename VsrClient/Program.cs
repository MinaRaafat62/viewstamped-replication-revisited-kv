using System.Net;
using Serilog;
using Serilog.Exceptions;

namespace VsrClient;

class Program
{
    static async Task Main(string[] args)
    {
        ConfigureLogging();
        VsrClient? client = null;

        try
        {
            Log.Information("Starting VSR client");
            var clusterPorts = args.Length > 0
                ? args[0].Split(',').Select(int.Parse).ToList()
                : new List<int> { 5000, 5001, 5002 };

            var clusterEndpoints = clusterPorts
                .Select(p =>
                    new IPEndPoint(IPAddress.Loopback,
                        p))
                .ToList();

            Log.Information("Targeting replicas: {Endpoints}",
                string.Join(", ", clusterEndpoints.Select(e => e.ToString())));

            // Use your custom PinnedBlockMemoryPool if desired, or null for default
            // var memoryPool = new VsrReplica.Networking.MemoryPool.PinnedBlockMemoryPool(); 
            client = new VsrClient(clusterEndpoints, null);

            await client.StartAsync(); // New method to start connection managers

            await client.PingAllAsync();

            await RunCliAsync(client);
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Fatal error in client application");
        }
        finally
        {
            if (client != null)
            {
                await client.DisposeAsync();
            }

            await Log.CloseAndFlushAsync();
        }
    }

    private static async Task TestMultipleRequests(VsrClient client)
    {
        try
        {
            var tasks = new List<Task>();
            for (var i = 0; i < 100000; i++)
            {
                // Add some variation to keys/values for easier debugging if needed
                tasks.Add(client.SetAsync($"testkey{i}", $"testvalue{DateTime.UtcNow.Ticks}-{i}"));
            }

            await Task.WhenAll(tasks);
            Log.Information("TestMultipleRequests: All SET requests completed.");

            var getTasks = new List<Task<string>>();
            for (var i = 0; i < 5; i++)
            {
                getTasks.Add(client.GetAsync($"testkey{i}"));
            }

            var results = await Task.WhenAll(getTasks);
            for (var i = 0; i < results.Length; i++)
            {
                Log.Information("TestMultipleRequests: GetAsync key{Index} result: {Result}", i, results[i]);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during test: {ex.Message}");
            Log.Error(ex, "Error during TestMultipleRequests");
        }
    }

    static async Task RunCliAsync(VsrClient client)
    {
        Console.WriteLine("VSR Client CLI");
        Console.WriteLine("Available commands: get <key>, set <key> <value>, update <key> <value>, ping, test, exit");

        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(line)) continue;

            var parts = line.Trim().Split(' ', 3);
            var command = parts[0].ToLowerInvariant();
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // Command timeout

            try
            {
                switch (command)
                {
                    case "get" when parts.Length >= 2:
                        var result = await client.GetAsync(parts[1], cts.Token);
                        Console.WriteLine($"Result: {result}");
                        break;
                    case "set" when parts.Length >= 3:
                        var setResult = await client.SetAsync(parts[1], parts[2], cts.Token);
                        Console.WriteLine(setResult);
                        break;
                    case "update" when parts.Length >= 3:
                        var updateResult = await client.UpdateAsync(parts[1], parts[2], cts.Token);
                        Console.WriteLine(updateResult);
                        break;
                    case "test":
                        Console.WriteLine("Running test sequence...");
                        await TestMultipleRequests(client);
                        Console.WriteLine("Test sequence finished.");
                        break;
                    case "ping":
                        await client.PingAllAsync(cts.Token);
                        Console.WriteLine("Ping command sent to all.");
                        break;
                    case "exit" or "quit":
                        return;
                    default:
                        Console.WriteLine("Invalid command or arguments.");
                        break;
                }
            }
            catch (OperationCanceledException ex) when (ex.CancellationToken == cts.Token)
            {
                Console.WriteLine($"Error: Command timed out. {ex.Message}");
                Log.Warning(ex, "Command timed out: {Command}", command);
            }
            catch (OperationCanceledException ex) // Other cancellations (e.g. client shutdown)
            {
                Console.WriteLine($"Error: Operation cancelled. {ex.Message}");
                Log.Warning(ex, "Operation cancelled for command: {Command}", command);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.GetType().Name} - {ex.Message}");
                Log.Error(ex, "Error executing command: {Command}", command);
            }
            finally
            {
                cts.Dispose();
            }
        }
    }

    static void ConfigureLogging()
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information() // Debug for connection details
            .Enrich.WithThreadId()
            .Enrich.WithExceptionDetails()
            .WriteTo.Console(
                outputTemplate: "[{Timestamp:HH:mm:ss.fff} thr:{ThreadId} {Level:u3}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger();
    }
}