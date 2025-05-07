using System.Collections.Concurrent;
using System.Text;
using Serilog;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.Application;

public class KvStore : IStateMachine
{
    private readonly ConcurrentDictionary<string, string> _store = new();

    public byte[] Apply(Operation operation, ReadOnlySpan<byte> data)
    {
        string dataString;
        string key;
        string value;

        Log.Verbose("StateMachine Apply: Operation={Operation}, Data Length={Length}", operation, data.Length);

        switch (operation)
        {
            case Operation.Set:
            case Operation.Update:
                dataString = Encoding.UTF8.GetString(data);
                var parts = dataString.Split(':', 2);
                if (parts.Length == 2)
                {
                    key = parts[0];
                    value = parts[1];
                    _store[key] = value;
                    Log.Information("StateMachine: Set/Updated Key='{Key}', Value='{Value}'", key, value);
                    return Encoding.UTF8.GetBytes("OK");
                }

                Log.Warning("StateMachine: Invalid format for Set/Update data: '{Data}'", dataString);
                return Encoding.UTF8.GetBytes("ERROR: Invalid format");

            case Operation.Get:
                // Key is the entire payload for GET
                key = Encoding.UTF8.GetString(data);
                if (_store.TryGetValue(key, out value!))
                {
                    Log.Information("StateMachine: Get Key='{Key}', Found Value='{Value}'", key, value);
                    // Return the found value
                    return Encoding.UTF8.GetBytes(value);
                }

                Log.Information("StateMachine: Get Key='{Key}', Not Found", key);
                return [];

            case Operation.Reserved:
            default:
                Log.Warning("StateMachine: Unsupported operation {Operation}", operation);
                return []; // Return empty for unsupported ops
        }
    }

    public byte[] GetState()
    {
        Log.Information("StateMachine: GetState requested. Current count: {Count}", _store.Count);
        using var ms = new MemoryStream();
        using var writer = new StreamWriter(ms, Encoding.UTF8);
        foreach (var kvp in _store.OrderBy(kv => kv.Key))
        {
            writer.WriteLine(kvp.Key);
            writer.WriteLine(kvp.Value);
        }

        writer.Flush();
        return ms.ToArray();
    }

    public void SetState(ReadOnlySpan<byte> state)
    {
        Log.Information("StateMachine: SetState called with state length: {Length}", state.Length);
        _store.Clear();
        try
        {
            using var ms = new MemoryStream(state.ToArray());
            using var reader = new StreamReader(ms, Encoding.UTF8);
            string? key;
            while ((key = reader.ReadLine()) != null)
            {
                var value = reader.ReadLine();
                if (value != null)
                {
                    _store[key] = value;
                }
                else
                {
                    Log.Warning("StateMachine SetState: Encountered key '{Key}' without a value line. Format error?",
                        key);
                    break;
                }
            }

            Log.Information("StateMachine: SetState completed. Store count: {Count}", _store.Count);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "StateMachine: Error during SetState processing.");
            _store.Clear();
        }
    }
}