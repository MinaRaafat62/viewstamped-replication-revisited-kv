using System.Buffers;
using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Application;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.State;

public class ReplicaState(byte replica, byte totalReplicas, IStateMachine stateMachine, MemoryPool<byte> memoryPool)
{
    private IStateMachine StateMachine => stateMachine;
    public uint Cluster { get; } = 0;
    public byte Replica { get; } = replica;
    public byte TotalReplicas { get; } = totalReplicas;
    public byte PrimaryReplica => (byte)(View % TotalReplicas);
    public bool IsPrimary => Replica == PrimaryReplica;
    public int QuorumSize => F + 1;
    private int F => (TotalReplicas - 1) / 2;

    public uint View { get; private set; } = 0;
    public ReplicaStatus Status { get; private set; } = ReplicaStatus.Normal;
    public uint StatusViewNumber { get; private set; }
    public ulong Op { get; set; } = 0;
    public ulong Commit { get; set; } = 0;
    public uint Epoch { get; set; } = 0;
    public MemoryPool<byte> MemoryPool { get; } = memoryPool;

    private readonly Dictionary<UInt128, ClientTableEntry> _clientTable = new();
    private readonly Dictionary<ulong, LogEntry> _log = new();
    private readonly Dictionary<ulong, ConnectionId> _pendingClientConnections = new();
    private readonly Dictionary<ulong, HashSet<byte>> _prepareOkCounts = new();
    private readonly Dictionary<ConnectionId, UInt128> _connectionsToClients = new();


    public void AddConnectionToClient(ConnectionId connectionId, UInt128 clientId)
    {
        _connectionsToClients.TryAdd(connectionId, clientId);
    }

    public UInt128 GetClientId(ConnectionId connectionId)
    {
        _connectionsToClients.TryGetValue(connectionId, out var clientId);
        return clientId;
    }


    public ClientTableEntry? GetClientTableEntry(UInt128 clientId)
    {
        _clientTable.TryGetValue(clientId, out var entry);
        return entry;
    }

    public LogEntry GetLogEntry(ulong opNumber)
    {
        _log.TryGetValue(opNumber, out var entry);
        return entry;
    }


    public ulong AppendLogEntry(Operation operation, Memory<byte> payload, VsrHeader header)
    {
        Op++;
        var currentOpNumber = Op;
        var entry = new LogEntry(currentOpNumber, View, operation, header.Client, header.Request, payload.ToArray());
        _log[currentOpNumber] = entry;
        Log.Debug(
            "Replica {ReplicaId}: Appended Log Entry Op={OpNumber}, View={View}, Client={ClientId}, Req={RequestNum}",
            Replica, currentOpNumber, View, header.Client, header.Request);
        return currentOpNumber;
    }

    public void UpdateClientTable(UInt128 clientId, uint requestNumber, VsrMessage? response = null)
    {
        if (!_clientTable.TryGetValue(clientId, out var entry))
        {
            entry = new ClientTableEntry { Request = requestNumber, Response = response };
            _clientTable[clientId] = entry;
        }
        else if (requestNumber > entry.Request)
        {
            entry.Request = requestNumber;
            entry.Response = response;
            entry.Executed = false;
            entry.Result = null;
        }
        else if (requestNumber == entry.Request && response != null && entry.Response == null)
        {
            entry.Response = response;
        }
        // Implicitly ignore if requestNumber < entry.RequestNumber (stale update)
    }

    public void AddPendingClientConnection(ulong opNumber, ConnectionId connectionId)
    {
        if (!IsPrimary) return;
        _pendingClientConnections[opNumber] = connectionId;
    }

    public int AddPrepareOk(ulong opNumber, byte replicaId)
    {
        if (!_prepareOkCounts.TryGetValue(opNumber, out var replicas))
        {
            replicas = new HashSet<byte>();
            _prepareOkCounts[opNumber] = replicas;
        }

        replicas.Add(replicaId);
        return replicas.Count;
    }

    public bool TryRemovePendingClientConnection(ulong opNumber, out ConnectionId connectionId)
    {
        if (IsPrimary)
        {
            return _pendingClientConnections.Remove(opNumber, out connectionId);
        }

        connectionId = default;
        return false;
    }

    public byte[]? ExecuteAndCommitOperation(ulong opNumber)
    {
        if (opNumber <= Commit)
        {
            Log.Warning(
                "Replica {ReplicaId}: Attempted to commit op {OpNumber} which is already committed ({CurrentCommit}).",
                Replica, opNumber, Commit);
            return null;
        }

        if (opNumber != Commit + 1)
        {
            Log.Error(
                "Replica {ReplicaId}: CRITICAL - Attempted to commit op {OpNumber} out of order. Current commit is {CurrentCommit}. Requires intervention or state transfer.",
                Replica, opNumber, Commit);
            return null;
        }

        if (!_log.TryGetValue(opNumber, out var logEntry))
        {
            Log.Warning(
                "Replica {ReplicaId}: Attempted to commit op {OpNumber} but log entry not found. Waiting for Prepare?",
                Replica, opNumber);
            return null;
        }

        Log.Information("Replica {ReplicaId}: Committing and executing operation {OpNumber}", Replica, opNumber);
        byte[] result;
        try
        {
            result = StateMachine.Apply(logEntry.Operation, logEntry.PayloadSpan);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: State machine execution failed for op {OpNumber}.", Replica, opNumber);
            return null;
        }

        Commit = opNumber; // Update commit number *after* successful execution

        // Update client table entry for this operation
        if (_clientTable.TryGetValue(logEntry.Client, out var clientEntry) &&
            clientEntry.Request == logEntry.Request)
        {
            clientEntry.Executed = true;
            clientEntry.Result = result;
        }
        else
        {
            Log.Warning(
                "Replica {ReplicaId}: Client table entry not found or mismatched for committed op {OpNumber}, client {ClientId}, req {RequestNumber}",
                Replica, opNumber, logEntry.Client, logEntry.Request);
        }

        _prepareOkCounts.Remove(opNumber);

        return result;
    }

    public bool HasEnoughPrepareOks(ulong opNumber)
    {
        return _prepareOkCounts.TryGetValue(opNumber, out var replicas) && replicas.Count >= QuorumSize;
    }
}