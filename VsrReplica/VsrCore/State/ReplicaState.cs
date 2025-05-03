using System.Buffers;
using System.Collections.Concurrent;
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

    public ulong Op { get; set; } = 0;
    public ulong Commit { get; set; } = 0;
    public uint Epoch { get; set; } = 0;
    public MemoryPool<byte> MemoryPool { get; } = memoryPool;

    private readonly Dictionary<UInt128, ClientTableEntry> _clientTable = new();
    private readonly Dictionary<ulong, LogEntry> _log = new();
    private readonly Dictionary<ulong, ConnectionId> _pendingClientConnections = new();
    private readonly Dictionary<ulong, HashSet<byte>> _prepareOkCounts = new();
    private readonly Dictionary<UInt128, ConnectionId> _clientToConnectionMap = new();

    public uint LastNormalView { get; private set; }
    public uint StatusViewNumber { get; private set; }

    private readonly Dictionary<uint, HashSet<byte>> _startViewChangeVotes = new();
    private readonly Dictionary<uint, List<DoViewChangeReceivedData>> _doViewChangeData = new();

    public record DoViewChangeReceivedData(
        byte SenderId,
        uint OldView, // The 'v' in the paper's DVC message (sender's view when it sent SVC)
        ulong Op,
        ulong Commit,
        List<LogEntry> LogSuffix // The log suffix sent by the sender
    );

    public void InitiateViewChange()
    {
        if (Status == ReplicaStatus.Normal)
        {
            LastNormalView = View;
        }

        var nextView = (Status == ReplicaStatus.ViewChange ? StatusViewNumber : View) + 1;
        Log.Warning(
            "Replica {ReplicaId}: Initiating View Change. Current View: {CurrentView}, Current Status: {Status}. Moving to View: {NextView}",
            Replica, View, Status, nextView);
        View = nextView;
        Status = ReplicaStatus.ViewChange;
        StatusViewNumber = nextView;
        _startViewChangeVotes.Remove(StatusViewNumber);
        _doViewChangeData.Remove(StatusViewNumber);
    }

    public bool AddStartViewChangeVote(uint view, byte replicaId)
    {
        if (!_startViewChangeVotes.TryGetValue(view, out var votes))
        {
            votes = [];
            _startViewChangeVotes[view] = votes;
        }

        var added = votes.Add(replicaId);
        Log.Debug(
            "Replica {ReplicaId}: Received StartViewChange vote for View {View} from Replica {SenderId}. Added={Added}. Total Votes: {Count}/{Needed}",
            Replica, view, replicaId, added, votes.Count, QuorumSize);
        return votes.Count >= QuorumSize; // Quorum for StartViewChange is f+1
    }


    public bool AddDoViewChangeData(uint view, DoViewChangeReceivedData data)
    {
        if (!_doViewChangeData.TryGetValue(view, out var dataList))
        {
            dataList = [];
            _doViewChangeData[view] = dataList;
        }

        if (dataList.Any(d => d.SenderId == data.SenderId))
        {
            Log.Warning(
                "Replica {ReplicaId}: Received duplicate DoViewChange for View {View} from Replica {SenderId}. Ignoring.",
                Replica, view, data.SenderId);
            return dataList.Count >= QuorumSize; // Return current status
        }

        dataList.Add(data);
        Log.Debug(
            "Replica {ReplicaId}: Received DoViewChange data for View {View} from Replica {SenderId}. Total Received: {Count}/{Needed}",
            Replica, view, data.SenderId, dataList.Count,
            QuorumSize);
        return dataList.Count >= QuorumSize;
    }

    public List<DoViewChangeReceivedData>? GetDoViewChangeQuorumData(uint view)
    {
        if (_doViewChangeData.TryGetValue(view, out var dataList) && dataList.Count >= QuorumSize)
        {
            return dataList;
        }

        return null;
    }


    public List<LogEntry> GetLogSuffix(ulong fromCommitNumber)
    {
        // Return entries with Op > fromCommitNumber, ordered by Op
        return _log.Where(kv => kv.Key > fromCommitNumber)
            .OrderBy(kv => kv.Key)
            .Select(kv => kv.Value)
            .ToList();
    }


    public void ReplaceLogSuffix(ulong startingOp, List<LogEntry> suffix)
    {
        Log.Information(
            "Replica {ReplicaId}: Replacing log suffix starting from Op={StartOp}. New suffix contains {Count} entries.",
            Replica, startingOp, suffix.Count);

        var opsToRemove = _log.Keys.Where(op => op >= startingOp).ToList();
        foreach (var op in opsToRemove)
        {
            if (_log.Remove(op, out var removedEntry))
            {
                Log.Debug("Replica {ReplicaId}: Removed existing log entry {Op} during suffix replacement.", Replica,
                    op);
                // Potentially dispose resources if LogEntry held any unmanaged ones, though unlikely here.
            }
        }

        _prepareOkCounts.Clear();

        ulong maxOpInSuffix = Op;
        foreach (var entry in suffix)
        {
            if (entry.Op < startingOp)
            {
                Log.Warning(
                    "Replica {ReplicaId}: Log suffix contains entry {Op} which is before startingOp {StartOp}. Skipping.",
                    Replica, entry.Op, startingOp);
                continue;
            }

            _log[entry.Op] = entry;
            Log.Debug("Replica {ReplicaId}: Added log entry {Op} from suffix.", Replica, entry.Op);
            if (entry.Op > maxOpInSuffix)
            {
                maxOpInSuffix = entry.Op;
            }
        }

        Op = maxOpInSuffix;
        Log.Information("Replica {ReplicaId}: Log suffix replaced. New Op number: {NewOp}", Replica, Op);
    }

    public void SetStatusNormal(uint newView, ulong newOp, ulong newCommit)
    {
        var previousView = View;
        Log.Information("Replica {ReplicaId}: Transitioning to Normal status. View: {View}, Op: {Op}, Commit: {Commit}",
            Replica, newView, newOp, newCommit);

        View = newView;
        Op = newOp;
        Commit = newCommit;
        Status = ReplicaStatus.Normal;
        LastNormalView = newView;
        var oldStatusViewNumber = StatusViewNumber;
        StatusViewNumber = 0;
        if (oldStatusViewNumber > 0) ClearViewChangeState(oldStatusViewNumber);
        if (previousView > 0 && previousView != newView) ClearViewChangeState(previousView);
    }

    public void ClearViewChangeState(uint view)
    {
        var removedSvc = _startViewChangeVotes.Remove(view);
        var removedDvc = _doViewChangeData.Remove(view);
        if (removedSvc || removedDvc)
        {
            Log.Debug(
                "Replica {ReplicaId}: Cleared view change tracking state for View {View}. SVC Removed: {RemovedSvc}, DVC Removed: {RemovedDvc}",
                Replica, view, removedSvc, removedDvc);
        }
    }


    public void AddConnectionToClient(ConnectionId connectionId, UInt128 clientId)
    {
        _clientToConnectionMap.TryAdd(clientId, connectionId);
    }

    public ConnectionId GetClientConnectionId(UInt128 clientId)
    {
        _clientToConnectionMap.TryGetValue(clientId, out var connectionId);
        return connectionId;
    }

    public int GetStartViewChangeVoteCount(uint view)
    {
        if (_startViewChangeVotes.TryGetValue(view, out var votes))
        {
            return votes.Count;
        }

        return 0; // No votes recorded for this view yet
    }


    public ClientTableEntry? GetClientTableEntry(UInt128 clientId)
    {
        _clientTable.TryGetValue(clientId, out var entry);
        return entry;
    }

    public LogEntry GetLogEntry(ulong opNumber)
    {
        _log.TryGetValue(opNumber, out var entry);
        return entry!;
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