using System.Diagnostics;
using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Handlers;

public class DoViewChangeHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        var senderId = header.Replica;
        var receivedView = header.View;

        Log.Debug(
            "Replica {ReplicaId}: Rcvd DoViewChange V:{View} from {SenderId}. MyV:{MyView}, Status:{Status}, TargetV:{TargetV}",
            state.Replica, receivedView, senderId, state.View, state.Status, state.StatusViewNumber);
        if (state.Status != ReplicaStatus.ViewChange || receivedView != state.StatusViewNumber)
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring DoViewChange for V:{View}. Status ({Status}) / TargetV ({TargetV}) mismatch.",
                state.Replica, receivedView, state.Status, state.StatusViewNumber);
            return false;
        }

        var expectedPrimary = (byte)(receivedView % state.TotalReplicas);
        if (state.Replica != expectedPrimary)
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring DoViewChange for V:{View}. Not the expected primary ({ExpectedPrimary}).",
                state.Replica, receivedView, expectedPrimary);
            return false;
        }

        Log.Information("Replica {ReplicaId} (New Primary): Processing DoViewChange for V:{View} from {SenderId}.",
            state.Replica, receivedView, senderId);

        DoViewChange payload;
        try
        {
            payload = DoViewChange.Deserialize(message.Payload.Span);
        }
        catch (Exception ex)
        {
            Log.Error(ex,
                "Replica {ReplicaId} (New Primary): Failed to deserialize DoViewChange payload from {SenderId} for V:{View}. Ignoring.",
                state.Replica, senderId, receivedView);
            return false;
        }

        var receivedData = new ReplicaState.DoViewChangeReceivedData(
            senderId,
            payload.LatestView,
            header.Op,
            header.Commit,
            payload.LogSuffix
        );
        var quorumReached = state.AddDoViewChangeData(receivedView, receivedData);
        if (!quorumReached)
        {
            Log.Debug(
                "Replica {ReplicaId} (New Primary): Rcvd DoViewChange for V:{View} from {SenderId}. Waiting for quorum ({Count}/{Needed}).",
                state.Replica, receivedView, senderId, state.GetDoViewChangeQuorumData(receivedView)?.Count ?? 0,
                state.QuorumSize);
            return true;
        }

        Log.Information(
            "Replica {ReplicaId} (New Primary): Quorum reached for DoViewChange for V:{View}. Processing state and sending StartView.",
            state.Replica, receivedView);

        var allDvcData = state.GetDoViewChangeQuorumData(receivedView);
        if (allDvcData == null || allDvcData.Count < state.QuorumSize)
        {
            Log.Error(
                "Replica {ReplicaId} (New Primary): Quorum reported but failed to retrieve sufficient DVC data for V:{View}.",
                state.Replica, receivedView);
            return false;
        }

        var bestData = allDvcData
            .OrderByDescending(d => d.OldView)
            .ThenByDescending(d => d.Op)
            .First();


        Log.Information(
            "Replica {ReplicaId} (New Primary): Selected log from Replica {BestSenderId} (OldView:{OldView}, Op:{Op}) for new V:{NewView}.",
            state.Replica, bestData.SenderId, bestData.OldView, bestData.Op, receivedView);

        var oldCommitNumber = state.Commit;
        var newCommitNumber = allDvcData.Max(d => d.Commit);
        var newOpNumber = bestData.Op;
        var baseCommitForSuffix = bestData.Commit;
        state.ReplaceLogSuffix(baseCommitForSuffix + 1, bestData.LogSuffix);
        Log.Information(
            "Replica {ReplicaId} (New Primary): Applied log suffix from Replica {BestSenderId} starting after Op {BestCommit}. New state.Op is now {NewOp}",
            state.Replica, bestData.SenderId, bestData.Commit, state.Op);
        if (newOpNumber < newCommitNumber)
        {
            Log.Warning(
                "Replica {ReplicaId} (New Primary): Selected bestData.Op ({SelectedOp}) is less than determined newCommitNumber ({NewCommit}). Adjusting Op upwards.",
                state.Replica, newOpNumber, newCommitNumber);
            newOpNumber = newCommitNumber; // Ensure Op is at least Commit
        }
        
        state.Op = newOpNumber;
        state.SetStatusNormal(receivedView, state.Op, newCommitNumber);

        await ExecuteAndReplyNewlyCommittedOpsAsync(oldCommitNumber, newCommitNumber, context);

        var finalLogSuffix = state.GetLogSuffix(newCommitNumber);

        Log.Debug(
            "Replica {ReplicaId} (New Primary): Preparing StartView V:{View}, Op:{Op}, Commit:{Commit} with {SuffixCount} log entries.",
            state.Replica, state.View, state.Op, state.Commit, finalLogSuffix.Count);
        var startViewPayload = new StartView(finalLogSuffix);
        var svPayloadSize = StartView.CalculateSerializedSize(startViewPayload);
        var payloadBytes = new byte[svPayloadSize];


        StartView.Serialize(startViewPayload, payloadBytes.AsSpan());
        var svPayloadMemory = payloadBytes.AsMemory();

        var startViewHeader = new VsrHeader(
            parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(),
            bodySize: (uint)svPayloadSize,
            request: 0, cluster: state.Cluster, epoch: state.Epoch,
            view: state.View, op: state.Op,
            commit: state.Commit, offset: 0,
            replica: state.Replica, command: Command.StartView,
            operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
        );
        var startViewMessage = new VsrMessage(startViewHeader, svPayloadMemory);
        using var serializedSv = VsrMessageSerializer.SerializeMessage(startViewMessage, state.MemoryPool);
        await context.BroadcastAsync(serializedSv).ConfigureAwait(false);
        Log.Information("Replica {ReplicaId} (Primary): Broadcasted StartView for V:{View}.", state.Replica,
            state.View); 
        return true;
    }

    private async Task ExecuteAndReplyNewlyCommittedOpsAsync(ulong previousCommit, ulong currentCommit,
        IReplicaContext context)
    {
        var state = context.State;
        if (currentCommit <= previousCommit)
        {
            Log.Debug(
                "Replica {ReplicaId} (Primary): No new operations to execute post-view-change (PrevCommit:{Prev}, CurrCommit:{Curr})",
                state.Replica, previousCommit, currentCommit);
            return;
        }

        Log.Information(
            "Replica {ReplicaId} (Primary): Executing ops {StartOp}..{EndOp} committed during view change.",
            state.Replica, previousCommit + 1, currentCommit);

        var stopwatch = Stopwatch.StartNew();
        var executedCount = 0;
        var replySentCount = 0;
        var replyFailedCount = 0;

        for (var op = previousCommit + 1; op <= currentCommit; op++)
        {
            var logEntry = state.GetLogEntry(op);
            if (logEntry == null) // Add null check
            {
                Log.Error(
                    "Replica {ReplicaId} (Primary): Log entry missing for Op={OpNumber} during commit sequence. Stopping.",
                    state.Replica, op);
                break;
            }

            var clientEntry = state.GetClientTableEntry(logEntry.Client);
            var alreadyExecuted =
                clientEntry != null && clientEntry.Request == logEntry.Request && clientEntry.Executed;

            if (alreadyExecuted)
            {
                Log.Debug("Replica {ReplicaId} (Primary): Op {OpNumber} was already executed. Skipping execution.",
                    state.Replica, op);
                // Ensure commit number is advanced if needed (ExecuteAndCommitOperation handles this)
                if (op > state.Commit) state.Commit = op;
                continue;
            }

            Log.Debug("Replica {ReplicaId} (Primary): Executing Op {OpNumber} post-view-change.", state.Replica, op);
            var result = state
                .ExecuteAndCommitOperation(op);

            if (result == null)
            {
                Log.Error(
                    "Replica {ReplicaId} (Primary): Failed to execute Op {OpNumber} post-view-change, though it should be committed. Stopping execution.",
                    state.Replica, op);
                break;
            }

            executedCount++;
            Log.Information("Replica {ReplicaId} (Primary): Executed Op {OpNumber} post-view-change.", state.Replica,
                op);
            var replyHeader = new VsrHeader(
                parent: 0, // Or a relevant context if available from original request
                client: logEntry.Client,
                context: BinaryUtils.NewGuidUInt128(), // Generate a new context for the reply
                bodySize: (uint)result.Length,
                request: logEntry.Request, // Original request number
                cluster: state.Cluster,
                epoch: state.Epoch,
                view: state.View,       // Current view of the new primary
                op: op,                 // Op number that generated the reply
                commit: state.Commit,   // Current commit number of the new primary
                offset: 0,
                replica: state.Replica, // Sender is this new primary
                command: Command.Reply,
                operation: logEntry.Operation, // Original operation type
                version: GlobalConfig.CurrentVersion
            );
            var replyMessage = new VsrMessage(replyHeader, result.AsMemory());
            state.UpdateClientTable(logEntry.Client, logEntry.Request, replyMessage);

           

            var clientId = logEntry.Client;
            var clientConnectionId = state.GetClientConnectionId(clientId);
            Log.Information(
                "Replica {ReplicaId} (Primary): Sending REPLY for Op {OpNumber} (Client {ClientId}, Req {Request}) to ConnId {ConnId}.",
                state.Replica, op, clientId, logEntry.Request, clientConnectionId.Id);

            using var serializedReply =
                VsrMessageSerializer.SerializeMessage(replyMessage, state.MemoryPool);
            try
            {
                await context.SendAsync(clientConnectionId, serializedReply).ConfigureAwait(false);
                replySentCount++;
            }
            catch (Exception ex)
            {
                Log.Warning(ex,
                    "Replica {ReplicaId} (Primary): Failed to send REPLY for Op {OpNumber} to Client {ClientId} (ConnId {ConnId}). Client might have disconnected.",
                    state.Replica, op, clientId, clientConnectionId.Id);
                replyFailedCount++;
                // TODO: Thinking about cleaning client connection id mapping
            }
        }

        stopwatch.Stop();
        Log.Information(
            "Replica {ReplicaId} (Primary): Finished post-view-change execution. Executed:{Executed}, Replies Sent:{Sent}, Replies Failed:{Failed}. Duration:{DurationMs}ms",
            state.Replica, executedCount, replySentCount, replyFailedCount, stopwatch.ElapsedMilliseconds);
    }
}