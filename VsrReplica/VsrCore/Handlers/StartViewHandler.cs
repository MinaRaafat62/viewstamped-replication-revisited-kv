using System.Diagnostics;
using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.Handlers;

public class StartViewHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        var primaryId = header.Replica;
        var receivedView = header.View;

        Log.Debug(
            "Replica {ReplicaId}: Rcvd StartView V:{View} from Primary {PrimaryId}. MyV:{MyView}, Status:{Status}, TargetV:{TargetV}",
            state.Replica, receivedView, primaryId, state.View, state.Status, state.StatusViewNumber);

        // --- Basic Validations ---
        if (state.Replica == primaryId)
        {
            Log.Warning("Replica {ReplicaId} (Primary): Received StartView from self? Ignoring.", state.Replica);
            return false;
        }

        byte expectedPrimary = (byte)(receivedView % state.TotalReplicas);
        if (primaryId != expectedPrimary)
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring StartView for V:{View}. Sender {SenderId} is not the expected primary ({ExpectedPrimary}).",
                state.Replica, receivedView, primaryId, expectedPrimary);
            return false;
        }

        // Ignore if message is for a view older than our current view or target view change.
        if (receivedView < state.View ||
            (state.Status == ReplicaStatus.ViewChange && receivedView < state.StatusViewNumber))
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring StartView for past V:{View} (Current:{MyView}, Target:{TargetV}).",
                state.Replica, receivedView, state.View, state.StatusViewNumber);
            return false;
        }

        // Ignore if we are Normal and the view matches (duplicate message)
        if (state.Status == ReplicaStatus.Normal && state.View == receivedView)
        {
            Log.Information("Replica {ReplicaId}: Received duplicate StartView for current V:{View}. Ignoring.",
                state.Replica, receivedView);
            return false;
        }

        // If we are in ViewChange, ensure it matches the target view
        if (state.Status == ReplicaStatus.ViewChange && receivedView != state.StatusViewNumber)
        {
            Log.Warning("Replica {ReplicaId}: Ignoring StartView for V:{View}. Target View Mismatch ({TargetV}).",
                state.Replica, receivedView, state.StatusViewNumber);
            return false;
        }

        // Allow transition if Normal but future view, or if Recovering
        if (state.Status != ReplicaStatus.ViewChange && state.Status != ReplicaStatus.Recovering &&
            receivedView <= state.View)
        {
            Log.Warning("Replica {ReplicaId}: Ignoring StartView for V:{View}. Unexpected Status: {Status}.",
                state.Replica, receivedView, state.Status);
            return false;
        }


        // --- Process StartView ---
        Log.Information("Replica {ReplicaId}: Processing StartView for V:{View} from Primary {PrimaryId}.",
            state.Replica, receivedView, primaryId);

        // Deserialize payload
        StartView payload;
        try
        {
            payload = StartView.Deserialize(message.Payload.Span);
        }
        catch (Exception ex)
        {
            Log.Error(ex,
                "Replica {ReplicaId}: Failed to deserialize StartView payload from Primary {PrimaryId} for V:{View}. Requesting recovery?",
                state.Replica, primaryId, receivedView);
            // TODO: Initiate state transfer / recovery mechanism here?
            return false; // Cannot proceed without valid payload
        }

        // --- Update State (Step 5.1 - 5.5) ---
        ulong oldCommitNumber = state.Commit; // Store commit number before update
        ulong newCommitNumber = header.Commit;
        ulong newOpNumber = header.Op;

        // Replace log suffix (Payload contains the suffix chosen by the primary)
        // The suffix starts *after* the commit number determined by the primary.
        state.ReplaceLogSuffix(newCommitNumber + 1, payload.LogSuffix);

        // Set final state variables and transition to Normal
        state.SetStatusNormal(receivedView, newOpNumber, newCommitNumber);

        // --- Post-View Change Actions (Step 5.6 - 5.9) ---

        // 5.6 Send PrepareOk for non-committed operations in the log to the new primary
        await SendPrepareOksForNewLogAsync(context);

        // 5.7 Execute all operations known to be committed that haven't been executed
        await ExecuteNewlyCommittedOpsAsync(oldCommitNumber, newCommitNumber, context);

        // 5.8 Commit number advanced by execution. Ensure it's at least the header commit.
        if (state.Commit >= newCommitNumber) return true; // Successfully processed StartView
        state.Commit = newCommitNumber;
        Log.Information("Replica {ReplicaId}: Advanced commit number to {CommitNum} after StartView processing.",
            state.Replica, newCommitNumber);
        // 5.9 Client table updated by execution.

        // Timers will be updated by the main loop calling UpdateTimerStates

        return true; // Successfully processed StartView
    }

    /// <summary>
    /// Sends PrepareOk messages to the primary for operations in the log
    /// that are after the current commit number. (Step 5.6)
    /// </summary>
    private async Task SendPrepareOksForNewLogAsync(IReplicaContext context)
    {
        var state = context.State;
        var primaryId = state.PrimaryReplica;
        var primaryConnId = context.GetConnectionIdForReplica(primaryId);

        if (!primaryConnId.HasValue)
        {
            Log.Warning(
                "Replica {ReplicaId}: Cannot send PrepareOk after StartView, new primary {PrimaryId} not connected.",
                state.Replica, primaryId);
            return;
        }

        var sentCount = 0;
        for (var op = state.Commit + 1; op <= state.Op; op++)
        {
            var entry = state.GetLogEntry(op);
            var prepareOkHeader = new VsrHeader(
                parent: 0, client: 0, context: 0, // Context not specified for this PrepareOk
                bodySize: 0, request: 0, cluster: state.Cluster, epoch: state.Epoch,
                view: state.View,
                op: op,
                commit: state.Commit, // Send current commit number
                offset: 0,
                replica: state.Replica, // Sender is this backup replica
                command: Command.PrepareOk,
                operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
            );
            var prepareOkMessage = new VsrMessage(prepareOkHeader, Memory<byte>.Empty);
            using var serializedOk = VsrMessageSerializer.SerializeMessage(prepareOkMessage, state.MemoryPool);

            try
            {
                await context.SendAsync(primaryConnId.Value, serializedOk).ConfigureAwait(false);
                sentCount++;
                Log.Debug(
                    "Replica {ReplicaId}: Sent PrepareOk for Op {Op} (V:{View}) to Primary {PrimaryId} after StartView.",
                    state.Replica, op, state.View, primaryId);
            }
            catch (Exception ex)
            {
                Log.Warning(ex,
                    "Replica {ReplicaId}: Failed to send PrepareOk for Op {Op} after StartView to Primary {PrimaryId}.",
                    state.Replica, op, primaryId);
                // If sending fails, primary might not reach quorum for this op later.
                // We might need to retry or rely on primary re-preparing. Continue for now.
            }
            // serializedOk disposed automatically by using
        }

        Log.Information("Replica {ReplicaId}: Sent {Count} PrepareOk messages for new log entries.", state.Replica,
            sentCount);
    }

    /// <summary>
    /// Executes operations that became committed after processing StartView. (Step 5.7)
    /// </summary>
    private async Task ExecuteNewlyCommittedOpsAsync(ulong previousCommit, ulong currentCommit, IReplicaContext context)
    {
        var state = context.State;
        if (currentCommit <= previousCommit)
        {
            Log.Debug(
                "Replica {ReplicaId}: No new operations to execute post-StartView (PrevCommit:{Prev}, CurrCommit:{Curr})",
                state.Replica, previousCommit, currentCommit);
            return;
        }

        Log.Information("Replica {ReplicaId}: Executing ops {StartOp}..{EndOp} committed via StartView.",
            state.Replica, previousCommit + 1, currentCommit);

        var stopwatch = Stopwatch.StartNew();
        var executedCount = 0;

        for (var op = previousCommit + 1; op <= currentCommit; op++)
        {
            var logEntry = state.GetLogEntry(op);

            var clientEntry = state.GetClientTableEntry(logEntry.Client);
            var alreadyExecuted =
                clientEntry != null && clientEntry.Request == logEntry.Request && clientEntry.Executed;

            if (alreadyExecuted)
            {
                Log.Debug("Replica {ReplicaId}: Op {OpNumber} was already executed. Skipping execution.",
                    state.Replica, op);
                if (op > state.Commit) state.Commit = op; // Ensure commit number advances
                continue;
            }

            // Execute the operation
            Log.Debug("Replica {ReplicaId}: Executing Op {OpNumber} post-StartView.", state.Replica, op);
            byte[]? result = state.ExecuteAndCommitOperation(op); // Advances state.Commit and updates client table

            if (result == null)
            {
                Log.Error(
                    "Replica {ReplicaId}: Failed to execute Op {OpNumber} post-StartView, though it should be committed. Stopping execution.",
                    state.Replica, op);
                break; // Stop processing further ops if one fails
            }

            executedCount++;
            Log.Information("Replica {ReplicaId}: Executed Op {OpNumber} post-StartView.", state.Replica, op);
            // No replies sent by backups
        }

        stopwatch.Stop();
        Log.Information(
            "Replica {ReplicaId}: Finished post-StartView execution. Executed:{Executed}. Duration:{DurationMs}ms",
            state.Replica, executedCount, stopwatch.ElapsedMilliseconds);

        await Task.CompletedTask; // Added for async signature, can be removed if no awaits inside loop
    }
}