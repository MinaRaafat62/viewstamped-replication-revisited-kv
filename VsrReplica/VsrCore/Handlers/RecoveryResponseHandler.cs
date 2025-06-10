using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.Handlers;

public class RecoveryResponseHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        var senderId = header.Replica;

        if (state.Status != ReplicaStatus.Recovering)
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring RecoveryResponse from {SenderId}. Status is {Status}, not Recovering.",
                state.Replica, senderId, state.Status);
            return false;
        }

        RecoveryResponse responsePayload;
        try
        {
            responsePayload = RecoveryResponse.Deserialize(message.Payload.Span, out _);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to deserialize RecoveryResponse payload from {SenderId}.",
                state.Replica, senderId);
            return false;
        }

        if (responsePayload.Nonce != state.CurrentRecoveryNonce)
        {
            Log.Warning(
                "Replica {ReplicaId}: Ignoring RecoveryResponse from {SenderId}. Nonce mismatch (Expected: {Expected}, Got: {Got}).",
                state.Replica, senderId, state.CurrentRecoveryNonce, responsePayload.Nonce);
            return false;
        }

        Log.Debug(
            "Replica {ReplicaId}: Rcvd valid RecoveryResponse V:{View} Op:{Op} C:{Commit} from {SenderId} (IsPrimary={IsPrimary}, LogEntries={LogCount}) for Nonce {Nonce}.",
            state.Replica, header.View, header.Op, header.Commit, senderId, responsePayload.IsPrimary,
            responsePayload.Logs.Count, responsePayload.Nonce);

        var responseData = new ReplicaState.RecoveryResponseData(
            senderId,
            responsePayload.Nonce,
            header.View,
            header.Op,
            header.Commit,
            responsePayload.IsPrimary,
            responsePayload.Logs // not the most efficient, but ok for now
        );
        var quorumReached = state.AddRecoveryResponse(responseData);
        if (!quorumReached)
        {
            Log.Debug("Replica {ReplicaId}: Added RecoveryResponse from {SenderId}. Waiting for quorum.",
                state.Replica, senderId);
            return true; // Processed, but no quorum yet
        }

        Log.Information("Replica {ReplicaId}: RecoveryResponse quorum reached.", state.Replica);
        var (quorumData, primaryData) = state.GetRecoveryQuorumData();

        if (quorumData == null || primaryData == null)
        {
            Log.Error(
                "Replica {ReplicaId}: Recovery quorum reached, but failed to get primary data (Primary in highest view {HighestView} might have failed?). Recovery cannot complete.",
                state.Replica,
                state.GetRecoveryQuorumData().QuorumData?.FirstOrDefault()?.View ??
                state.View);
            return false;
        }

        Log.Information(
            "Replica {ReplicaId}: Identified Primary {PrimaryId} in highest view {HighestView} as source for recovery state.",
            state.Replica, primaryData.SenderId, primaryData.View);

        var oldCommitNumber = state.Commit; // Store before update
        var newView = primaryData.View;
        var newOp = primaryData.Op;
        var newCommit = primaryData.Commit;
        var primaryFullLog = primaryData.Logs;
        state.ReplaceEntireLog(primaryFullLog);
        state.ClearClientTable();
        try
        {
            await ApplyAdoptedLogToStateMachineAsync(newCommit, context);
        }
        catch (Exception ex)
        {
            Log.Error(ex,
                "Replica {ReplicaId}: Failed to apply adopted log to state machine during recovery. Recovery failed.",
                state.Replica);
            return false;
        }

        state.SetStatusNormal(newView, newOp, newCommit);
        Log.Information(
            "Replica {ReplicaId}: Recovery complete. Adopted state from Primary {PrimaryId}. Transitioned to Normal. View:{View}, Op:{Op}, Commit:{Commit}",
            state.Replica, primaryData.SenderId, state.View, state.Op, state.Commit);
        return true;
    }

    private async Task ApplyAdoptedLogToStateMachineAsync(ulong targetCommitNumber, IReplicaContext context)
    {
        var state = context.State;

        if (targetCommitNumber == 0)
        {
            Log.Information("Replica {ReplicaId}: No operations to execute post-recovery (Adopted Commit=0).",
                state.Replica);
            if (state.Commit != 0) state.Commit = 0;
            return;
        }

        Log.Information("Replica {ReplicaId}: Applying adopted log to state machine up to Commit={TargetCommit}.",
            state.Replica, targetCommitNumber);

        var executedCount = 0;

        for (var opToExecute = 1UL; opToExecute <= targetCommitNumber; opToExecute++)
        {
            var logEntry = state.GetLogEntry(opToExecute);
            if (logEntry == null)
            {
                Log.Error(
                    "Replica {ReplicaId}: CRITICAL - Log entry missing for Op {OpNumber} (<= Adopted Commit {CommitNum}) in newly adopted log during state machine application!",
                    state.Replica, opToExecute, targetCommitNumber);
                await context.InitiateRecoveryAsync();
                throw new InvalidOperationException(
                    $"Log entry missing for committed op {opToExecute} during recovery state application.");
            }

            if (opToExecute != state.Commit + 1)
            {
                Log.Error(
                    "Replica {ReplicaId}: CRITICAL - State machine application failure. Expected to execute Op {ExpectedOp} but state.Commit is {ActualCommit}.",
                    state.Replica, state.Commit + 1, state.Commit);
                await context.InitiateRecoveryAsync();
                throw new InvalidOperationException(
                    $"Commit number mismatch during recovery state application. Expected {state.Commit + 1}, trying {opToExecute}.");
            }

            Log.Debug("Replica {ReplicaId}: Applying Op {OpNumber} to state machine post-recovery.", state.Replica,
                opToExecute);
            byte[]? result = state.ExecuteAndCommitOperation(opToExecute);

            if (result == null)
            {
                Log.Error(
                    "Replica {ReplicaId}: State machine application failed for Op {OpNumber} post-recovery. Stopping application.",
                    state.Replica, opToExecute);
                await context.InitiateRecoveryAsync();
                throw new InvalidOperationException(
                    $"State machine Apply failed for op {opToExecute} during recovery.");
            }

            executedCount++;
        }

        if (state.Commit != targetCommitNumber)
        {
            Log.Error(
                "Replica {ReplicaId}: CRITICAL - Final state.Commit ({ActualCommit}) does not match targetCommitNumber ({TargetCommit}) after applying log.",
                state.Replica, state.Commit, targetCommitNumber);
            await context.InitiateRecoveryAsync();
            throw new InvalidOperationException("Commit number mismatch after recovery state application.");
        }

        Log.Information(
            "Replica {ReplicaId}: Finished applying adopted log to state machine. Executed:{Executed}. Final Commit:{Commit}.",
            state.Replica, executedCount, state.Commit);

        await Task.CompletedTask;
    }
}