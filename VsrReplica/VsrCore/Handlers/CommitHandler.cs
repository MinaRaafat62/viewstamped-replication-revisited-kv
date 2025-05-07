using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.Handlers;

public class CommitHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;

        if (state.IsPrimary)
        {
            Log.Warning("Replica {ReplicaId} (Primary): Received COMMIT message. Ignoring.", state.Replica);
            return false;
        }

        if (state.Status != ReplicaStatus.Normal)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received COMMIT up to Op={CommitNum} from {SenderId} but status is {Status}. Ignoring.",
                state.Replica, header.Commit, header.Replica, state.Status);
            return false;
        }

        if (header.View < state.View)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received COMMIT from old view {MsgView} (current is {CurrentView}). Ignoring.",
                state.Replica, header.View, state.View);
            return false;
        }

        if (header.View > state.View)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received COMMIT from new view {MsgView} (current is {CurrentView}). View change needed? Ignoring for now.",
                state.Replica, header.View, state.View);
            // TODO: Handle view change initiation or message buffering
            return false;
        }

        var commitNumber = header.Commit;

        if (commitNumber > state.Commit)
        {
            Log.Information(
                "Replica {ReplicaId}: Received COMMIT message. Primary commit is {PrimaryCommit}, local commit is {LocalCommit}. Processing commits.",
                state.Replica, commitNumber, state.Commit);
            await CommitOperationsAsync(commitNumber, context);
        }
        else
        {
            Log.Debug(
                "Replica {ReplicaId}: Received COMMIT message but Primary commit {PrimaryCommit} is not ahead of local commit {LocalCommit}. No action needed.",
                state.Replica, commitNumber, state.Commit);
        }

        return true;
    }

    private async Task CommitOperationsAsync(ulong targetCommitNumber, IReplicaContext context)
    {
        var state = context.State;
        var opToCommit = state.Commit + 1;

        while (opToCommit <= targetCommitNumber)
        {
            var logEntry = state.GetLogEntry(opToCommit);
            if (logEntry == null)
            {
                if (opToCommit <= state.Op)
                {
                    Log.Error(
                        "Replica {ReplicaId}: Log entry missing for Op={OpNumber} (State.Op={CurrentOp}) which should be present! Initiating recovery.",
                        state.Replica, opToCommit, state.Op);
                    await context.InitiateRecoveryAsync();
                    return;
                }

                Log.Warning(
                    "Replica {ReplicaId}: Received commit instruction for future Op={OpNumber} but log entry not found (State.Op={CurrentOp}). Waiting for PREPARE.",
                    state.Replica, opToCommit, state.Op);
                break;
            }

            var result = state.ExecuteAndCommitOperation(opToCommit);
            if (result == null)
            {
                Log.Error(
                    "Replica {ReplicaId}: Failed to commit Op={OpNumber} which should have been ready. Stopping commit sequence.",
                    state.Replica, opToCommit);
                break;
            }

            Log.Information(
                "Replica {ReplicaId}: Successfully committed and executed Op={OpNumber} based on COMMIT message.",
                state.Replica, opToCommit);

            opToCommit++;
        }

        // Small optimization: If we committed everything up to targetCommitNumber,
        // we don't strictly need to await anything here, but added for potential future async operations within the loop.
        await Task.CompletedTask;
    }
}