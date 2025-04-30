using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Handlers;

public class PrepareHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;

        if (state.IsPrimary)
        {
            Log.Warning("Replica {ReplicaId} (Primary): Received PREPARE message. Ignoring.", state.Replica);
            return false; // Primary should not receive PREPARE
        }

        if (state.Status != ReplicaStatus.Normal)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received PREPARE Op={OpNumber} from {SenderId} but status is {Status}. Ignoring.",
                state.Replica, header.Op, header.Replica, state.Status);
            return false;
        }

        if (header.View < state.View)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received PREPARE Op={OpNumber} from old view {MsgView} (current is {CurrentView}). Ignoring.",
                state.Replica, header.Op, header.View, state.View);
            return false;
        }

        if (header.View > state.View)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received PREPARE Op={OpNumber} from new view {MsgView} (current is {CurrentView}). View change needed.",
                state.Replica, header.Op, header.View, state.View);
            // TODO: Handle view change initiation or message buffering
            return false;
        }

        var opNumber = header.Op;
        var commitNumber = header.Commit;

        if (opNumber <= state.Op)
        {
            // Duplicate PREPARE or already processed
            Log.Information(
                "Replica {ReplicaId}: Received duplicate PREPARE for Op={OpNumber} (Current Op={CurrentOp}). Sending PREPAREOK again.",
                state.Replica, opNumber, state.Op);
            // Resend PrepareOK to be safe
            await SendPrepareOkAsync(message, opNumber, context);
            return true; // Handled
        }

        if (opNumber > state.Op + 1)
        {
            // Log hole detected!
            Log.Warning(
                "Replica {ReplicaId}: Received PREPARE for Op={OpNumber} but expected Op={ExpectedOp}. Log hole detected! State transfer needed.",
                state.Replica, opNumber, state.Op + 1);
            // TODO: Initiate state transfer / recovery mechanism
            return false; // Cannot process this PREPARE yet
        }

        var appendedOpNumber = state.AppendLogEntry(header.Operation, message.Payload, header);
        if (appendedOpNumber != opNumber)
        {
            Log.Error(
                "Replica {ReplicaId}: CRITICAL - Op number mismatch after appending log entry. Expected {ExpectedOp}, Got {GotOp}. State inconsistent.",
                state.Replica, opNumber, appendedOpNumber);
            // This indicates a serious internal logic error.
            return false;
        }

        state.UpdateClientTable(header.Client, header.Request);

        await SendPrepareOkAsync(message, opNumber, context);

        if (commitNumber <= state.Commit) return true;
        Log.Information(
            "Replica {ReplicaId}: PREPARE for Op={OpNumber} carried new commit number {NewCommit} (Current={CurrentCommit}). Processing commits.",
            state.Replica, opNumber, commitNumber, state.Commit);
        await CommitOperationsAsync(commitNumber, context);

        return true;
    }

    private async Task SendPrepareOkAsync(VsrMessage message, ulong opNumber, IReplicaContext context)
    {
        var state = context.State;
        var primaryReplicaId = state.PrimaryReplica;
        var primaryConnectionId = context.GetConnectionIdForReplica(primaryReplicaId);

        if (!primaryConnectionId.HasValue)
        {
            Log.Error(
                "Replica {ReplicaId}: Cannot send prepare ok for Op={OpNumber}. Primary Replica {PrimaryId} is not connected.",
                state.Replica, opNumber, primaryReplicaId);
            return;
        }

        var prepareOkHeader = new VsrHeader(
            parent: 0,
            client: 0, // Not relevant for PrepareOK directly
            context: message.Header.Checksum,
            bodySize: 0,
            request: 0, // Not relevant
            cluster: state.Cluster,
            epoch: state.Epoch,
            view: state.View,
            op: opNumber,
            commit: state.Commit,
            offset: 0,
            replica: state.Replica, // Sender is this backup replica
            command: Command.PrepareOk,
            operation: Operation.Reserved,
            version: GlobalConfig.CurrentVersion
        );
        var prepareOkMessage = new VsrMessage(prepareOkHeader, Memory<byte>.Empty);

        Log.Debug(
            "Replica {ReplicaId}: Sending PREPAREOK for Op={OpNumber}, View={View} to Primary {PrimaryId} ({ConnId})",
            state.Replica, opNumber, state.View, primaryReplicaId, primaryConnectionId.Value.Id);

        using var serializedOk = VsrMessageSerializer.SerializeMessage(prepareOkMessage, state.MemoryPool);
        try
        {
            await context.SendAsync(primaryConnectionId.Value, serializedOk).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Warning(ex,
                "Replica {ReplicaId}: Failed to send PREPAREOK for Op={OpNumber} to Primary {PrimaryId} ({ConnId}).",
                state.Replica, opNumber, primaryReplicaId, primaryConnectionId.Value.Id);
        }
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
                // This backup doesn't have the log entry yet.
                // This can happen if prepares arrive out of order or are lost.
                // The backup must wait for the corresponding - PREPARE message to arrive.
                Log.Warning(
                    "Replica {ReplicaId}: Received commit instruction for Op={OpNumber} but log entry not found. Waiting for PREPARE.",
                    state.Replica, opToCommit);
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
                "Replica {ReplicaId}: Successfully committed and executed Op={OpNumber} based on piggybacked commit number.",
                state.Replica, opToCommit);

            opToCommit++;
        }

        // Small optimization: If we committed everything up to targetCommitNumber,
        // we don't strictly need to await anything here, but added for potential future async operations within the loop.
        await Task.CompletedTask;
    }
}