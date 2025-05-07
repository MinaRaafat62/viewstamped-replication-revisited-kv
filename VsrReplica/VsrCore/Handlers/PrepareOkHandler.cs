using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Handlers;

public class PrepareOkHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        if (!state.IsPrimary)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received PREPAREOK Op={OpNumber} from {SenderId} but not Primary. Ignoring.",
                state.Replica, header.Op, header.Replica);
            return false;
        }

        if (state.Status != ReplicaStatus.Normal)
        {
            Log.Warning(
                "Replica {ReplicaId} (Primary): Received PREPAREOK Op={OpNumber} from {SenderId} but status is {Status}. Ignoring.",
                state.Replica, header.Op, header.Replica, state.Status);
            return false;
        }

        if (header.View < state.View)
        {
            Log.Warning(
                "Replica {ReplicaId} (Primary): Received PREPAREOK Op={OpNumber} from old view {MsgView} (current is {CurrentView}). Ignoring.",
                state.Replica, header.Op, header.View, state.View);
            return false;
        }

        if (header.View > state.View)
        {
            Log.Warning(
                "Replica {ReplicaId} (Primary): Received PREPAREOK Op={OpNumber} from new view {MsgView} (current is {CurrentView}). View change needed? Ignoring for now.",
                state.Replica, header.Op, header.View, state.View);
            // TODO: Handle view change initiation or message buffering
            return false;
        }

        var opNumber = header.Op;
        var senderReplicaId = header.Replica;

        var prepareOkCount = state.AddPrepareOk(opNumber, senderReplicaId);

        if (prepareOkCount == state.QuorumSize)
        {
            Log.Information(
                "Replica {ReplicaId} (Primary): Op={OpNumber} reached quorum ({Count}/{Needed}) after PrepareOk from {SenderId}.",
                state.Replica, opNumber, prepareOkCount, state.QuorumSize, senderReplicaId);

            await CommitAndReplyIfReadyAsync(opNumber, context);
        }
        else if (prepareOkCount > state.QuorumSize)
        {
            Log.Debug(
                "Replica {ReplicaId} (Primary): Received extra PrepareOk for Op={OpNumber} from {SenderId}. Count={Count}/{Needed}.",
                state.Replica, opNumber, senderReplicaId, prepareOkCount, state.QuorumSize);
        }
        else
        {
            Log.Debug(
                "Replica {ReplicaId} (Primary): Received PREPAREOK for Op={OpNumber} from {SenderId}. Count={Count}/{Needed}. Waiting for more.",
                state.Replica, opNumber, senderReplicaId, prepareOkCount, state.QuorumSize);
        }


        return true; // Processed PrepareOK
    }

    private async Task CommitAndReplyIfReadyAsync(ulong opNumberReachedQuorum, IReplicaContext context)
    {
        var state = context.State;
        if (!state.IsPrimary) return;
        var currentOp = state.Commit + 1;

        while (currentOp <= opNumberReachedQuorum && state.HasEnoughPrepareOks(currentOp))
        {
            var logEntry = state.GetLogEntry(currentOp);
            if (logEntry == null) // Add null check
            {
                Log.Error(
                    "Replica {ReplicaId} (Primary): Log entry missing for Op={OpNumber} during commit sequence. Stopping.",
                    state.Replica, currentOp);
                break; // Or handle appropriately
            }

            var clientEntry = state.GetClientTableEntry(logEntry.Client);
            var needsExecution = clientEntry is not { Executed: true } ||
                                 clientEntry.Request != logEntry.Request;

            byte[]? result = null;
            if (needsExecution)
            {
                result = state.ExecuteAndCommitOperation(currentOp);
            }
            else
            {
                // Already executed, just ensure commit number advances if needed
                if (currentOp == state.Commit + 1)
                {
                    state.Commit = currentOp; // Advance commit number even if already executed
                    Log.Debug(
                        "Replica {ReplicaId} (Primary): Advanced commit number to {CommitNum} for already executed Op={OpNumber}.",
                        state.Replica, state.Commit, currentOp);
                }

                result = clientEntry?.Result;
            }


            if (result != null)
            {
                if (needsExecution)
                {
                    Log.Information("Replica {ReplicaId} (Primary): Successfully committed and executed Op={OpNumber}.",
                        state.Replica, currentOp);
                }

                var replyHeader = new VsrHeader(
                    parent: 0,
                    client: logEntry.Client,
                    context: BinaryUtils.NewGuidUInt128(),
                    bodySize: (uint)result.Length,
                    request: logEntry.Request,
                    cluster: state.Cluster,
                    epoch: state.Epoch,
                    view: state.View,
                    op: currentOp,
                    commit: state.Commit,
                    offset: 0,
                    replica: state.Replica,
                    command: Command.Reply,
                    operation: logEntry.Operation,
                    version: GlobalConfig.CurrentVersion
                );
                var replyMessage = new VsrMessage(replyHeader, result);

                state.UpdateClientTable(logEntry.Client, logEntry.Request, replyMessage);

                if (needsExecution)
                {
                    if (state.TryRemovePendingClientConnection(currentOp, out var clientConnId))
                    {
                        Log.Information(
                            "Replica {ReplicaId} (Primary): Sending REPLY for Op={OpNumber}, Req={RequestNum} to Client Connection {ConnId}",
                            state.Replica, currentOp, logEntry.Request, clientConnId.Id);
                        using var serializedReply =
                            VsrMessageSerializer.SerializeMessage(replyMessage, state.MemoryPool);
                        try
                        {
                            await context.SendAsync(clientConnId, serializedReply).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Log.Warning(ex,
                                "Replica {ReplicaId} (Primary): Failed to send REPLY for Op={OpNumber} to Client Connection {ConnId}. Client might have disconnected.",
                                state.Replica, currentOp, clientConnId.Id);
                        }
                    }
                    else
                    {
                        Log.Warning(
                            "Replica {ReplicaId} (Primary): Committed Op={OpNumber} but could not find pending client connection to send REPLY.",
                            state.Replica, currentOp);
                    }
                }
            }
            else
            {
                Log.Error(
                    "Replica {ReplicaId} (Primary): Failed to commit/execute Op={OpNumber} even though quorum was met. Stopping commit sequence.",
                    state.Replica, currentOp);
                break;
            }

            currentOp++;
        }
    }
}