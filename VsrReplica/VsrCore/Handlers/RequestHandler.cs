using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Handlers;

public class RequestHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        if (!state.IsPrimary || state.Status != ReplicaStatus.Normal)
        {
            Log.Warning(
                "Replica {ReplicaId}: Received Request from {ClientId} but not Primary or not Normal. Status={Status}, IsPrimary={IsPrimary}. Ignoring.",
                state.Replica, connectionId?.Id ?? -1, state.Status, state.IsPrimary);
            return false;
        }

        if (!connectionId.HasValue)
        {
            Log.Error("Replica {ReplicaId}: Received Request but ConnectionId is null. Cannot process.", state.Replica);
            return false;
        }

        var header = message.Header;
        var clientId = header.Client;
        var requestNumber = header.Request;
        var clientEntry = state.GetClientTableEntry(clientId);
        state.AddConnectionToClient(connectionId.Value, clientId);
        if (clientEntry != null && requestNumber < clientEntry.Request)
        {
            // Stale request, ignore
            Log.Information(
                "Replica {ReplicaId} (Primary): Received stale request {RequestNum} from client {Client} via {ConnId}. Current is {CurrentNum}. Ignoring.",
                state.Replica, requestNumber, clientId, connectionId.Value.Id, clientEntry.Request);
            return true;
        }

        if (clientEntry != null && requestNumber == clientEntry.Request)
        {
            if (clientEntry.Response != null)
            {
                Log.Information(
                    "Replica {ReplicaId} (Primary): Resending reply for duplicate request {RequestNum} from client {Client} via {ConnId}.",
                    state.Replica, requestNumber, clientId, connectionId.Value.Id);
                using var serializedReply =
                    VsrMessageSerializer.SerializeMessage(clientEntry.Response, state.MemoryPool);
                await context.SendAsync(connectionId.Value, serializedReply).ConfigureAwait(false);
            }
            else
            {
                Log.Warning(
                    "Replica {ReplicaId} (Primary): Received duplicate request {RequestNum} from client {Client} via {ConnId}, but response not yet available.",
                    state.Replica, requestNumber, clientId, connectionId.Value.Id);
            }

            return true;
        }

        var opNumber = state.AppendLogEntry(header.Operation, message.Payload, header);
        state.UpdateClientTable(clientId, requestNumber);
        state.AddPendingClientConnection(opNumber, connectionId.Value);
        var prepareHeader = new VsrHeader(
            header.Context, clientId, message.Header.Checksum,
            (uint)message.Payload.Length, requestNumber, state.Cluster, state.Epoch,
            state.View, opNumber, state.Commit, 0, state.Replica, Command.Prepare, header.Operation,
            GlobalConfig.CurrentVersion);
        var prepareMessage = new VsrMessage(prepareHeader, message.Payload);
        using var serializedPrepareMessage =
            VsrMessageSerializer.SerializeMessage(prepareMessage, state.MemoryPool);
        await context.BroadcastAsync(serializedPrepareMessage).ConfigureAwait(false);
        var prepareOkCount = state.AddPrepareOk(opNumber, state.Replica); // Add self to prepareOk
        if (prepareOkCount < state.QuorumSize) return true;
        Log.Information(
            "Replica {ReplicaId} (Primary): Op={OpNumber} reached quorum ({Count}/{Needed}) immediately after PREPARE.",
            state.Replica, opNumber, prepareOkCount, state.QuorumSize);
        // This triggers the commit logic usually found in PrepareOkHandler
        await CommitAndReplyIfReadyAsync(opNumber, context);

        return true;
    }

    private async Task CommitAndReplyIfReadyAsync(ulong opNumberToCommit, IReplicaContext context)
    {
        var state = context.State;
        if (!state.IsPrimary) return; // Only primary sends replies

        // Try to commit sequentially starting from the next expected commit
        var currentOp = state.Commit + 1;
        while (currentOp <= opNumberToCommit && state.HasEnoughPrepareOks(currentOp))
        {
            var logEntry = state.GetLogEntry(currentOp);
            var result = state.ExecuteAndCommitOperation(currentOp);
            if (result != null) // Commit successful
            {
                Log.Information("Replica {ReplicaId} (Primary): Successfully committed and executed Op={OpNumber}.",
                    state.Replica, currentOp);

                // Construct Reply message
                var replyHeader = new VsrHeader(
                    parent: 0,
                    client: logEntry.Client,
                    context: BinaryUtils.NewGuidUInt128(), // New context for the reply
                    bodySize: (uint)result.Length,
                    request: logEntry.Request, // Use original request number
                    cluster: state.Cluster,
                    epoch: state.Epoch,
                    view: state.View,
                    op: currentOp, // Op number that generated the reply
                    commit: state.Commit, // Current commit number
                    offset: 0,
                    replica: state.Replica, // Sender is primary
                    command: Command.Reply,
                    operation: logEntry.Operation, // Original operation
                    version: GlobalConfig.CurrentVersion
                );
                var replyMessage = new VsrMessage(replyHeader, result); // Payload is the result

                // Store the reply in the client table *before* trying to send
                state.UpdateClientTable(logEntry.Client, logEntry.Request, replyMessage);

                // Send reply to the original client connection
                if (state.TryRemovePendingClientConnection(currentOp, out var clientConnId))
                {
                    Log.Information(
                        "Replica {ReplicaId} (Primary): Sending REPLY for Op={OpNumber}, Req={RequestNum} to Client Connection {ConnId}",
                        state.Replica, currentOp, logEntry.Request, clientConnId.Id);
                    using var serializedReply = VsrMessageSerializer.SerializeMessage(replyMessage, state.MemoryPool);
                    try
                    {
                        await context.SendAsync(clientConnId, serializedReply).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Log.Warning(ex,
                            "Replica {ReplicaId} (Primary): Failed to send REPLY for Op={OpNumber} to Client Connection {ConnId}. Client might have disconnected.",
                            state.Replica, currentOp, clientConnId.Id);
                        // The reply is stored in the client table, so if the client reconnects and asks again, we can send it.
                    }
                }
                else
                {
                    Log.Warning(
                        "Replica {ReplicaId} (Primary): Committed Op={OpNumber} but could not find pending client connection to send REPLY.",
                        state.Replica, currentOp);
                }
            }
            else
            {
                Log.Error(
                    "Replica {ReplicaId} (Primary): Failed to commit Op={OpNumber} even though quorum was met. Stopping commit sequence.",
                    state.Replica, currentOp);
                break; // Stop trying to commit further ops if one fails
            }

            currentOp++; // Move to the next operation number
        }
    }
}