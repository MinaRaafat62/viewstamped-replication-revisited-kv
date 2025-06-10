using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;

namespace VsrReplica.VsrCore.Handlers;

public class RecoveryHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        var recoveringReplicaId = header.Replica;

        Log.Debug(
            "Replica {ReplicaId}: Rcvd RECOVERY from Replica {RecoveringId}. MyStatus={MyStatus}, MyView={MyView}",
            state.Replica, recoveringReplicaId, state.Status, state.View);
        if (state.Status != ReplicaStatus.Normal)
        {
            Log.Information("Replica {ReplicaId}: Ignoring RECOVERY from {RecoveringId} because status is {MyStatus}.",
                state.Replica, recoveringReplicaId, state.Status);
            return false; // Don't process if not Normal
        }

        Recovery recoveryPayload;
        try
        {
            recoveryPayload = Recovery.Deserialize(message.Payload.Span, out _);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to deserialize RECOVERY payload from {RecoveringId}.",
                state.Replica, recoveringReplicaId);
            return false;
        }

        var nonce = recoveryPayload.Nonce;
        var fullLog = state.GetAllLogEntries();
        var responsePayload = new RecoveryResponse(nonce, state.IsPrimary, fullLog);
        var payloadSize = RecoveryResponse.CalculateSerializedSize(responsePayload);
        var payloadBytes = new byte[payloadSize];
        RecoveryResponse.Serialize(responsePayload, payloadBytes.AsSpan());
        var responsePayloadMemory = payloadBytes.AsMemory();
        var responseHeader = new VsrHeader(
            parent: 0, client: 0, context: message.Header.Context,
            bodySize: (uint)payloadSize,
            request: 0,
            epoch: context.State.Epoch, cluster: context.State.Cluster,
            view: context.State.View, op: context.State.Op, commit: context.State.Commit,
            command: Command.RecoveryResponse, operation: Operation.Reserved,
            offset: 0, replica: state.Replica, version: GlobalConfig.CurrentVersion);
        var responseMessage = new VsrMessage(responseHeader, responsePayloadMemory);
        var recoveringReplicaConnId = context.GetConnectionIdForReplica(recoveringReplicaId);
        if (recoveringReplicaConnId.HasValue)
        {
            Log.Information(
                "Replica {ReplicaId}: Sending RecoveryResponse V:{View} Op:{Op} C:{Commit} Nonce:{Nonce} IsPrimary:{IsPrimary} LogEntries:{LogCount} to Replica {RecoveringId}",
                state.Replica, state.View, state.Op, state.Commit, nonce, state.IsPrimary, fullLog.Count,
                recoveringReplicaId);
            using var serializedResponse = VsrMessageSerializer.SerializeMessage(responseMessage, state.MemoryPool);
            try
            {
                await context.SendAsync(recoveringReplicaConnId.Value, serializedResponse).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Replica {ReplicaId}: Failed to send RecoveryResponse to Replica {RecoveringId}.",
                    state.Replica, recoveringReplicaId);
            }
        }
        else
        {
            Log.Warning(
                "Replica {ReplicaId}: Cannot send RecoveryResponse to Replica {RecoveringId}, connection not found.",
                state.Replica, recoveringReplicaId);
        }
        return true;
    }
}