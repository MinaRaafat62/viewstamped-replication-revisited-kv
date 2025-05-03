using Serilog;
using VsrReplica.Networking;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.ReplicaInternals;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.Handlers;

public class StartViewChangeHandler : IVsrCommandHandler
{
    public async Task<bool> HandleCommandAsync(VsrMessage message, ConnectionId? connectionId, IReplicaContext context)
    {
        var state = context.State;
        var header = message.Header;
        var senderId = header.Replica;
        var receivedView = header.View; // This is the target view number

        Log.Debug(
            "Replica {ReplicaId}: Rcvd StartViewChange V:{View} from Replica {SenderId}. MyV:{MyView}, Status:{Status}, TargetV:{TargetV}",
            state.Replica, receivedView, senderId, state.View, state.Status, state.StatusViewNumber);

        // --- Basic Validations ---

        // Ignore messages for views older than our current view, unless we are already in a view change for that older view (unlikely but possible during recovery/flaps)
        if (receivedView < state.View &&
            (state.Status != ReplicaStatus.ViewChange || receivedView < state.StatusViewNumber))
        {
            Log.Information(
                "Replica {ReplicaId}: Ignoring StartViewChange for past V:{View} (Current:{MyView}, Target:{TargetV}).",
                state.Replica, receivedView, state.View, state.StatusViewNumber);
            return false;
        }

        // If we are Normal and receive SVC for a future view, initiate our own view change.
        if (receivedView > state.View && state.Status == ReplicaStatus.Normal)
        {
            Log.Information(
                "Replica {ReplicaId}: Received StartViewChange for future V:{View} while Normal. Initiating View Change to V:{NextView}.",
                state.Replica, receivedView, receivedView); // Target the received future view

            // Repeatedly initiate until our target view matches or exceeds the received view
            while (state.StatusViewNumber < receivedView || state.Status != ReplicaStatus.ViewChange)
            {
                state.InitiateViewChange(); // This increments View and sets StatusViewNumber
                if (state.StatusViewNumber > receivedView + 5) // Safety break for runaway view changes
                {
                    Log.Error("Replica {ReplicaId}: View change initiation loop detected. Aborting.", state.Replica);
                    // Consider entering a recovery state here
                    return false;
                }
            }

            // Add own vote for the view we just entered
            state.AddStartViewChangeVote(state.StatusViewNumber, state.Replica);

            // Broadcast our own StartViewChange message for the view we are now targeting
            var selfStartViewChangeHeader = new VsrHeader(
                parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(), // New context
                bodySize: 0, request: 0, cluster: state.Cluster, epoch: state.Epoch,
                view: state.StatusViewNumber, // The view we are now targeting
                op: state.Op,
                commit: state.Commit,
                offset: 0,
                replica: state.Replica,
                command: Command.StartViewChange,
                operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
            );
            var selfStartViewChangeMessage = new VsrMessage(selfStartViewChangeHeader, Memory<byte>.Empty);
            using var serializedSelfSvc =
                VsrMessageSerializer.SerializeMessage(selfStartViewChangeMessage, state.MemoryPool);
            try
            {
                Log.Information(
                    "Replica {ReplicaId}: Broadcasting StartViewChange for V:{View} after receiving future SVC.",
                    state.Replica, state.StatusViewNumber);
                await context.BroadcastAsync(serializedSelfSvc).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Replica {ReplicaId}: Failed to broadcast self-StartViewChange for V:{View}.",
                    state.Replica, state.StatusViewNumber);
            }
            // serializedSelfSvc disposed automatically by using
        }

        // Ignore messages that don't match the view we are currently trying to change to.
        if (state.Status != ReplicaStatus.ViewChange || receivedView != state.StatusViewNumber)
        {
            Log.Information(
                "Replica {ReplicaId}: Ignoring StartViewChange for V:{View}. Current state is Status:{Status}, TargetV:{TargetV}.",
                state.Replica, receivedView, state.Status, state.StatusViewNumber);
            return false;
        }

        // --- Process Vote ---
        var quorumReached = state.AddStartViewChangeVote(state.StatusViewNumber, senderId);

        if (!quorumReached)
        {
            Log.Debug(
                "Replica {ReplicaId}: Added SVC vote for V:{View} from {SenderId}. Count:{Count}/{Needed}. Waiting for quorum.",
                state.Replica, state.StatusViewNumber, senderId,
                state.GetStartViewChangeVoteCount(state.StatusViewNumber), state.QuorumSize);
            return true; // Vote added, but no quorum yet
        }

        // --- Quorum Reached for StartViewChange ---
        Log.Information(
            "Replica {ReplicaId}: Quorum reached for StartViewChange for V:{View}. Preparing for DoViewChange phase.",
            state.Replica, state.StatusViewNumber);

        var targetView = state.StatusViewNumber; // The view we have quorum for
        var newPrimaryId = (byte)(targetView % state.TotalReplicas);

        // Construct the DoViewChange data representing *this* replica's state.
        // This is needed whether we are the primary or a backup.
        var logSuffix = state.GetLogSuffix(state.Commit);
        var selfDvcData = new ReplicaState.DoViewChangeReceivedData(
            state.Replica,
            state.LastNormalView, // Our last stable view
            state.Op, // Our current op number
            state.Commit, // Our current commit number
            logSuffix // Our log suffix since our commit number
        );

        if (state.Replica == newPrimaryId)
        {
            // --- I AM THE NEW PRIMARY for the targetView ---
            Log.Information(
                "Replica {ReplicaId} (New Primary): Reached SVC quorum for V:{View}. Adding own state to DVC data.",
                state.Replica, targetView);

            // Add own data directly to the state for DVC processing.
            // This might immediately trigger DVC quorum if f=0 or other messages arrived quickly.
            bool dvcQuorumReached = state.AddDoViewChangeData(targetView, selfDvcData);

            if (dvcQuorumReached)
            {
                Log.Information(
                    "Replica {ReplicaId} (New Primary): DVC Quorum potentially reached immediately after adding own data for V:{View}. DoViewChangeHandler will process.",
                    state.Replica, targetView);
                // The DoViewChangeHandler logic will be triggered by the message processing loop
                // when it processes the DVC messages (including the one we just added).
            }
            else
            {
                Log.Debug(
                    "Replica {ReplicaId} (New Primary): Added own DVC data for V:{View}. Waiting for DVC messages from backups.",
                    state.Replica, targetView);
            }
        }
        else
        {
            // --- I AM A BACKUP for the targetView ---
            Log.Debug("Replica {ReplicaId} (Backup): Entered backup logic for DVC V:{View}.", state.Replica,
                targetView); // ADD THIS
            var primaryConnectionId = context.GetConnectionIdForReplica(newPrimaryId);
            Log.Debug("Replica {ReplicaId} (Backup): GetConnectionIdForReplica({PrimaryId}) returned {ConnId}",
                state.Replica, newPrimaryId, primaryConnectionId?.Id ?? -1); // ADD THIS

            if (primaryConnectionId.HasValue)
            {
                Log.Information( // Keep this
                    "Replica {ReplicaId} (Backup): Reached SVC quorum for V:{View}. Sending DoViewChange to new Primary {PrimaryId}.",
                    state.Replica, targetView, newPrimaryId);

                // We need to serialize the payload containing our state (LastNormalView and LogSuffix)
                var doViewChangePayload = new DoViewChange(selfDvcData.OldView, selfDvcData.LogSuffix);
                Log.Debug("Replica {ReplicaId} (Backup): DVC Payload created: OldView={OldView}, SuffixCount={Count}",
                    state.Replica, doViewChangePayload.LatestView, doViewChangePayload.LogSuffix.Count); // ADD THIS

                var payloadSize = DoViewChange.CalculateSerializedSize(doViewChangePayload);
                var tempPayloadBytes = new byte[payloadSize];

                try
                {
                    DoViewChange.Serialize(doViewChangePayload, tempPayloadBytes.AsSpan());
                    var payloadMemory = tempPayloadBytes.AsMemory();
                    Log.Debug("Replica {ReplicaId} (Backup): DVC Payload serialized ({Size} bytes).", state.Replica,
                        payloadSize); // ADD THIS

                    var doViewChangeHeader = new VsrHeader(
                        parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(),
                        bodySize: (uint)payloadSize,
                        request: 0, cluster: state.Cluster, epoch: state.Epoch,
                        view: targetView,
                        op: selfDvcData.Op,
                        commit: selfDvcData.Commit,
                        offset: 0,
                        replica: state.Replica,
                        command: Command.DoViewChange,
                        operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
                    );
                    Log.Debug("Replica {ReplicaId} (Backup): DVC Header created: {Header}", state.Replica,
                        doViewChangeHeader); // ADD THIS

                    var doViewChangeMessage = new VsrMessage(doViewChangeHeader, payloadMemory);

                    using var serializedDvc =
                        VsrMessageSerializer.SerializeMessage(doViewChangeMessage, state.MemoryPool);
                    Log.Debug(
                        "Replica {ReplicaId} (Backup): Full DVC message serialized ({Size} bytes). Attempting send...",
                        state.Replica, serializedDvc.Memory.Length); // ADD THIS

                    await context.SendAsync(primaryConnectionId.Value, serializedDvc).ConfigureAwait(false);

                    Log.Information( // Keep this
                        "Replica {ReplicaId}: Sent DoViewChange for View {View} to new Primary {PrimaryId} ({ConnId})",
                        state.Replica, targetView, newPrimaryId, primaryConnectionId.Value.Id);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, // Keep this
                        "Replica {ReplicaId}: Failed to send DoViewChange for View {View} to Primary {PrimaryId}.",
                        state.Replica, targetView, newPrimaryId);
                }
            }
            else
            {
                Log.Warning( // Keep this
                    "Replica {ReplicaId} (Backup): Reached SVC quorum for View {View}, but new Primary {PrimaryId} is not connected. Cannot send DoViewChange.",
                    state.Replica, targetView, newPrimaryId);
            }
        }

        // We successfully processed the SVC message and handled the quorum condition.
        return true;
    }
}