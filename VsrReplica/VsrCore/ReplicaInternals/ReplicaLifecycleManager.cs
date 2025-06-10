using Serilog;
using VsrReplica.VsrCore.Messages;
using VsrReplica.VsrCore.State;
using VsrReplica.VsrCore.Timers;
using VsrReplica.VsrCore.Utils;

namespace VsrReplica.VsrCore.ReplicaInternals;

public class ReplicaLifecycleManager : IDisposable
{
    private readonly ReplicaState _state;
    private readonly IReplicaTimer _primaryMonitorTimer;
    private readonly IReplicaTimer _primaryIdleCommitTimer;
    private IReplicaContext ReplicaContext { get; set; } = null!;

    private bool _disposed;
    private Task? _recoveryBroadcastTask = null;


    public ReplicaLifecycleManager(
        ReplicaState state,
        IReplicaTimer primaryMonitorTimer,
        IReplicaTimer primaryIdleCommitTimer)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _primaryMonitorTimer = primaryMonitorTimer ?? throw new ArgumentNullException(nameof(primaryMonitorTimer));
        _primaryIdleCommitTimer =
            primaryIdleCommitTimer ?? throw new ArgumentNullException(nameof(primaryIdleCommitTimer));

        Log.Information("Replica {ReplicaId}: Lifecycle Manager initialized.", _state.Replica);
    }

    public void SetReplicaContext(IReplicaContext replicaContext)
    {
        ReplicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        UpdateTimerStates();
    }

    public async Task InitiateRecoveryProtocolAsync()
    {
        // Prevent multiple concurrent recovery attempts (optional but good practice)
        if (_state.Status == ReplicaStatus.Recovering && _recoveryBroadcastTask != null &&
            !_recoveryBroadcastTask.IsCompleted)
        {
            Log.Warning("Replica {ReplicaId}: Recovery already in progress, ignoring initiation request.",
                _state.Replica);
            return;
        }

        Log.Warning("Replica {ReplicaId}: Initiating recovery protocol.", _state.Replica);

        var nonce = BinaryUtils.NewGuidUInt128();
        _state.StartRecovery(nonce);
        UpdateTimerStates(); // Stop normal timers (call own method)

        var recoveryPayload = new Recovery(nonce);
        var payloadSize = Recovery.CalculateSerializedSize(recoveryPayload);

        var recoveryHeader = new VsrHeader(
            parent: 0, client: 0, context: 0,
            bodySize: (uint)payloadSize,
            request: 0, cluster: _state.Cluster, epoch: _state.Epoch,
            view: _state.View,
            op: _state.Op,
            commit: _state.Commit,
            offset: 0,
            replica: _state.Replica,
            command: Command.Recovery,
            operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
        );

        // Rent buffer and serialize payload
        var payloadBytes = new byte[payloadSize];

        Recovery.Serialize(recoveryPayload, payloadBytes.AsSpan());
        var payloadMemory = payloadBytes.AsMemory();

        var recoveryMessage = new VsrMessage(recoveryHeader, payloadMemory);

        Log.Information("Replica {ReplicaId}: Broadcasting RECOVERY message with Nonce {Nonce}.", _state.Replica,
            nonce);
        using var serializedMsg = VsrMessageSerializer.SerializeMessage(recoveryMessage, _state.MemoryPool);
        try
        {
            _recoveryBroadcastTask = ReplicaContext.BroadcastAsync(serializedMsg);
            await _recoveryBroadcastTask;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to broadcast RECOVERY message.", _state.Replica);
            _recoveryBroadcastTask = null; // Reset task reference
            _state.SetStatusNormal(_state.View, _state.Op, _state.Commit); // Revert status
            UpdateTimerStates(); // Restart timers
        }
    }

    public void UpdateTimerStates()
    {
        if (_disposed) return;

        // Log.Debug("Replica {ReplicaId}: Updating timer states. IsPrimary={IsPrimary}, Status={Status}",
        //     _state.Replica, _state.IsPrimary, _state.Status);

        // Stop both timers by default
        _primaryMonitorTimer.Stop();
        _primaryIdleCommitTimer.Stop();

        switch (_state.Status)
        {
            case ReplicaStatus.Normal:
                if (_state.IsPrimary)
                {
                    _primaryMonitorTimer.Stop();
                    _primaryIdleCommitTimer.Start(GlobalConfig.PrimaryIdleCommitInterval);
                    Log.Debug("Replica {ReplicaId}: Configured as PRIMARY. Started IdleCommitTimer.", _state.Replica);
                }
                else
                {
                    _primaryIdleCommitTimer.Stop();
                    _primaryMonitorTimer.Start(GlobalConfig.BackupPrimaryTimeoutDuration);
                    var primaryConn = ReplicaContext.GetConnectionIdForReplica(_state.PrimaryReplica);
                    if (!primaryConn.HasValue)
                    {
                        Log.Warning(
                            "Replica {ReplicaId}: Configured as BACKUP. Primary {PrimaryId} is not currently connected. PrimaryMonitorTimer is active and will trigger a view change if no Prepare/Commit messages are received.",
                            _state.Replica, _state.PrimaryReplica);
                    }
                }

                break;

            case ReplicaStatus.ViewChange:
            case ReplicaStatus.Recovering:
                Log.Debug("Replica {ReplicaId}: Status is {Status}. Stopped both timers.", _state.Replica,
                    _state.Status);
                break;

            default:
                Log.Warning("Replica {ReplicaId}: Unknown status {Status} in UpdateTimerStates.", _state.Replica,
                    _state.Status);
                break;
        }
    }

    public void NotifyActivity(byte messageSourceReplicaId, Command command)
    {
        if (_disposed || _state.Status != ReplicaStatus.Normal) return;

        if (_state.IsPrimary)
        {
            Log.Verbose(
                "Replica {ReplicaId} (Primary): Activity detected (Source: {SourceId}, Cmd: {Command}). Resetting IdleCommitTimer.",
                _state.Replica, messageSourceReplicaId, command);
            _primaryIdleCommitTimer.ActivityDetected();
        }
        else
        {
            if (messageSourceReplicaId != _state.PrimaryReplica ||
                command is not (Command.Prepare or Command.Commit)) return;
            Log.Verbose(
                "Replica {ReplicaId} (Backup): Liveness activity detected from Primary {PrimaryId} (Cmd: {Command}). Resetting MonitorTimer.",
                _state.Replica, messageSourceReplicaId, command);
            _primaryMonitorTimer.ActivityDetected();
        }
    }

    public async Task HandlePrimaryTimeout()
    {
        Log.Warning("Replica {ReplicaId}: Handling PrimaryTimeout event. Current View: {View}, Status: {Status}",
            _state.Replica, _state.View, _state.Status);

        if (_state.Status == ReplicaStatus.ViewChange)
        {
            Log.Information(
                "Replica {ReplicaId}: Ignoring PrimaryTimeout event because a view change is already in progress for View {TargetView}.",
                _state.Replica, _state.StatusViewNumber);
            return;
        }

        if (_state is { Op: 0, Commit: 0 })
        {
            Log.Verbose("Replica {ReplicaId}: Ignoring PrimaryTimeout event (Op=0, Commit=0).", _state.Replica);
            return;
        }

        if (_state.IsPrimary || _state.Status == ReplicaStatus.Recovering)
        {
            Log.Information(
                "Replica {ReplicaId}: Ignoring PrimaryTimeout event (IsPrimary={IsPrimary}, Status={Status}).",
                _state.Replica, _state.IsPrimary, _state.Status);
            return;
        }

        _state.InitiateViewChange();
        _state.AddStartViewChangeVote(_state.StatusViewNumber, _state.Replica);

        var nextView = _state.StatusViewNumber;
        var startViewChangeHeader = new VsrHeader(
            parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(),
            bodySize: 0, request: 0, cluster: _state.Cluster, epoch: _state.Epoch,
            view: nextView,
            op: _state.Op,
            commit: _state.Commit,
            offset: 0,
            replica: _state.Replica,
            command: Command.StartViewChange,
            operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
        );
        var startViewChangeMessage = new VsrMessage(startViewChangeHeader, Memory<byte>.Empty);

        Log.Information("Replica {ReplicaId}: Broadcasting StartViewChange for View {View} due to timeout.",
            _state.Replica, nextView);
        var serializedMsg = VsrMessageSerializer.SerializeMessage(startViewChangeMessage, _state.MemoryPool);
        try
        {
            await ReplicaContext.BroadcastAsync(serializedMsg).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to broadcast StartViewChange for View {View}.", _state.Replica,
                nextView);
        }
        finally
        {
            serializedMsg.Dispose();
        }
    }


    public async Task HandleSendIdleCommit()
    {
        // Log.Debug("Replica {ReplicaId}: Handling SendIdleCommit event. IsPrimary={IsPrimary}, Status={Status}",
        //     _state.Replica, _state.IsPrimary, _state.Status);

        // if (_state is { Op: 0, Commit: 0 })
        // {
        //     Log.Verbose("Replica {ReplicaId}: Ignoring SendIdleCommit event (Op=0, Commit=0).", _state.Replica);
        //     return;
        // }

        if (!_state.IsPrimary || _state.Status != ReplicaStatus.Normal)
        {
            Log.Debug("Replica {ReplicaId}: Ignoring SendIdleCommit event (Not Primary or not Normal).",
                _state.Replica);
            return;
        }

        var commitHeader = new VsrHeader(
            parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(),
            bodySize: 0, request: 0, cluster: _state.Cluster, epoch: _state.Epoch,
            view: _state.View, op: _state.Op, commit: _state.Commit,
            offset: 0, replica: _state.Replica, command: Command.Commit,
            operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
        );
        var commitMessage = new VsrMessage(commitHeader, Memory<byte>.Empty);

        // Log.Debug(
        //     "Replica {ReplicaId} (Primary): Broadcasting idle COMMIT message for View {View}, CommitNum {CommitNum}",
        //     _state.Replica, _state.View, _state.Commit);

        using var serializedMsg = VsrMessageSerializer.SerializeMessage(commitMessage, _state.MemoryPool);
        try
        {
            await ReplicaContext.BroadcastAsync(serializedMsg).ConfigureAwait(false);
            _primaryIdleCommitTimer.ActivityDetected();
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId} (Primary): Failed to broadcast idle COMMIT message.", _state.Replica);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _primaryMonitorTimer?.Dispose();
        _primaryIdleCommitTimer?.Dispose();
        Log.Information("Replica {ReplicaId}: Lifecycle Manager disposed.", _state.Replica);
        GC.SuppressFinalize(this);
    }
}
