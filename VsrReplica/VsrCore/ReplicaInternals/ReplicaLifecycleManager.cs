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
    private readonly IReplicaContext _replicaContext;

    private bool _disposed;


    public ReplicaLifecycleManager(
        ReplicaState state,
        IReplicaTimer primaryMonitorTimer,
        IReplicaTimer primaryIdleCommitTimer,
        IReplicaContext replicaContext)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _primaryMonitorTimer = primaryMonitorTimer ?? throw new ArgumentNullException(nameof(primaryMonitorTimer));
        _primaryIdleCommitTimer =
            primaryIdleCommitTimer ?? throw new ArgumentNullException(nameof(primaryIdleCommitTimer));
        _replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));

        Log.Information("Replica {ReplicaId}: Lifecycle Manager initialized.", _state.Replica);
        UpdateTimerStates();
    }

    public void UpdateTimerStates()
    {
        if (_disposed) return;

        Log.Debug("Replica {ReplicaId}: Updating timer states. IsPrimary={IsPrimary}, Status={Status}",
            _state.Replica, _state.IsPrimary, _state.Status);

        if (_state.Status == ReplicaStatus.Normal)
        {
            if (_state.IsPrimary)
            {
                _primaryMonitorTimer.Stop();
                _primaryIdleCommitTimer.Start(GlobalConfig.PrimaryIdleCommitInterval);
                Log.Debug("Replica {ReplicaId}: Configured as PRIMARY. Started IdleCommitTimer, Stopped MonitorTimer.",
                    _state.Replica);
            }
            else
            {
                _primaryIdleCommitTimer.Stop();
                var primaryConn = _replicaContext.GetConnectionIdForReplica(_state.PrimaryReplica);
                if (primaryConn.HasValue)
                {
                    _primaryMonitorTimer.Start(GlobalConfig.BackupPrimaryTimeoutDuration);
                    Log.Debug(
                        "Replica {ReplicaId}: Configured as BACKUP. Started MonitorTimer (Primary connected), Stopped IdleCommitTimer.",
                        _state.Replica);
                }
                else
                {
                    _primaryMonitorTimer.Stop();
                    Log.Warning(
                        "Replica {ReplicaId}: Configured as BACKUP but Primary {PrimaryId} not connected. MonitorTimer stopped.",
                        _state.Replica, _state.PrimaryReplica);
                }
            }
        }
        else
        {
            _primaryMonitorTimer.Stop();
            _primaryIdleCommitTimer.Stop();
            Log.Debug("Replica {ReplicaId}: Status is {Status}. Stopped both timers.", _state.Replica, _state.Status);
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
            await _replicaContext.BroadcastAsync(serializedMsg).ConfigureAwait(false);
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
        Log.Debug("Replica {ReplicaId}: Handling SendIdleCommit event. IsPrimary={IsPrimary}, Status={Status}",
            _state.Replica, _state.IsPrimary, _state.Status);

        if (!_state.IsPrimary || _state.Status != ReplicaStatus.Normal)
        {
            Log.Debug("Replica {ReplicaId}: Ignoring SendIdleCommit event (Not Primary or not Normal).",
                _state.Replica);
            return;
        }

        var commitHeader = new VsrHeader(
            parent: 0, client: 0, context: BinaryUtils.NewGuidUInt128(),
            bodySize: 0, request: 0, cluster: _state.Cluster, epoch: _state.Epoch,
            view: _state.View,
            op: _state.Op,
            commit: _state.Commit,
            offset: 0,
            replica: _state.Replica,
            command: Command.Commit,
            operation: Operation.Reserved, version: GlobalConfig.CurrentVersion
        );
        var commitMessage = new VsrMessage(commitHeader, Memory<byte>.Empty);

        Log.Debug(
            "Replica {ReplicaId} (Primary): Broadcasting idle COMMIT message for View {View}, CommitNum {CommitNum}",
            _state.Replica, _state.View, _state.Commit);

        using var serializedMsg = VsrMessageSerializer.SerializeMessage(commitMessage, _state.MemoryPool);
        try
        {
            await _replicaContext.BroadcastAsync(serializedMsg).ConfigureAwait(false);
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