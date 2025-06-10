using System.Diagnostics;
using Serilog;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.Timers;

public class PrimaryIdleCommitTimer(byte owningReplicaId, Action<InternalEventPipelineItem> idleCallback)
    : IReplicaTimer
{
    private readonly Action<InternalEventPipelineItem> _idleCallback =
        idleCallback ?? throw new ArgumentNullException(nameof(idleCallback));

    private readonly object _lock = new();
    private Timer? _timer;
    private readonly Stopwatch _stopwatch = new();
    private TimeSpan _idleInterval;
    private volatile bool _isRunning;
    private volatile bool _disposed;


    public void Start(TimeSpan duration)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(PrimaryIdleCommitTimer));
        if (duration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(duration), "Interval must be positive.");

        lock (_lock)
        {
            _idleInterval = duration;
            if (!_isRunning)
            {
                Log.Debug("Replica {ReplicaId}: Starting PrimaryIdleCommitTimer with interval {Interval}.",
                    owningReplicaId, _idleInterval);
                _timer = new Timer(OnTimerElapsed, null, _idleInterval, _idleInterval);
                _stopwatch.Restart();
                _isRunning = true;
            }
            else
            {
                Log.Debug(
                    "Replica {ReplicaId}: Resetting (re-starting) PrimaryIdleCommitTimer with interval {Interval}.",
                    owningReplicaId, _idleInterval);
                _timer?.Change(_idleInterval, _idleInterval); // Update period
                _stopwatch.Restart();
            }
        }
    }

    public void Stop()
    {
        if (_disposed) return;
        lock (_lock)
        {
            StopInternal();
        }
    }

    public void ActivityDetected()
    {
        if (_disposed || !_isRunning) return;

        lock (_lock)
        {
            if (!_isRunning) return;
            _stopwatch.Restart();
        }
    }

    private void StopInternal()
    {
        // Must be called within _lock
        if (!_isRunning) return;

        Log.Debug("Replica {ReplicaId}: Stopping PrimaryIdleCommitTimer.", owningReplicaId);
        _timer?.Dispose();
        _timer = null;
        _stopwatch.Stop();
        _isRunning = false;
    }

    private void OnTimerElapsed(object? state)
    {
        InternalEventPipelineItem? idleEvent = null;
        lock (_lock)
        {
            if (!_isRunning || _disposed) return;
            var elapsedSinceActivity = _stopwatch.Elapsed;
            if (elapsedSinceActivity >= _idleInterval)
            {
                // Log.Debug(
                //     "Replica {ReplicaId}: PrimaryIdleCommitTimer interval elapsed ({ElapsedSinceActivity} >= {Interval}). Enqueuing SendIdleCommit event.",
                //     owningReplicaId, elapsedSinceActivity, _idleInterval);
                _stopwatch.Restart();
                idleEvent = new InternalEventPipelineItem(PipelineMessageType.SendIdleCommit);
            }
        }
        if (idleEvent == null) return;
        try
        {
            _idleCallback(idleEvent);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to enqueue SendIdleCommit event via callback.",
                owningReplicaId);
        }
    }


    public void Dispose()
    {
        if (_disposed) return;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            StopInternal();
            _stopwatch.Stop();
        }

        GC.SuppressFinalize(this);
        Log.Debug("Replica {ReplicaId}: PrimaryIdleCommitTimer disposed.", owningReplicaId);
    }
}