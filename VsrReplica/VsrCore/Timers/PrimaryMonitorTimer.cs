using System.Diagnostics;
using Serilog;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.Timers;

public class PrimaryMonitorTimer(byte owningReplicaId, Action<InternalEventPipelineItem> timeoutCallback)
    : IReplicaTimer
{
    private readonly Action<InternalEventPipelineItem> _timeoutCallback =
        timeoutCallback ?? throw new ArgumentNullException(nameof(timeoutCallback)); // Action to enqueue timeout event

    private readonly object _lock = new();
    private Timer? _timer;
    private readonly Stopwatch _stopwatch = new();
    private TimeSpan _timeoutDuration;
    private volatile bool _isRunning;
    private volatile bool _disposed;

    public void Start(TimeSpan duration)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(PrimaryMonitorTimer));
        if (duration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(duration), "Duration must be positive.");

        lock (_lock)
        {
            _timeoutDuration = duration;
            if (!_isRunning)
            {
                Log.Debug("Replica {ReplicaId}: Starting PrimaryMonitorTimer with duration {Duration}.",
                    owningReplicaId, _timeoutDuration);
                _timer = new Timer(OnTimerElapsed, null, _timeoutDuration, Timeout.InfiniteTimeSpan);
                _stopwatch.Restart();
                _isRunning = true;
            }
            else
            {
                Log.Debug("Replica {ReplicaId}: Resetting (re-starting) PrimaryMonitorTimer with duration {Duration}.",
                    owningReplicaId, _timeoutDuration);
                _timer?.Change(_timeoutDuration, Timeout.InfiniteTimeSpan);
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

            _timer?.Change(_timeoutDuration, Timeout.InfiniteTimeSpan);
            _stopwatch.Restart();
        }
    }

    private void StopInternal()
    {
        // Must be called within _lock
        if (!_isRunning) return;

        Log.Debug("Replica {ReplicaId}: Stopping PrimaryMonitorTimer.", owningReplicaId);
        _timer?.Dispose();
        _timer = null;
        _stopwatch.Stop();
        _isRunning = false;
    }

    private void OnTimerElapsed(object? state)
    {
        InternalEventPipelineItem? timeoutEvent = null;
        lock (_lock)
        {
            if (!_isRunning || _disposed) return;
            var elapsed = _stopwatch.Elapsed;
            if (elapsed >= _timeoutDuration - TimeSpan.FromMilliseconds(50))
            {
                Log.Warning(
                    "Replica {ReplicaId}: PrimaryMonitorTimer timeout detected after {Elapsed} (Threshold: {Threshold}). Enqueuing PrimaryTimeout event.",
                    owningReplicaId, elapsed, _timeoutDuration);
                StopInternal();
                timeoutEvent = new InternalEventPipelineItem(PipelineMessageType.PrimaryTimeout);
            }
            else
            {
                var remaining = _timeoutDuration - elapsed;
                Log.Debug(
                    "Replica {ReplicaId}: PrimaryMonitorTimer fired early? Rescheduling for remaining {Remaining}.",
                    owningReplicaId, remaining);
                _timer?.Change(remaining, Timeout.InfiniteTimeSpan);
            }
        }

        if (timeoutEvent == null) return;
        try
        {
            _timeoutCallback(timeoutEvent);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Replica {ReplicaId}: Failed to enqueue PrimaryTimeout event via callback.",
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
        Log.Debug("Replica {ReplicaId}: PrimaryMonitorTimer disposed.", owningReplicaId);
    }
}