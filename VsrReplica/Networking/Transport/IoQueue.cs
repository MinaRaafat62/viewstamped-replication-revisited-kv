using System.Collections.Concurrent;
using System.IO.Pipelines;
using Serilog;

namespace VsrReplica.Networking.Transport;

public class IoQueue : PipeScheduler, IThreadPoolWorkItem
{
    private readonly ConcurrentQueue<Work> _workItems = new ConcurrentQueue<Work>();
    private int _doingWork;

    public override void Schedule(Action<object?> action, object? state)
    {
        _workItems.Enqueue(new Work(action, state));

        // Set working if it wasn't (via atomic Interlocked).
        if (Interlocked.CompareExchange(ref _doingWork, 1, 0) == 0)
        {
            // Wasn't working, schedule.
            System.Threading.ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
        }
    }

    void IThreadPoolWorkItem.Execute()
    {
        while (true)
        {
            while (_workItems.TryDequeue(out var item))
            {
                try
                {
                    item.Callback(item.State);
                }
                catch (Exception e)
                {
                    Log.Error(e, "Error executing work item in IoQueue.");
                }
            }

            // All work done.

            // Set _doingWork (0 == false) prior to checking IsEmpty to catch any missed work in interim.
            // This doesn't need to be volatile due to the following barrier (i.e. it is volatile).
            _doingWork = 0;

            // Ensure _doingWork is written before IsEmpty is read.
            // As they are two different memory locations, we insert a barrier to guarantee ordering.
            Thread.MemoryBarrier();

            // Check if there is work to do
            if (_workItems.IsEmpty)
            {
                // Nothing to do, exit.
                break;
            }

            // Is work, can we set it as active again (via atomic Interlocked), prior to scheduling?
            if (Interlocked.Exchange(ref _doingWork, 1) == 1)
            {
                // Execute has been rescheduled already, exit.
                break;
            }

            // Is work, wasn't already scheduled so continue loop.
        }
    }

    private readonly struct Work(Action<object?> callback, object? state)
    {
        public readonly Action<object?> Callback = callback;
        public readonly object? State = state;
    }
}