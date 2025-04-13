using System.Collections.Concurrent;
using VsrReplica.Networking.Transport;

namespace VsrTests.VsrReplicaTests.NetworkingTests.Transport;

public class IoQueueTests
{
    private readonly IoQueue _ioQueue = new();

    private static bool WaitWithTimeout(WaitHandle handle, int timeoutMilliseconds = 2000) // Increased timeout slightly
    {
        return handle.WaitOne(timeoutMilliseconds);
    }

    private static bool
        WaitWithTimeout(CountdownEvent handle, int timeoutMilliseconds = 2000) // Increased timeout slightly
    {
        return handle.Wait(timeoutMilliseconds);
    }

    [Fact]
    public void Schedule_SingleItem_ExecutesAction()
    {
        // Arrange
        var executed = new ManualResetEventSlim(false);

        // Act
        _ioQueue.Schedule(Action, null);

        // Assert
        Assert.True(WaitWithTimeout(executed.WaitHandle), "Action was not executed within the timeout.");
        return;

        void Action(object? state) => executed.Set();
    }

    [Fact]
    public void Schedule_MultipleItems_ExecutesAllActions()
    {
        // Arrange
        const int itemCount = 10;
        var countdown = new CountdownEvent(itemCount);
        Action<object?> action = _ => countdown.Signal();

        // Act
        for (int i = 0; i < itemCount; i++)
        {
            _ioQueue.Schedule(action, null);
        }

        // Assert
        Assert.True(WaitWithTimeout(countdown), $"Not all {itemCount} actions executed within the timeout.");
    }

    [Fact]
    public void Schedule_PassesStateCorrectly()
    {
        // Arrange
        var stateReceived = new ManualResetEventSlim(false);
        var expectedState = new object(); // Use a unique object instance
        object? actualState = null;

        Action<object?> action = (state) =>
        {
            actualState = state;
            stateReceived.Set();
        };

        // Act
        _ioQueue.Schedule(action, expectedState);

        // Assert
        Assert.True(WaitWithTimeout(stateReceived.WaitHandle), "Action was not executed within the timeout.");
        Assert.Same(expectedState, actualState); // Verify the exact object instance was passed
    }

    [Fact]
    public void Schedule_PassesNullStateCorrectly()
    {
        // Arrange
        var stateReceived = new ManualResetEventSlim(false);
        bool stateWasNull = false;

        Action<object?> action = (state) =>
        {
            stateWasNull = state == null;
            stateReceived.Set();
        };

        // Act
        _ioQueue.Schedule(action, null); // Explicitly pass null

        // Assert
        Assert.True(WaitWithTimeout(stateReceived.WaitHandle), "Action was not executed within the timeout.");
        Assert.True(stateWasNull, "State passed to action was not null.");
    }

    [Fact]
    public async Task Schedule_Concurrently_ExecutesAllActions()
    {
        // Arrange
        const int threadCount = 4;
        const int itemsPerThread = 25;
        const int totalItems = threadCount * itemsPerThread;
        var countdown = new CountdownEvent(totalItems);
        var results = new ConcurrentBag<int>(); // To verify execution

        Action<object?> action = (state) =>
        {
            results.Add((int)state!); // Add item index to bag
            countdown.Signal();
            // Simulate some potential work/delay
            Thread.SpinWait(100); // Small spin to increase chance of contention
        };

        // Act
        var tasks = new Task[threadCount];
        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t; // Capture loop variable
            tasks[t] = Task.Run(() =>
            {
                for (int i = 0; i < itemsPerThread; i++)
                {
                    int itemIndex = threadId * itemsPerThread + i;
                    _ioQueue.Schedule(action, itemIndex);
                }
            });
        }

        await Task.WhenAll(tasks); // Ensure all scheduling calls are done

        // Assert
        Assert.True(WaitWithTimeout(countdown, 5000),
            $"Not all {totalItems} actions executed within the timeout (concurrent)."); // Longer timeout for concurrency
        Assert.Equal(totalItems, results.Count);
        // We can't guarantee the order, but we know all should have executed.
    }

    [Fact]
    public void Schedule_WhileExecuting_ExecutesNewItems()
    {
        // Arrange
        var firstItemStarted = new ManualResetEventSlim(false);
        var firstItemFinished = new ManualResetEventSlim(false);
        var secondItemFinished = new ManualResetEventSlim(false);
        var thirdItemFinished = new ManualResetEventSlim(false);


        // Act
        // 1. Schedule the first long-running item
        _ioQueue.Schedule(FirstAction, "first");

        // 2. Wait until the first item *starts* executing
        Assert.True(WaitWithTimeout(firstItemStarted.WaitHandle), "First item did not start executing.");

        // 3. While the first item is (presumably) still running, schedule more items
        _ioQueue.Schedule(SecondAction, "second");
        _ioQueue.Schedule(ThirdAction, "third");

        // 4. Allow the first item to finish
        firstItemFinished.Set();

        // Assert
        // Check that the items scheduled *while* the queue was active were also processed
        Assert.True(WaitWithTimeout(secondItemFinished.WaitHandle), "Second item did not execute.");
        Assert.True(WaitWithTimeout(thirdItemFinished.WaitHandle), "Third item did not execute.");
        return;

        void ThirdAction(object? state)
        {
            thirdItemFinished.Set();
        }

        void SecondAction(object? state)
        {
            secondItemFinished.Set();
        }

        void FirstAction(object? state)
        {
            firstItemStarted.Set();
            // Simulate work - wait until signaled to finish
            WaitWithTimeout(firstItemFinished.WaitHandle, 1000);
        }
    }


    [Fact]
    public void Schedule_ActionSchedulesMoreWork_ExecutesAll()
    {
        // Arrange
        const int initialItems = 1;
        const int subsequentItems = 5;
        var totalItems = initialItems + subsequentItems;
        var countdown = new CountdownEvent(totalItems);

        Action<object?> subsequentAction = _ => countdown.Signal();

        // Act
        _ioQueue.Schedule(InitialAction, null);

        // Assert
        Assert.True(WaitWithTimeout(countdown, 3000),
            $"Not all {totalItems} actions executed within the timeout (reentrant).");
        return;

        void InitialAction(object? state)
        {
            // Schedule more work from within this action
            for (var i = 0; i < subsequentItems; i++)
            {
                _ioQueue.Schedule(subsequentAction, i);
            }

            countdown.Signal(); // Signal completion of the initial action
        }
    }

    [Fact]
    public void Schedule_ActionThrowsException_DoesNotPreventFutureScheduling() // Or name based on desired behavior
    {
        // Arrange
        var item1Executed = new ManualResetEventSlim(false);
        var item3Executed = new ManualResetEventSlim(false);

        // Act
        _ioQueue.Schedule(Action1Throws, null);
        _ioQueue.Schedule(Action2, null); // Schedule another item immediately after the one that throws

        // Wait for the first item to execute (and throw)
        Assert.True(WaitWithTimeout(item1Executed.WaitHandle), "Action 1 did not start.");

        // Give the ThreadPool some time to potentially handle the exception fallout.
        // The thread processing the queue likely died.
        Thread.Sleep(100); // Small delay, might need adjustment

        // Now, schedule a new item. This *should* trigger a new ThreadPool work item.
        _ioQueue.Schedule(Action3, null);

        // Assert
        // Verify that scheduling *new* work after the exception still works.
        Assert.True(WaitWithTimeout(item3Executed.WaitHandle), "Action 3 did not execute after exception.");
        return;

        void Action3(object? state)
        {
            item3Executed.Set();
        }

        void Action2(object? state)
        {
            // This action might or might not run depending on timing and ThreadPool behavior
            // after the exception in the previous action. It's hard to reliably test
            // if it runs *immediately* after the exception without modifying IoQueue.
        }

        // You could also add assertions about action2 *not* running if that's the expected behavior.
        // If you modify IoQueue to catch exceptions and continue, you'd assert action2 *does* run.
        void Action1Throws(object? state)
        {
            item1Executed.Set();
            throw new InvalidOperationException("Test Exception");
        }
    }
}