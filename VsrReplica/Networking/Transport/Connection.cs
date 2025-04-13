using System.IO.Pipelines;
using System.Net.Sockets;
using System.Buffers;
using Serilog;
using VsrReplica.Networking.Interfaces;

namespace VsrReplica.Networking.Transport;

public class Connection : IAsyncDisposable
{
    private const int MinBuffSize = 512;
    private readonly ISocket _socket;
    private readonly IReceiver _receiver;
    private ISender? _sender;
    private readonly ISenderPool _senderPool;
    private Task? _receiveTask;
    private Task? _sendTask;
    private readonly Pipe _transportPipe;
    private readonly Pipe _applicationPipe;
    private readonly object _shutdownLock = new object();
    private volatile bool _socketDisposed;
    public PipeWriter Output { get; }
    public PipeReader Input { get; }

    public event Action? OnClosed;
    public event Action<Exception>? OnError;
    public int Id => GetHashCode();

    public bool IsClosed => _socketDisposed;

    public Connection(ISocket socket, ISenderPool senderPool, IReceiver receiver,
        PipeScheduler transportScheduler, PipeScheduler applicationScheduler,
        MemoryPool<byte> memoryPool)
    {
        _socket = socket;
        _receiver = receiver;
        _senderPool = senderPool;
        _transportPipe = new Pipe(new PipeOptions(memoryPool,
            applicationScheduler, transportScheduler,
            useSynchronizationContext: false));
        Output = _transportPipe.Writer;
        _applicationPipe = new Pipe(new PipeOptions(memoryPool,
            transportScheduler, applicationScheduler,
            useSynchronizationContext: false));
        Input = _applicationPipe.Reader;
    }


    public void Start()
    {
        try
        {
            _sendTask = SendLoop();
            _receiveTask = ReceiveLoop();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    private async Task SendLoop()
    {
        try
        {
            while (true)
            {
                var result = await _transportPipe.Reader.ReadAsync();
                var buff = result.Buffer;
                if (!buff.IsEmpty)
                {
                    _sender = _senderPool.Rent();
                    await _sender.SendAsync(_socket, result.Buffer);
                    _senderPool.Return(_sender);
                    _sender = null;
                }

                _transportPipe.Reader.AdvanceTo(buff.End);
                if (result.IsCompleted || result.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (Exception e)
        {
            OnError?.Invoke(e);
            Log.Error(e, "Error sending data");
            throw;
        }
        finally
        {
            await _applicationPipe.Writer.CompleteAsync();
            Shutdown();
            OnClosed?.Invoke();
        }
    }

    private async Task ReceiveLoop()
    {
        try
        {
            while (true)
            {
                var buff = _applicationPipe.Writer.GetMemory(MinBuffSize);
                var bytes = await _receiver.ReceiveAsync(_socket, buff);
                if (bytes == 0)
                {
                    break;
                }

                _applicationPipe.Writer.Advance(bytes);
                var result = await _applicationPipe.Writer.FlushAsync();
                if (result.IsCanceled || result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception e)
        {
            OnError?.Invoke(e);
            Log.Error(e, "Error receiving data");
            throw;
        }
        finally
        {
            await _applicationPipe.Writer.CompleteAsync();
            Shutdown();
            OnClosed?.Invoke();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _transportPipe.Reader.CompleteAsync();
        await _applicationPipe.Writer.CompleteAsync();
        try
        {
            if (_receiveTask != null)
            {
                await _receiveTask;
            }

            if (_sendTask != null)
            {
                await _sendTask;
            }
        }
        finally
        {
            _receiver.Dispose();
            _sender?.Dispose();
        }
    }

    public void Shutdown()
    {
        lock (_shutdownLock)
        {
            if (_socketDisposed)
            {
                return;
            }

            _socketDisposed = true;
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception e)
            {
                OnError?.Invoke(e);
                Log.Error(e, "Error shutting down socket");
                throw;
            }
            finally
            {
                _socket.Dispose();
            }
        }
    }
}