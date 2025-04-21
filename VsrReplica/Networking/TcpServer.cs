using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Serilog;
using VsrReplica.Networking.MemoryPool;
using VsrReplica.Networking.Transport;

namespace VsrReplica.Networking;

public class TcpServer : IAsyncDisposable
{
    private readonly Socket _listenSocket;
    private readonly IoQueue _transportScheduler;
    private readonly PipeScheduler _applicationScheduler;
    private readonly SenderPool _senderPool;
    private readonly PinnedBlockMemoryPool _memoryPool;
    private readonly CancellationTokenSource _cts;
    private Task _acceptTask = null!;
    private readonly Action<Connection> _connectionCallback;

    public TcpServer(IPEndPoint endPoint, Action<Connection> onConnectionAccepted, SenderPool senderPool,
        IoQueue transportScheduler, PipeScheduler applicationScheduler, PinnedBlockMemoryPool memoryPool)
    {
        _listenSocket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.Bind(endPoint);
        _transportScheduler = transportScheduler;
        _applicationScheduler = applicationScheduler;
        _senderPool = senderPool;
        _cts = new CancellationTokenSource();
        _connectionCallback = onConnectionAccepted;
        _memoryPool = memoryPool;
    }

    public void Start()
    {
        _listenSocket.Listen(128);
        _acceptTask = AcceptConnections(_cts.Token);
    }

    private async Task AcceptConnections(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var socket = await _listenSocket.AcceptAsync(cancellationToken);
                var socketWrapper = new SocketWrapper(socket);
                var connection = new Connection(
                    socketWrapper,
                    _senderPool,
                    new Receiver(),
                    _transportScheduler,
                    _applicationScheduler,
                    _memoryPool);
                _connectionCallback(connection);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation, no action needed
            Log.Debug("TcpServer: AcceptConnections cancelled");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "TcpServer: Error accepting connections");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        try
        {
            await _acceptTask;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "TcpServer: Error during DisposeAsync");
        }
        finally
        {
            _listenSocket.Close();
            _listenSocket.Dispose();
            _cts.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}