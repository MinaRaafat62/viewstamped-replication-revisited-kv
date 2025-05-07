using VsrReplica.VsrCore.Handlers;
using VsrReplica.VsrCore.Messages;

namespace VsrReplica.VsrCore.ReplicaInternals;

public class ReplicaHandlers
{
    private static readonly Dictionary<Command, IVsrCommandHandler> CommandHandlers = new();

    private static void RegisterHandler(Command command, IVsrCommandHandler handler)
    {
        if (!CommandHandlers.TryAdd(command, handler))
            throw new ArgumentException($"Handler for command {command} already registered");
    }

    public static IVsrCommandHandler GetCommandHandler(Command command)
    {
        if (CommandHandlers.TryGetValue(command, out var handler))
            return handler;

        throw new KeyNotFoundException($"No handler registered for command {command}");
    }

    public static void RegisterCommandHandlers()
    {
        RegisterHandler(Command.Ping, new PingHandler());
        RegisterHandler(Command.Request, new RequestHandler());
        RegisterHandler(Command.Commit, new CommitHandler());
        RegisterHandler(Command.Prepare, new PrepareHandler());
        RegisterHandler(Command.PrepareOk, new PrepareOkHandler());
        RegisterHandler(Command.StartViewChange, new StartViewChangeHandler());
        RegisterHandler(Command.DoViewChange, new DoViewChangeHandler());
        RegisterHandler(Command.StartView, new StartViewHandler());
        RegisterHandler(Command.Recovery, new RecoveryHandler());
        RegisterHandler(Command.RecoveryResponse, new RecoveryResponseHandler());
    }
}