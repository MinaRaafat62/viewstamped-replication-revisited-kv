using Serilog;
using Serilog.Exceptions;

static void ConfigureLogging()
{
    Log.Logger = new LoggerConfiguration()
        .Enrich.WithExceptionDetails()
        .WriteTo.Console()
        .MinimumLevel.Debug()
        .CreateLogger();
}

ConfigureLogging();