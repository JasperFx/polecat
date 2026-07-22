using System.Reflection;
using JasperFx;
using Microsoft.Extensions.Logging;
using Polecat.Internal;

namespace Polecat.Tests.Events;

// #345: Polecat surfaces JasperFx's GH-3521 application-assembly-reuse warning
// (JasperFxOptions.ApplicationAssemblyReuseWarning, jasperfx#543). JasperFx only *detects*
// the condition — the warning is non-null only when a later host in the process adopts an
// earlier host's process-pinned application assembly that differs from its own registration
// assembly. Consumers surface it; Polecat does so once at startup from PolecatActivator, the
// always-on hosted service.
//
// Because the warning derives from JasperFx's process-static RememberedApplicationAssembly,
// these tests exercise the two integration seams in isolation rather than through the real
// (order-dependent) assembly-resolution path.
public class application_assembly_reuse_warning_tests
{
    // JasperFxOptions.ApplicationAssemblyReuseWarning has an internal setter (owned by the
    // JasperFx assembly), so a test sets it via reflection to simulate a detected reuse.
    private static JasperFxOptions WithReuseWarning(string message)
    {
        var options = new JasperFxOptions();
        typeof(JasperFxOptions)
            .GetProperty(nameof(JasperFxOptions.ApplicationAssemblyReuseWarning),
                BindingFlags.Public | BindingFlags.Instance)!
            .SetValue(options, message);
        return options;
    }

    [Fact]
    public void read_jasperfx_options_buffers_the_reuse_warning()
    {
        var storeOptions = new StoreOptions();

        storeOptions.ReadJasperFxOptions(WithReuseWarning("assembly reuse detected"));

        storeOptions.ApplicationAssemblyReuseWarning.ShouldBe("assembly reuse detected");
    }

    [Fact]
    public void first_non_null_warning_is_not_clobbered_by_a_later_null()
    {
        // Both the primary (AddPolecat) and ancillary (AddPolecatStore<T>) paths call
        // ReadJasperFxOptions; ??= keeps the first non-null value.
        var storeOptions = new StoreOptions();

        storeOptions.ReadJasperFxOptions(WithReuseWarning("first warning"));
        storeOptions.ReadJasperFxOptions(new JasperFxOptions()); // ApplicationAssemblyReuseWarning == null

        storeOptions.ApplicationAssemblyReuseWarning.ShouldBe("first warning");
    }

    [Fact]
    public void no_warning_when_jasperfx_did_not_detect_reuse()
    {
        var storeOptions = new StoreOptions();

        storeOptions.ReadJasperFxOptions(new JasperFxOptions());

        storeOptions.ApplicationAssemblyReuseWarning.ShouldBeNull();
    }

    [Fact]
    public async Task activator_logs_the_buffered_warning_once_at_startup()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "assembly_reuse_warning";
            // ShouldApplyChangesOnStartup stays false and no InitialData is registered, so
            // StartAsync only reaches the warning-logging branch — no database interaction.
            opts.ApplicationAssemblyReuseWarning = "adopted assembly 'A' but registered from 'B'";
        });

        var logger = new CapturingLogger<PolecatActivator>();
        var activator = new PolecatActivator(store, logger);

        await activator.StartAsync(CancellationToken.None);

        var warning = logger.Entries.ShouldHaveSingleItem();
        warning.Level.ShouldBe(LogLevel.Warning);
        warning.Message.ShouldContain("adopted assembly 'A' but registered from 'B'");
    }

    [Fact]
    public async Task activator_logs_nothing_when_there_is_no_warning()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "assembly_reuse_no_warning";
        });

        var logger = new CapturingLogger<PolecatActivator>();
        var activator = new PolecatActivator(store, logger);

        await activator.StartAsync(CancellationToken.None);

        logger.Entries.ShouldBeEmpty();
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
