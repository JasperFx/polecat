using System.Collections.Concurrent;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging;
using Polecat.Projections;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Configuration;

/// <summary>
///     #1134 (marten#6144): side effects stay on for inline projections, which in Polecat 5 silently never ran, so the
///     store names each Inline projection that overrides <c>RaiseSideEffects()</c> in a warning when it is built.
/// </summary>
public class inline_side_effects_warning_tests
{
    public partial class QuietTally
    {
        public Guid Id { get; set; }
        public int Count { get; set; }

        public void Apply(RetriedTallied e) => Count++;
    }

    // Declares no RaiseSideEffects() of its own
    public partial class QuietTallyProjection : SingleStreamProjection<QuietTally, Guid>;

    private static string nameOf<T>(StoreOptions options) => options.Projections.All.OfType<T>()
        .Cast<ProjectionBase>().Single().Name;

    [Fact]
    public void inline_projections_that_override_raise_side_effects_are_found()
    {
        var options = new StoreOptions();
        options.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);
        options.Projections.Add(new QuietTallyProjection(), ProjectionLifecycle.Inline);

        options.EventGraph.InlineProjectionsThatRaiseSideEffects()
            .ShouldBe([nameOf<PublishingTallyProjection>(options)]);
    }

    [Fact]
    public void async_projections_and_disabled_inline_side_effects_are_not_warned_about()
    {
        var async = new StoreOptions();
        async.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Async);
        async.EventGraph.InlineProjectionsThatRaiseSideEffects().ShouldBeEmpty();

        var disabled = new StoreOptions();
        disabled.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);
        disabled.Events.EnableSideEffectsOnInlineProjections = false;
        disabled.EventGraph.InlineProjectionsThatRaiseSideEffects().ShouldBeEmpty();

        var restored = new StoreOptions();
        restored.RestoreV5Defaults();
        restored.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);
        restored.EventGraph.InlineProjectionsThatRaiseSideEffects().ShouldBeEmpty();
    }

    [Fact]
    public void building_the_store_logs_one_warning_per_projection()
    {
        var logger = new RecordingLogger();
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.DotNetLogger = logger;
        options.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);
        options.Projections.Add(new QuietTallyProjection(), ProjectionLifecycle.Inline);

        using var store = new DocumentStore(options);

        var warning = logger.Warnings.Where(x => x.Contains("RaiseSideEffects()")).ShouldHaveSingleItem();
        warning.ShouldContain(nameOf<PublishingTallyProjection>(options));
        warning.ShouldContain("EnableSideEffectsOnInlineProjections = false");
    }

    [Fact]
    public void no_warning_with_side_effects_turned_off()
    {
        var logger = new RecordingLogger();
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.DotNetLogger = logger;
        options.Events.EnableSideEffectsOnInlineProjections = false;
        options.Projections.Add(new PublishingTallyProjection(), ProjectionLifecycle.Inline);

        using var store = new DocumentStore(options);

        logger.Warnings.ShouldNotContain(x => x.Contains("RaiseSideEffects()"));
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<string> _warnings = new();

        public IReadOnlyList<string> Warnings => _warnings.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning) _warnings.Enqueue(formatter(state, exception));
        }
    }
}
