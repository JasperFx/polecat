using JasperFx;
using Microsoft.Extensions.DependencyInjection;

namespace Polecat.Tests.Events;

// Mirrors Marten PR #4741 (CoreTests/jasper_fx_mechanics.cs): when the JasperFx host
// opts into advanced tracking (typically via CritterWatch configuration) by setting
// JasperFxOptions.EnableAdvancedTracking, every Polecat DocumentStore in the container
// — primary (AddPolecat) and ancillary (AddPolecatStore<T>) — opts into extended
// progression tracking so downstream tooling sees the richer per-shard state.
//
// The single integration point is StoreOptions.ReadJasperFxOptions, called by both
// registration paths after the IConfigurePolecat chain and before the IDocumentStore
// singleton is constructed.

public class advanced_tracking_propagation_tests
{
    [Fact]
    public void enable_advanced_tracking_propagates_to_main_document_store()
    {
        var services = new ServiceCollection();
        services.AddJasperFx(o => o.EnableAdvancedTracking = true);
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "advanced_tracking_main";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }

    [Fact]
    public void advanced_tracking_off_leaves_extended_progression_tracking_at_default()
    {
        // Sanity: when EnableAdvancedTracking is not set (default false), we do NOT
        // flip EnableExtendedProgressionTracking on. Per-store opt-in is preserved.
        var services = new ServiceCollection();
        services.AddJasperFx();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "advanced_tracking_off_main";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();
    }

    [Fact]
    public void no_jasperfx_options_registered_leaves_extended_progression_tracking_at_default()
    {
        // When AddJasperFx is never called, ReadJasperFxOptions receives null and is a
        // no-op — the per-store default (opt-out) stands.
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "advanced_tracking_none";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();
    }

    // Both AddPolecat and AddPolecatStore<T> funnel through the same
    // StoreOptions.ReadJasperFxOptions integration point. Polecat's AddPolecatStore<T>
    // resolution throws InvalidCastException (no dynamic store proxy — see
    // event_store_instrumentation_di_tests.cs), so the ancillary path is exercised
    // directly against the shared method instead of through container resolution.

    [Fact]
    public void read_jasperfx_options_propagates_advanced_tracking_when_enabled()
    {
        var options = new StoreOptions();
        options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();

        options.ReadJasperFxOptions(new JasperFxOptions { EnableAdvancedTracking = true });

        options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }

    [Fact]
    public void read_jasperfx_options_leaves_flag_alone_when_advanced_tracking_off()
    {
        var options = new StoreOptions();

        options.ReadJasperFxOptions(new JasperFxOptions { EnableAdvancedTracking = false });

        options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();
    }

    [Fact]
    public void read_jasperfx_options_is_a_noop_for_null()
    {
        var options = new StoreOptions();
        options.Events.EnableExtendedProgressionTracking = true;

        options.ReadJasperFxOptions(null);

        // Null host options must not disturb the existing per-store value.
        options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }
}
