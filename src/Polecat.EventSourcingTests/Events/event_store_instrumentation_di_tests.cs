using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// Issue #177: the DI registration mechanic that lets external tooling (e.g. CritterWatch)
// resolve IEventStoreInstrumentation from the container, flip ExtendedProgressionEnabled,
// and have the toggle land on the store BEFORE first IDocumentStore resolution. Mirrors
// Marten's CoreTests.Events.EventGraph_IEventStoreInstrumentation. The storage-side
// effects of the flag (schema delta, daemon write, shard-state read) are covered by
// event_store_instrumentation_tests.cs — these tests focus purely on the wiring.

public interface IInvoicingStore : IDocumentStore;

public class event_store_instrumentation_di_tests
{
    [Fact]
    public void build_store_with_progression_tracking_override()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var instrumentation = provider.GetRequiredService<IEventStoreInstrumentation>();
        instrumentation.ShouldNotBeNull();
        // Defaults to opt-out — same as Marten and Polecat's StoreOptions default.
        instrumentation.ExtendedProgressionEnabled.ShouldBeFalse();

        // External tooling flips the toggle BEFORE first IDocumentStore resolution.
        instrumentation.ExtendedProgressionEnabled = true;

        // First IDocumentStore resolution drains the IConfigurePolecat chain, which
        // includes the SetEventStoreInstrumentation instance — so the toggle lands
        // on options.Events.EnableExtendedProgressionTracking.
        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }

    [Fact]
    public void registers_one_instrumentation_per_store_and_primary_toggle_lands()
    {
        // Mirrors the shape of Marten's
        // build_store_with_progression_tracking_override_with_ancillary_store: each of
        // AddPolecat / AddPolecatStore<T> contributes its own IEventStoreInstrumentation,
        // so GetServices<IEventStoreInstrumentation>() yields one per store and external
        // tooling can flip them all in one pass.
        //
        // Polecat's AddPolecatStore<T> does (T)(IDocumentStore)store at runtime, which
        // requires DocumentStore to actually implement T — Polecat does not emit dynamic
        // proxies for ancillary stores the way Marten does, so resolving IInvoicingStore
        // here would throw InvalidCastException. That's a separate concern from #177; this
        // test verifies the instrumentation registration shape + the primary-store toggle
        // landing, both of which are entirely the #177 wiring.

        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });
        services.AddPolecatStore<IInvoicingStore>(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "invoices_177";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        // Two IEventStoreInstrumentation registrations exist before BuildServiceProvider.
        services.Count(d => d.ServiceType == typeof(IEventStoreInstrumentation)).ShouldBe(2);

        using var provider = services.BuildServiceProvider();

        var instruments = provider.GetServices<IEventStoreInstrumentation>().ToList();
        instruments.Count.ShouldBe(2);
        instruments.ShouldAllBe(i => !i.ExtendedProgressionEnabled);

        foreach (var i in instruments)
        {
            i.ExtendedProgressionEnabled = true;
        }

        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();
    }
}
