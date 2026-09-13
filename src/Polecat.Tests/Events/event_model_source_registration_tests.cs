using JasperFx.Events;
using JasperFx.Events.EventModeling;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region The registered projection under test

public record LedgerOpened(string Owner);

public record LedgerCredited(decimal Amount);

public record LedgerClosed;

/// <summary>
///     A self-aggregating snapshot — one document, three applied event types, which is the whole role
///     set of a State View slice.
/// </summary>
public partial class LedgerBalance
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public bool Closed { get; set; }

    public static LedgerBalance Create(LedgerOpened e) => new() { Owner = e.Owner };

    public void Apply(LedgerCredited e) => Balance += e.Amount;

    public void Apply(LedgerClosed _) => Closed = true;
}

/// <summary>The ancillary store's marker — its own document type, so its slice is distinguishable.</summary>
public interface IEventModelAncillaryStore : IDocumentStore;

public record ManifestLogged(string Reference);

public partial class ManifestLog
{
    public Guid Id { get; set; }
    public string Reference { get; set; } = string.Empty;

    public static ManifestLog Create(ManifestLogged e) => new() { Reference = e.Reference };
}

#endregion

/// <summary>
///     #594 / jasperfx#825 — <c>AddPolecat</c> and <c>AddPolecatStore&lt;T&gt;</c> each register the
///     store-derived Event Model rung, so every registered projection becomes a
///     <see cref="SlicePattern.View" /> slice with no Bobcat reference and nobody having written one
///     down.
/// </summary>
/// <remarks>
///     <para>
///         Asserted through <see cref="EventModelDiscovery" />, the path a tool actually takes, rather
///         than by resolving <see cref="ProjectionEventModelSource" /> out of the container and calling
///         it. A source that is constructed and never registered under
///         <see cref="IEventModelDefinitionSource" /> would satisfy the second and contribute nothing
///         to the first, which is precisely the failure this issue exists to prevent.
///     </para>
///     <para>
///         The ancillary arm is not a formality. <c>AddPolecatStore&lt;T&gt;</c> registers its store
///         under the marker type <c>T</c> and deliberately does NOT add it to the
///         <c>GetServices&lt;IEventStore&gt;()</c> list the parameterless source would read, so an
///         ancillary store's projections are invisible unless its own registration names it.
///     </para>
/// </remarks>
public class event_model_source_registration_tests
{
    private static void ConfigureMain(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = "event_model_main";
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Snapshot<LedgerBalance>(SnapshotLifecycle.Inline);
    }

    private static void ConfigureAncillary(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = "event_model_ancillary";
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Snapshot<ManifestLog>(SnapshotLifecycle.Inline);
    }

    private static async Task<IReadOnlyList<EventModelSliceDescriptor>> SlicesFrom(
        IServiceProvider provider, CancellationToken token)
    {
        var models = await EventModelDiscovery.AssembleAsync(provider, token);
        return models.SelectMany(x => x.Slices).ToList();
    }

    [Fact]
    public async Task add_polecat_registers_the_store_derived_source()
    {
        var token = TestContext.Current.CancellationToken;

        var services = new ServiceCollection();
        services.AddPolecat(ConfigureMain);

        await using var provider = services.BuildServiceProvider();

        var slices = await SlicesFrom(provider, token);

        var ledger = slices.ShouldHaveSingleItem();

        // Named after the DOCUMENT, not the projection class — that is what makes a store-derived
        // slice merge with a spec-declared one of the same name rather than sitting beside it.
        ledger.Name.ShouldBe(nameof(LedgerBalance));
        ledger.Pattern.ShouldBe(SlicePattern.View);

        ledger.ReadModelTypes.Select(x => x.Name).ShouldContain(nameof(LedgerBalance));
        ledger.ProjectionTypes.ShouldNotBeEmpty();

        // Every event the projection's Create / Apply methods take.
        var consumed = ledger.ConsumedEvents.Select(x => x.Name).ToList();
        consumed.ShouldContain(nameof(LedgerOpened));
        consumed.ShouldContain(nameof(LedgerCredited));
        consumed.ShouldContain(nameof(LedgerClosed));

        // Plus the two framework markers, which is shared JasperFx behaviour rather than anything
        // Polecat adds: JasperFxSingleStreamProjectionBase.determineEventTypes appends Archived and
        // Compacted<TDoc> to a single-stream projection's AllEventTypes (jasperfx#778 / #796), because
        // an aggregate has no reason to declare an Apply for either and every store composes its
        // async loader's allow list from that set. Asserted rather than filtered out: this is what
        // Marten and Fisher report too, and a test that quietly trimmed them would hide a real change
        // if the set ever stopped carrying them.
        consumed.ShouldContain(typeof(JasperFx.Events.Archived).Name);
        consumed.ShouldContain(typeof(JasperFx.Events.Compacted<LedgerBalance>).Name);
    }

    [Fact]
    public async Task an_ancillary_store_contributes_its_own_slices()
    {
        var token = TestContext.Current.CancellationToken;

        var services = new ServiceCollection();
        services.AddPolecat(ConfigureMain);
        services.AddPolecatStore<IEventModelAncillaryStore>(ConfigureAncillary);

        await using var provider = services.BuildServiceProvider();

        var names = (await SlicesFrom(provider, token)).Select(x => x.Name).ToList();

        names.ShouldContain(nameof(LedgerBalance));
        names.ShouldContain(nameof(ManifestLog),
            "AddPolecatStore<T> registers its store under the marker type only, so an ancillary store's "
            + "projections reach the Event Model only if its own registration names it.");
    }

    [Fact]
    public async Task an_ancillary_store_on_its_own_still_contributes()
    {
        // No AddPolecat at all -- the ancillary registration has to stand by itself, because there is
        // no primary store's GetServices<IEventStore>() list for it to ride in on.
        var token = TestContext.Current.CancellationToken;

        var services = new ServiceCollection();
        services.AddPolecatStore<IEventModelAncillaryStore>(ConfigureAncillary);

        await using var provider = services.BuildServiceProvider();

        var slices = await SlicesFrom(provider, token);

        slices.Select(x => x.Name).ShouldContain(nameof(ManifestLog));
    }

    [Fact]
    public async Task the_derived_slice_merges_with_a_declared_slice_of_the_same_name()
    {
        var token = TestContext.Current.CancellationToken;

        var services = new ServiceCollection();
        services.AddPolecat(ConfigureMain);

        // A Bobcat-style declaration of the SAME slice, naming the document the way the curated model
        // file and Bobcat's {readmodel} capture do. One slice must come back carrying both claims,
        // not two stickies saying the same thing.
        services.AddEventModelSource(new DeclaredLedgerSource());

        await using var provider = services.BuildServiceProvider();

        var slices = await SlicesFrom(provider, token);

        var ledger = slices.Single(x => x.Name == nameof(LedgerBalance));

        // The declared half survives the merge...
        ledger.CommandType.ShouldNotBeNull();
        ledger.CommandType!.Name.ShouldBe(nameof(CreditLedger));

        // ...and so does the derived half, which nobody wrote down.
        ledger.ReadModelTypes.Select(x => x.Name).ShouldContain(nameof(LedgerBalance));
        ledger.ConsumedEvents.Select(x => x.Name).ShouldContain(nameof(LedgerCredited));
    }

    public record CreditLedger(Guid Id, decimal Amount);

    /// <summary>A stand-in for the Declared rung — the shape a Bobcat spec contributes.</summary>
    private sealed class DeclaredLedgerSource : IEventModelDefinitionSource
    {
        public Uri Subject { get; } = new("event-model://declared-test");

        public EventModelProvenance Provenance => EventModelProvenance.Declared;

        public Task<EventModelDescriptor?> TryCreateAsync(IServiceProvider services, CancellationToken token)
        {
            var slice = EventModelSliceDescriptor.Named(nameof(LedgerBalance)) with
            {
                Pattern = SlicePattern.View,
                CommandType = JasperFx.Descriptors.TypeDescriptor.For(typeof(CreditLedger))
            };

            return Task.FromResult<EventModelDescriptor?>(
                new EventModelDescriptor(ProjectionEventModelSource.DefaultModelName, [slice]));
        }
    }
}
