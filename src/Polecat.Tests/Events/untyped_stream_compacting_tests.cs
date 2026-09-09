using JasperFx;
using JasperFx.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #572 — <c>IEventStore.CompactStreamAsync</c>, the UNTYPED overload, which threw
///     "Stream compaction is not yet supported in Polecat" while the typed
///     <c>CompactStreamAsync&lt;T&gt;</c> beside it was complete.
/// </summary>
/// <remarks>
///     <para>
///         The two overloads are not two spellings of one operation. The untyped one's whole contract
///         is <em>resolve the aggregate type from stream state</em>, and that is the only form
///         available to a caller holding a runtime <see cref="Type" />: a stream compaction policy
///         ("compact any stream of aggregate type X whose un-compacted growth exceeds N") selects with
///         <c>QueryStreamStates()</c> and then has a <c>Type</c>, not a <c>T</c>. It cannot close the
///         typed overload over it without doing this reflection itself.
///     </para>
///     <para>
///         So the shape of the bug was a store that could SELECT exactly the right streams —
///         <c>QueryStreamStates()</c> landed in #534 and works — and then act on none of them.
///         Reported from a cross-store parity spec run against Marten, Polecat and Fisher, where every
///         armed case failed on Polecat and every selector case passed.
///     </para>
/// </remarks>
public class untyped_stream_compacting_tests
{
    private static DocumentStore CreateStore(StreamIdentity identity, string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.StreamIdentity = identity;
        });
    }

    /// <summary>
    ///     The issue's own repro, string-identified: three events in, one <see cref="Compacted{T}" />
    ///     out, with nothing but the stream key and the store handed to the call.
    /// </summary>
    [Fact]
    public async Task compacts_a_string_identified_stream_resolved_from_stream_state()
    {
        using var store = CreateStore(StreamIdentity.AsString, "compact_untyped_str");
        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        const string streamKey = "freighter-1";

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Freighter>(streamKey,
                new Loaded(1), new Loaded(2), new Loaded(3));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await ((IEventStore)store).CompactStreamAsync(streamKey, TestContext.Current.CancellationToken);

        await using var query = store.QuerySession();

        // The store-level overload owns the session it opened, so it commits. The typed overload
        // deliberately does not — it queues onto the caller's unit of work — and reading the events
        // back through a SEPARATE session is what tells the two apart. Before this was fixed by
        // mirroring Marten without its missing SaveChangesAsync, the whole call was a silent no-op
        // that reported success.
        var events = await query.Events.FetchStreamAsync(streamKey, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        var compacted = events[0].Data.ShouldBeOfType<Compacted<Freighter>>();
        compacted.Snapshot.Cargo.ShouldBe(6);

        var state = await query.Events.FetchStreamStateAsync(streamKey, TestContext.Current.CancellationToken);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(3);
        state.CompactedVersion.ShouldBe(3);
    }

    /// <summary>
    ///     The Guid half of the same pair, and the CritterWatch "Compact Stream" button's shape.
    /// </summary>
    [Fact]
    public async Task compacts_a_guid_identified_stream_resolved_from_stream_state()
    {
        using var store = CreateStore(StreamIdentity.AsGuid, "compact_untyped_guid");
        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<QuestParty>(streamId,
                new QuestStarted("Destroy the Ring"),
                new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas"]),
                new MembersJoined(2, "Moria", ["Gimli"]));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await ((IEventStore)store).CompactStreamAsync(streamId, TestContext.Current.CancellationToken);

        await using var query = store.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        var compacted = events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
        compacted.Snapshot.Name.ShouldBe("Destroy the Ring");
        compacted.Snapshot.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);

        // The snapshot has to be readable as the stream's whole history, which is the only reason
        // deleting the events it folded is safe.
        var live = await query.Events.AggregateStreamAsync<QuestParty>(streamId,
            token: TestContext.Current.CancellationToken);
        live.ShouldNotBeNull();
        live.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
    }

    /// <summary>
    ///     The policy driver's actual shape: select streams by aggregate type through
    ///     <c>QueryStreamStates()</c>, then compact each one holding only a runtime <see cref="Type" />.
    ///     This is the case the typed overload cannot serve at all, and the reason #572 is not
    ///     cosmetic.
    /// </summary>
    [Fact]
    public async Task a_runtime_type_selector_can_act_on_every_stream_it_selects()
    {
        using var store = CreateStore(StreamIdentity.AsGuid, "compact_untyped_policy");
        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        await using (var session = store.LightweightSession())
        {
            for (var i = 0; i < 3; i++)
            {
                session.Events.StartStream<QuestParty>(Guid.NewGuid(),
                    new QuestStarted($"Quest {i}"),
                    new MembersJoined(1, "Town", ["Hero"]),
                    new MonsterSlain("Goblin", 10));
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession();

        // Selection: exactly what a "compact anything of type X grown past N" policy issues, and the
        // half that already worked (#534 / jasperfx#740).
        var selected = await ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates()
            .Where(x => x.AggregateType == typeof(QuestParty) && x.Version - x.CompactedVersion > 2)
            .ToListAsync(TestContext.Current.CancellationToken);

        selected.Count.ShouldBe(3);

        foreach (var state in selected)
        {
            await ((IEventStore)store).CompactStreamAsync(state.Id, TestContext.Current.CancellationToken);
        }

        foreach (var state in selected)
        {
            var events = await query.Events.FetchStreamAsync(state.Id, token: TestContext.Current.CancellationToken);
            events.Count.ShouldBe(1);
            events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
        }

        // And the watermark moved, so the same policy does not select them again on its next pass —
        // the property that makes a nightly policy idempotent rather than a busy loop.
        var stillSelected = await ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates()
            .Where(x => x.AggregateType == typeof(QuestParty) && x.Version - x.CompactedVersion > 2)
            .ToListAsync(TestContext.Current.CancellationToken);

        stillSelected.ShouldBeEmpty();
    }

    /// <summary>
    ///     A stream that does not exist cannot resolve an aggregate type, and saying so beats the
    ///     alternative this method has: fetch nothing, fold nothing, and return successfully.
    /// </summary>
    [Fact]
    public async Task an_unknown_stream_is_refused_rather_than_quietly_doing_nothing()
    {
        using var store = CreateStore(StreamIdentity.AsGuid, "compact_untyped_missing");
        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => ((IEventStore)store).CompactStreamAsync(Guid.NewGuid(), TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("no such stream");
    }

    /// <summary>
    ///     A stream started without an aggregate type has nothing for this overload to resolve. The
    ///     message has to say that the TYPE is missing and name the typed overload as the way through
    ///     — "stream not found" would point the caller at their data instead of at their call.
    /// </summary>
    [Fact]
    public async Task a_stream_with_no_aggregate_type_names_the_typed_overload_as_the_way_through()
    {
        using var store = CreateStore(StreamIdentity.AsGuid, "compact_untyped_notype");
        await store.Advanced.Clean.DeleteAllEventDataAsync(TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            // No <T>: pc_streams.type stays null.
            session.Events.StartStream(streamId, new QuestStarted("Untyped"), new MonsterSlain("Orc", 1));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => ((IEventStore)store).CompactStreamAsync(streamId, TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("no aggregate type recorded");
        ex.Message.ShouldContain("CompactStreamAsync<T>");
    }

    /// <summary>
    ///     The identity guard the typed path already had (marten#5244 / <see
    ///     cref="stream_compacting_identity_mismatch_tests" />), extended to this entry point —
    ///     and it has to run BEFORE the stream-state read, or a mismatched overload matches no row and
    ///     comes back as "no such stream", blaming the data for a configuration mistake.
    /// </summary>
    [Fact]
    public async Task a_mismatched_overload_names_the_configuration_not_the_data()
    {
        using var guidStore = CreateStore(StreamIdentity.AsGuid, "compact_untyped_mix_guid");

        var byKey = await Should.ThrowAsync<InvalidOperationException>(
            () => ((IEventStore)guidStore).CompactStreamAsync("some-key", TestContext.Current.CancellationToken));
        byKey.Message.ShouldContain("identify streams with Guids");

        using var stringStore = CreateStore(StreamIdentity.AsString, "compact_untyped_mix_str");

        var byId = await Should.ThrowAsync<InvalidOperationException>(
            () => ((IEventStore)stringStore).CompactStreamAsync(Guid.NewGuid(), TestContext.Current.CancellationToken));
        byId.Message.ShouldContain("identify streams with strings");
    }
}

public record Loaded(int Amount);

/// <summary>
///     A string-identified aggregate, deliberately trivial — the issue's own repro type. Every
///     Polecat aggregate in the suite is Guid-identified, and the untyped overload's identity
///     handling is one of the two things #572 is about.
/// </summary>
public class Freighter
{
    public string Id { get; set; } = string.Empty;
    public int Cargo { get; set; }

    public void Apply(Loaded e) => Cargo += e.Amount;
}
