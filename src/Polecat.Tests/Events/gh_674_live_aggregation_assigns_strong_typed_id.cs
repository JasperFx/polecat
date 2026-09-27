using System.Text.Json.Serialization;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

#region document types

/// <summary>A Guid-backed strong-typed identifier — the shape the issue reports.</summary>
public readonly record struct Gh674LetterId(Guid Value);

/// <summary>And a string-backed one, for a store using string stream identity.</summary>
public readonly record struct Gh674ParcelId(string Value);

public record Gh674Scanned;

/// <summary>
///     Live-aggregated (no <c>Snapshot</c> registration), keyed by a wrapper.
/// </summary>
/// <remarks>
///     Deliberately does <b>not</b> set <c>Id</c> in a <c>Create</c> method — assigning the stream
///     identity onto the aggregate is the store's job, which is the whole point of the issue. A
///     <c>Create(IEvent&lt;T&gt;)</c> that wrapped <c>e.StreamId</c> itself would pass on the broken
///     build and prove nothing.
/// </remarks>
public class Gh674Letter
{
    [JsonInclude] public Gh674LetterId Id { get; set; }
    [JsonInclude] public int Count { get; set; }

    public void Apply(Gh674Scanned _) => Count++;
}

/// <summary>The control: identical, with a bare <c>Guid</c>. This always worked.</summary>
public class Gh674PlainLetter
{
    [JsonInclude] public Guid Id { get; set; }
    [JsonInclude] public int Count { get; set; }

    public void Apply(Gh674Scanned _) => Count++;
}

/// <summary>Snapshotted, so <c>FetchForWriting</c> rebuilds from events over a stored baseline.</summary>
public class Gh674Snapshotted
{
    [JsonInclude] public Gh674LetterId Id { get; set; }
    [JsonInclude] public int Count { get; set; }

    public void Apply(Gh674Scanned _) => Count++;
}

#endregion

/// <summary>
///     #674 — an aggregate keyed by a <c>[StronglyTypedId]</c> wrapper gets its <c>Id</c> assigned on
///     every aggregation path, not just when the member is the bare stream-id type.
/// </summary>
/// <remarks>
///     <para>
///         <c>QueryEventStore.TrySetIdentity</c> assigned only when the member's type <em>was</em> the
///         raw stream id, so a wrapper was skipped and the aggregate came back with <c>Id</c> at
///         <c>default</c>. Marten populates it, so this was a missing unwrap rather than a decision.
///     </para>
///     <para>
///         ⚠️ <b>The snapshot case is why this was easy to miss.</b> An inline snapshot of such an
///         aggregate reads back <em>correctly</em> — the value was serialized into the stored document,
///         so the document path answers from storage and never goes through the assignment. Only a
///         rebuild from events does, which is what <c>FetchForWriting</c> always does even for a
///         snapshotted type. Hence the snapshotted arm below.
///     </para>
/// </remarks>
public class gh_674_live_aggregation_assigns_strong_typed_id : OneOffConfigurationsContext
{
    [Fact]
    public async Task fetch_latest_assigns_a_guid_backed_wrapper()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await StartStreamAsync<Gh674Letter>();

        await using var session = theStore.LightweightSession();
        var letter = await session.Events.FetchLatest<Gh674Letter>(streamId,
            TestContext.Current.CancellationToken);

        letter.ShouldNotBeNull();
        letter.Count.ShouldBe(2, "the events were not applied at all, so the identity is not what failed");
        letter.Id.ShouldBe(new Gh674LetterId(streamId));
    }

    [Fact]
    public async Task fetch_for_writing_assigns_a_guid_backed_wrapper()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await StartStreamAsync<Gh674Letter>();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Gh674Letter>(streamId,
            TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.ShouldBe(new Gh674LetterId(streamId));
    }

    [Fact]
    public async Task aggregate_stream_assigns_a_guid_backed_wrapper()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await StartStreamAsync<Gh674Letter>();

        await using var session = theStore.QuerySession();
        var letter = await session.Events.AggregateStreamAsync<Gh674Letter>(streamId,
            token: TestContext.Current.CancellationToken);

        letter.ShouldNotBeNull();
        letter.Id.ShouldBe(new Gh674LetterId(streamId));
    }

    /// <summary>
    ///     A snapshotted aggregate, where <c>FetchForWriting</c> rebuilds from events over the stored
    ///     document — the arm the issue singles out as the tell that aggregation, not storage, was the
    ///     missing piece.
    /// </summary>
    [Fact]
    public async Task fetch_for_writing_assigns_the_wrapper_on_a_snapshotted_aggregate()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<Gh674Snapshotted>(SnapshotLifecycle.Inline));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await StartStreamAsync<Gh674Snapshotted>();

        await using var session = theStore.LightweightSession();

        // The document path reads correctly even on the broken build, because the value was serialized
        // into the stored snapshot. Asserted anyway: it is the baseline the rebuild starts from.
        var stored = await session.LoadAsync<Gh674Snapshotted>(new Gh674LetterId(streamId),
            TestContext.Current.CancellationToken);
        stored.ShouldNotBeNull().Id.ShouldBe(new Gh674LetterId(streamId));

        var stream = await session.Events.FetchForWriting<Gh674Snapshotted>(streamId,
            TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.ShouldBe(new Gh674LetterId(streamId));
    }

    /// <summary>
    ///     A string-backed wrapper under string stream identity.
    /// </summary>
    /// <remarks>
    ///     Its own store, because stream identity is store configuration. Worth the rebuild: the raw id
    ///     the assignment receives is a <c>string</c> here rather than a <c>Guid</c>, so this is the arm
    ///     that catches a fix hard-coded to <c>Guid</c>.
    /// </remarks>
    [Fact]
    public async Task fetch_latest_assigns_a_string_backed_wrapper()
    {
        ConfigureStore(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        const string streamKey = "parcel/674";

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Gh674Parcel>(streamKey, new Gh674Scanned(), new Gh674Scanned());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        var parcel = await query.Events.FetchLatest<Gh674Parcel>(streamKey,
            TestContext.Current.CancellationToken);

        parcel.ShouldNotBeNull();
        parcel.Count.ShouldBe(2);
        parcel.Id.ShouldBe(new Gh674ParcelId(streamKey));
    }

    /// <summary>
    ///     The control: a bare <c>Guid</c> member. This never broke, and it has to keep working — the fix
    ///     rebuilt the assignment rather than adding a branch beside it.
    /// </summary>
    [Fact]
    public async Task a_plain_guid_member_is_still_assigned()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await StartStreamAsync<Gh674PlainLetter>();

        await using var session = theStore.LightweightSession();
        var letter = await session.Events.FetchLatest<Gh674PlainLetter>(streamId,
            TestContext.Current.CancellationToken);

        letter.ShouldNotBeNull();
        letter.Id.ShouldBe(streamId);
    }

    private async Task<Guid> StartStreamAsync<T>() where T : class
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<T>(streamId, new Gh674Scanned(), new Gh674Scanned());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return streamId;
    }
}

/// <summary>String-keyed twin of <see cref="Gh674Letter" />.</summary>
public class Gh674Parcel
{
    [JsonInclude] public Gh674ParcelId Id { get; set; }
    [JsonInclude] public int Count { get; set; }

    public void Apply(Gh674Scanned _) => Count++;
}
