using JasperFx.Events;
using JasperFx.Events.Protected;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Compaction replaces the last event with a <see cref="Compacted{T}" /> snapshot and deletes the rest, so
///     every way out of it that leaves the history unrecoverable has to be a deliberate one. These pin the two
///     that were not: a stream folding to no aggregate, and an archiver the store could not call.
/// </summary>
[Collection("integration")]
public class stream_compacting_safety_tests : IntegrationContext
{
    public stream_compacting_safety_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly object[] TheDeletedStream =
        [new ItemCreated("Doomed"), new ItemUpdated("Still doomed"), new ItemUpdated("Very doomed"), new ItemDeleted()];

    [Fact]
    public async Task a_guid_stream_folding_to_null_keeps_every_event()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_null_guid");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<DeletableAggregate>(streamId, TheDeletedStream);
        await theSession.SaveChangesAsync(Ct);

        await using var compacting = theStore.LightweightSession();
        await compacting.Events.CompactStreamAsync<DeletableAggregate>(streamId);
        await compacting.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Select(x => x.Data.GetType())
            .ShouldBe([typeof(ItemCreated), typeof(ItemUpdated), typeof(ItemUpdated), typeof(ItemDeleted)]);

        var state = await query.Events.FetchStreamStateAsync(streamId, Ct);
        state.ShouldNotBeNull().CompactedVersion.ShouldBe(0);
    }

    [Fact]
    public async Task a_string_stream_folding_to_null_keeps_every_event()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "compact_null_string";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var streamKey = Guid.NewGuid().ToString();
        theSession.Events.StartStream<DeletableAggregate>(streamKey, TheDeletedStream);
        await theSession.SaveChangesAsync(Ct);

        await using var compacting = theStore.LightweightSession();
        await compacting.Events.CompactStreamAsync<DeletableAggregate>(streamKey);
        await compacting.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamKey, token: Ct);
        events.Select(x => x.Data.GetType())
            .ShouldBe([typeof(ItemCreated), typeof(ItemUpdated), typeof(ItemUpdated), typeof(ItemDeleted)]);
    }

    [Fact]
    public async Task a_cutoff_folding_to_null_keeps_the_stream_that_carried_on_past_it()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_null_cutoff");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<DeletableAggregate>(streamId, TheDeletedStream);
        await theSession.SaveChangesAsync(Ct);

        await using (var appending = theStore.LightweightSession())
        {
            appending.Events.Append(streamId, new ItemCreated("Back again"));
            await appending.SaveChangesAsync(Ct);
        }

        // Deleted at version 4, so this cutoff folds to nothing even though the stream is live today
        await using var compacting = theStore.LightweightSession();
        await compacting.Events.CompactStreamAsync<DeletableAggregate>(streamId, x => x.Version = 4);
        await compacting.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Count.ShouldBe(5);
        events.ShouldAllBe(x => !x.Data.GetType().IsGenericType);

        var aggregate = await query.Events.AggregateStreamAsync<DeletableAggregate>(streamId, token: Ct);
        aggregate.ShouldNotBeNull().Name.ShouldBe("Back again");
    }

    [Fact]
    public async Task the_archiver_is_not_told_about_a_compaction_that_will_not_happen()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_null_archiver");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<DeletableAggregate>(streamId, TheDeletedStream);
        await theSession.SaveChangesAsync(Ct);

        var archiver = new CountingArchiver();

        await using var compacting = theStore.LightweightSession();
        await compacting.Events.CompactStreamAsync<DeletableAggregate>(streamId, x => x.Archiver = archiver);
        await compacting.SaveChangesAsync(Ct);

        archiver.Calls.ShouldBe(0);
    }

    [Fact]
    public async Task an_archiver_this_store_cannot_call_refuses_the_compaction()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_bad_archiver");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<DeletableAggregate>(streamId,
            new ItemCreated("Kept"), new ItemUpdated("Still kept"), new ItemUpdated("Kept again"));
        await theSession.SaveChangesAsync(Ct);

        var archiver = new UnusableArchiver();

        await using var compacting = theStore.LightweightSession();

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await compacting.Events.CompactStreamAsync<DeletableAggregate>(streamId, x => x.Archiver = archiver);
        });

        ex.Message.ShouldContain(nameof(UnusableArchiver));

        await compacting.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: Ct)).Count.ShouldBe(3);
    }

    private sealed class CountingArchiver : IEventsArchiver<IDocumentSession>
    {
        public int Calls { get; private set; }

        public Task MaybeArchiveAsync<T>(IDocumentSession operations, StreamCompactingRequest<T> request,
            IReadOnlyList<IEvent> events, CancellationToken cancellation) where T : class
        {
            Calls++;
            return Task.CompletedTask;
        }
    }

    // Closed over something Polecat has no way to hand it. It is not an archiver Polecat can run, and the
    // events are about to be deleted, so it has to be an error rather than a skipped step.
    private sealed class UnusableArchiver : IEventsArchiver<string>
    {
        public Task MaybeArchiveAsync<T>(string operations, StreamCompactingRequest<T> request,
            IReadOnlyList<IEvent> events, CancellationToken cancellation) where T : class
        {
            return Task.CompletedTask;
        }
    }
}
