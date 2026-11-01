using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #847: a rewind replays everything at or above its floor, so a dead letter up there is about to be
///     written again if the event still fails, and is stale if it no longer does. Marten deletes them
///     (<c>DocumentStore.EventStore.RewindSubscriptionProgressAsync</c>). Polecat touched progression only,
///     so a replay that failed again left two rows for one failed event, and the counts monitoring reads
///     grew with every rewind.
/// </summary>
public class rewind_dead_letters_tests : OneOffConfigurationsContext
{
    private const string Alpha = "Alpha";
    private const string Beta = "Beta";

    private IEventStore<IDocumentSession, IQuerySession> theEventStore => theStore;
    private IEventDatabase theEventDatabase => theDatabase;

    private void ConfigureEmptyStore(Action<StoreOptions>? also = null)
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "rewind_dl_" + Guid.NewGuid().ToString("N")[..8];
            also?.Invoke(opts);
        });
    }

    private Task StoreDeadLetterAsync(string projection, long sequence, string shardKey = ShardName.All,
        string? tenantId = null)
    {
        return theEventDatabase.StoreDeadLetterEventAsync(null!, new DeadLetterEvent
        {
            Id = Guid.NewGuid(),
            ProjectionName = projection,
            ShardName = shardKey,
            EventSequence = sequence,
            TenantId = tenantId,
            Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = nameof(InvalidOperationException),
            ExceptionMessage = "boom"
        }, TestContext.Current.CancellationToken);
    }

    private async Task<long[]> SequencesAsync(string projection, string shardKey = ShardName.All)
    {
        var rows = await theEventDatabase.QueryDeadLetterEventsAsync(new ShardName(projection, shardKey, 1), null, 0,
            100, TestContext.Current.CancellationToken);

        return rows.Select(x => x.EventSequence).Order().ToArray();
    }

    [Fact]
    public async Task a_replay_that_fails_again_leaves_one_dead_letter_and_not_two()
    {
        var token = TestContext.Current.CancellationToken;

        ConfigureEmptyStore(opts =>
        {
            opts.Projections.Errors.SkipApplyErrors = true;
            opts.Projections.RebuildErrors.SkipApplyErrors = true;
            opts.Projections.Add<PoisonEventProjection>(ProjectionLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new DeadLetterThing(Guid.NewGuid(), "boom", Poison: true));
            await session.SaveChangesAsync(token);
        }

        // Held open: the dead-letter write is a background block keyed to the daemon's own token.
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var shard = daemon.CurrentAgents().Single().Name;
        var original = await WaitForDeadLetterAsync(shard, _ => true);

        await daemon.RewindSubscriptionAsync(shard.Name, token, sequenceFloor: 0);

        // The replay fails the same event again, so its dead letter is rewritten under a new id. Waiting
        // for that id is what proves the replay finished, however long it took.
        var replayed = await WaitForDeadLetterAsync(shard, x => x.Id != original.Id);

        (await AllDeadLettersAsync(shard)).ShouldHaveSingleItem().Id.ShouldBe(replayed.Id);

        await daemon.StopAllAsync();
    }

    private Task<IReadOnlyList<DeadLetterEvent>> AllDeadLettersAsync(ShardName shard) =>
        theEventDatabase.QueryDeadLetterEventsAsync(shard, null, 0, 100, TestContext.Current.CancellationToken);

    private async Task<DeadLetterEvent> WaitForDeadLetterAsync(ShardName shard, Func<DeadLetterEvent, bool> match)
    {
        var deadline = DateTimeOffset.UtcNow.Add(15.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            var found = (await AllDeadLettersAsync(shard)).FirstOrDefault(match);
            if (found != null) return found;

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("The dead letter never appeared.");
    }

    [Fact]
    public async Task only_dead_letters_at_or_above_the_floor_go()
    {
        var token = TestContext.Current.CancellationToken;
        ConfigureEmptyStore();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await StoreDeadLetterAsync(Alpha, 4);
        await StoreDeadLetterAsync(Alpha, 5);
        await StoreDeadLetterAsync(Alpha, 6);

        await theEventStore.RewindSubscriptionProgressAsync(theDatabase, Alpha, token, 5);

        (await SequencesAsync(Alpha)).ShouldBe([4L]);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(null)]
    public async Task a_rewind_to_the_start_takes_every_dead_letter_of_that_projection_only(long? floor)
    {
        var token = TestContext.Current.CancellationToken;
        ConfigureEmptyStore();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await StoreDeadLetterAsync(Alpha, 4);
        await StoreDeadLetterAsync(Beta, 4);

        await theEventStore.RewindSubscriptionProgressAsync(theDatabase, Alpha, token, floor);

        (await SequencesAsync(Alpha)).ShouldBeEmpty();
        (await SequencesAsync(Beta)).ShouldBe([4L]);
    }

    /// <summary>
    ///     The daemon does not always pass the projection's name. Every one of its per-agent paths passes
    ///     <c>agent.Name.Identity</c>, and a dead letter records the two halves separately.
    /// </summary>
    [Fact]
    public async Task a_rewind_by_shard_identity_stays_on_that_shard()
    {
        var token = TestContext.Current.CancellationToken;
        ConfigureEmptyStore();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await StoreDeadLetterAsync(Alpha, 4, "ShardOne");
        await StoreDeadLetterAsync(Alpha, 4, "ShardTwo");

        await theEventStore.RewindSubscriptionProgressAsync(theDatabase, "Alpha:ShardOne", token, 1);

        (await SequencesAsync(Alpha, "ShardOne")).ShouldBeEmpty();
        (await SequencesAsync(Alpha, "ShardTwo")).ShouldBe([4L]);
    }

    /// <summary>
    ///     Under per-tenant event partitioning a floor is a position in one tenant's own numbering, so the
    ///     same sequence number in another tenant is a different event.
    /// </summary>
    [Fact]
    public async Task a_rewind_of_one_tenants_shard_leaves_the_other_tenants_alone()
    {
        var token = TestContext.Current.CancellationToken;
        ConfigureEmptyStore();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await StoreDeadLetterAsync(Alpha, 4, tenantId: "Red");
        await StoreDeadLetterAsync(Alpha, 4, tenantId: "Blue");

        await theEventStore.RewindSubscriptionProgressAsync(theDatabase, "Alpha:All:Red", token, 1);

        var remaining = await theEventDatabase.QueryDeadLetterEventsAsync(new ShardName(Alpha), null, 0, 100, token);
        remaining.ShouldHaveSingleItem().TenantId.ShouldBe("Blue");
    }

    /// <summary>
    ///     A rewind's job is progression. A store that has never dead-lettered under
    ///     <see cref="AutoCreate.None" /> has no table to clear, and that must not fail the rewind.
    /// </summary>
    [Fact]
    public async Task a_missing_dead_letter_table_does_not_fail_the_rewind()
    {
        var token = TestContext.Current.CancellationToken;

        ConfigureEmptyStore();
        var schema = theStore.Options.DatabaseSchemaName;
        await theDatabase.EnsureStorageExistsAsync(typeof(IEvent), token);

        // Applying the schema creates this table with everything else, so it has to be taken away.
        var conn = await OpenConnectionAsync();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DROP TABLE [{schema}].[pc_doc_deadletterevent];";
            await cmd.ExecuteNonQueryAsync(token);
        }

        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.None;
        });

        var shard = new ShardName(Alpha);
        await theEventStore.RewindSubscriptionProgressAsync(theDatabase, shard.Identity, token, 5);

        (await theEventDatabase.ProjectionProgressFor(shard, token)).ShouldBe(5);
    }
}
