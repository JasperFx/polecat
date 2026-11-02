using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #871 item 1: a rebuild replays everything, so every dead letter a projection has is about to be
///     written again if its event still fails, and is stale if it no longer does. Marten's teardown deletes
///     them (<c>teardownProjectionStorage</c>). Polecat's kept them, so a rebuild left two rows for one
///     failed event. The rewind half of this was #847, in <see cref="rewind_dead_letters_tests" />.
/// </summary>
public class rebuild_dead_letters_tests : OneOffConfigurationsContext
{
    private const string Alpha = "Alpha";
    private const string Beta = "Beta";

    private IEventStore<IDocumentSession, IQuerySession> theEventStore => theStore;
    private IEventDatabase theEventDatabase => theDatabase;

    private void ConfigureEmptyStore(Action<StoreOptions>? also = null)
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "rebuild_dl_" + Guid.NewGuid().ToString("N")[..8];
            also?.Invoke(opts);
        });
    }

    private Task StoreDeadLetterAsync(string projection, string? tenantId = null)
    {
        return theEventDatabase.StoreDeadLetterEventAsync(null!, new DeadLetterEvent
        {
            Id = Guid.NewGuid(),
            ProjectionName = projection,
            ShardName = ShardName.All,
            EventSequence = 4,
            TenantId = tenantId,
            Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = nameof(InvalidOperationException),
            ExceptionMessage = "boom"
        }, TestContext.Current.CancellationToken);
    }

    private Task<IReadOnlyList<DeadLetterEvent>> DeadLettersAsync(string projection) =>
        theEventDatabase.QueryDeadLetterEventsAsync(new ShardName(projection), null, 0, 100,
            TestContext.Current.CancellationToken);

    private async Task<DeadLetterEvent> WaitForDeadLetterAsync(ShardName shard, Func<DeadLetterEvent, bool> match)
    {
        var deadline = DateTimeOffset.UtcNow.Add(15.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            var found = (await DeadLettersAsync(shard.Name)).FirstOrDefault(match);
            if (found != null) return found;

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("The dead letter never appeared.");
    }

    [Fact]
    public async Task a_rebuild_that_fails_again_leaves_one_dead_letter_and_not_two()
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

        await daemon.RebuildProjectionAsync(shard.Name, token);

        var replayed = await WaitForDeadLetterAsync(shard, x => x.Id != original.Id);

        (await DeadLettersAsync(shard.Name)).ShouldHaveSingleItem().Id.ShouldBe(replayed.Id);

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task a_teardown_takes_that_projections_dead_letters_only()
    {
        var token = TestContext.Current.CancellationToken;
        ConfigureEmptyStore();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await StoreDeadLetterAsync(Alpha);
        await StoreDeadLetterAsync(Beta);

        await theEventStore.TeardownExistingProjectionStateAsync(theDatabase, Alpha, token);

        (await DeadLettersAsync(Alpha)).ShouldBeEmpty();
        (await DeadLettersAsync(Beta)).ShouldHaveSingleItem();
    }

    [Fact]
    public async Task a_composite_teardown_takes_its_members_dead_letters_too()
    {
        var token = TestContext.Current.CancellationToken;
        const string composite = "DeadLetterComposite";

        ConfigureEmptyStore(opts => opts.Projections.CompositeProjectionFor(composite,
            x => x.Add(new Polecat.Tests.Projections.Teardown439ProductProjection())));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        var member = theStore.Options.Projections.All
            .OfType<JasperFx.Events.Projections.Composite.CompositeProjection<IDocumentSession, IQuerySession>>()
            .Single().AllProjections().Single().Name;

        await StoreDeadLetterAsync(composite);
        await StoreDeadLetterAsync(member);
        await StoreDeadLetterAsync(Beta);

        await theEventStore.TeardownExistingProjectionStateAsync(theDatabase, composite, token);

        (await DeadLettersAsync(composite)).ShouldBeEmpty();
        (await DeadLettersAsync(member)).ShouldBeEmpty();
        (await DeadLettersAsync(Beta)).ShouldHaveSingleItem();
    }

    /// <summary>
    ///     Marten's per-tenant teardown lists the tenant's dead letters among what it resets, and a rebuild
    ///     of one tenant has no business touching another tenant's.
    /// </summary>
    [Fact]
    public async Task a_rebuild_of_one_tenant_takes_that_tenants_dead_letters_only()
    {
        var token = TestContext.Current.CancellationToken;

        ConfigureEmptyStore(opts => opts.Projections.Add<PoisonEventProjection>(ProjectionLifecycle.Async));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        // An EventProjection is named by its full type name, so this has to come from the store.
        var projection = theStore.Options.Projections.AllShards().Single().Name.Name;

        await StoreDeadLetterAsync(projection, "Red");
        await StoreDeadLetterAsync(projection, "Blue");

        await theEventStore.DeleteProjectionProgressAsync(theDatabase, projection, "Red", token);

        (await DeadLettersAsync(projection)).ShouldHaveSingleItem().TenantId.ShouldBe("Blue");
    }

    /// <summary>
    ///     Same reasoning as the rewind's: under <see cref="AutoCreate.None" /> a store that has never
    ///     dead-lettered has no table, and that must not fail a rebuild.
    /// </summary>
    [Fact]
    public async Task a_missing_dead_letter_table_does_not_fail_the_teardown()
    {
        var token = TestContext.Current.CancellationToken;

        ConfigureEmptyStore();
        var schema = theStore.Options.DatabaseSchemaName;
        await theDatabase.EnsureStorageExistsAsync(typeof(IEvent), token);

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

        await Should.NotThrowAsync(() => theEventStore.TeardownExistingProjectionStateAsync(theDatabase, Alpha, token));
    }
}
