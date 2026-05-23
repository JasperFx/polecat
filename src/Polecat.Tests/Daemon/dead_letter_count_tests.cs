using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Daemon;

// Regression for Polecat#146 (jasperfx#356): under SkipApplyErrors (the
// JasperFx.Events 2.0 default), a projection Project/Apply failure is recorded as
// a DeadLetterEvent and the shard keeps advancing. DeadLetterEvent is a Polecat
// document (pc_doc_deadletterevent); this exercises that document-backed storage
// plus the IEventDatabase count reads CountDeadLetterEventsAsync /
// FetchDeadLetterCountsAsync (LINQ queries over the document). Mirrors Marten's
// when_skipping_events_in_daemon recipe (EventProjection that throws on a poison
// event, both error options set to skip).

public record DeadLetterThing(Guid Id, string Name, bool Poison);

public class DeadLetterView
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public partial class PoisonEventProjection : EventProjection
{
    public void Project(DeadLetterThing e, IDocumentSession ops)
    {
        if (e.Poison)
        {
            throw new InvalidOperationException("poison event — boom");
        }

        ops.Store(new DeadLetterView { Id = e.Id, Name = e.Name });
    }
}

public class dead_letter_count_tests : OneOffConfigurationsContext
{
    // Dead letters are stored as the DeadLetterEvent document (pc_doc_deadletterevent).
    // The fixed test schema is not truncated by schema migration, so rows persist
    // across test runs — clear them up front so the count assertions are deterministic.
    private async Task ClearDeadLetters(string schema)
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(theStore.Options.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"IF OBJECT_ID('[{schema}].[pc_doc_deadletterevent]', 'U') IS NOT NULL DELETE FROM [{schema}].[pc_doc_deadletterevent];";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task records_and_counts_dead_letters_per_shard()
    {
        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "dead_letters";
            opts.Projections.Errors.SkipApplyErrors = true;
            opts.Projections.RebuildErrors.SkipApplyErrors = true;
            opts.Projections.Add<PoisonEventProjection>(ProjectionLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await ClearDeadLetters("dead_letters");

        var goodId = Guid.NewGuid();
        var poisonId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(),
                new DeadLetterThing(goodId, "ok", Poison: false),
                new DeadLetterThing(poisonId, "boom", Poison: true)); // <-- Project throws here
            await session.SaveChangesAsync();
        }

        // Own the daemon lifecycle: dead-letter storage is posted to a background
        // RetryBlock (JasperFxAsyncDaemon.RecordDeadLetterEventAsync ->
        // _deadLetterBlock.PostAsync) keyed on the daemon's cancellation token, and
        // the block drops queued items once that token is cancelled on dispose. So
        // keep the daemon alive while polling for the flush, then dispose.
        IReadOnlyList<DeadLetterShardCount> counts = [];
        using (var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync())
        {
            // Keep the daemon RUNNING while polling (do not CatchUpAsync — that stops
            // the agents and cancels the daemon's token, and the dead-letter RetryBlock
            // drops queued items on cancellation before they flush). Poll for the
            // background dead-letter store to land while the block is still alive.
            await daemon.StartAllAsync();

            for (var attempt = 0; attempt < 120; attempt++) // up to ~30s
            {
                counts = await theStore.Database.FetchDeadLetterCountsAsync(CancellationToken.None);
                if (counts.Sum(c => c.Count) >= 1) break;
                await Task.Delay(250);
            }

            counts.ShouldNotBeEmpty();

            // Per-shard read — reconstruct the ShardName from the recorded
            // (ProjectionName, ShardKey) pair rather than guessing the names.
            var entry = counts.Single();
            var perShard = await theStore.Database.CountDeadLetterEventsAsync(
                new ShardName(entry.ProjectionName, entry.ShardKey, 1), CancellationToken.None);
            perShard.ShouldBe(1);

            // A shard with no dead letters reads zero.
            var clean = await theStore.Database.CountDeadLetterEventsAsync(
                new ShardName("NoSuchProjection", "All", 1), CancellationToken.None);
            clean.ShouldBe(0);
        }

        // Exactly one poison event recorded across the one shard.
        counts.Sum(c => c.Count).ShouldBe(1);
    }

    // Isolates the storage + count reads from the daemon-recording path: store
    // dead letters directly via IEventDatabase, then read them back.
    [Fact]
    public async Task store_and_count_dead_letters_directly()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = "dead_letters_direct");
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await ClearDeadLetters("dead_letters_direct");

        var db = (IEventDatabase)theDatabase;
        await db.StoreDeadLetterEventAsync(null!, new DeadLetterEvent
        {
            Id = Guid.NewGuid(), ProjectionName = "Alpha", ShardName = "All",
            EventSequence = 7, Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = "InvalidOperationException", ExceptionMessage = "boom"
        }, CancellationToken.None);
        await db.StoreDeadLetterEventAsync(null!, new DeadLetterEvent
        {
            Id = Guid.NewGuid(), ProjectionName = "Alpha", ShardName = "All",
            EventSequence = 9, Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = "InvalidOperationException", ExceptionMessage = "boom2"
        }, CancellationToken.None);
        await db.StoreDeadLetterEventAsync(null!, new DeadLetterEvent
        {
            Id = Guid.NewGuid(), ProjectionName = "Beta", ShardName = "All",
            EventSequence = 3, Timestamp = DateTimeOffset.UtcNow,
            ExceptionType = "InvalidOperationException", ExceptionMessage = "boom3"
        }, CancellationToken.None);

        (await db.CountDeadLetterEventsAsync(new ShardName("Alpha", "All", 1))).ShouldBe(2);
        (await db.CountDeadLetterEventsAsync(new ShardName("Beta", "All", 1))).ShouldBe(1);

        var counts = await db.FetchDeadLetterCountsAsync();
        counts.Count.ShouldBe(2);
        counts.Sum(c => c.Count).ShouldBe(3);
    }
}
