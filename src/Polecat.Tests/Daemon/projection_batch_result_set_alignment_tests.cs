using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #831 on the daemon's side. <see cref="PolecatProjectionBatch" /> had the same one-advance-per-
///     operation walk as a session's flush, and arguably more exposure: it concatenates every tenant
///     session's operations, the raised-event appends and the progression row into one batch, so the
///     mix is not under any single caller's control. It also had no drain at all, so a failed command
///     the walk did not happen to advance over was swallowed and the batch committed around it.
/// </summary>
public partial class projection_batch_result_set_alignment_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PolecatProjectionBatch _batch = null!;

    private async Task StartAsync(Action<StoreOptions>? configure = null)
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<UniqueDoc>().UniqueIndex(x => x.Email);
            configure?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        _batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database);
        AsyncDisposables.Add(_batch);
    }

    private IDocumentSession BatchSession() => _batch.SessionForTenant(StorageConstants.DefaultTenantId);

    private async Task<RevisionedDoc> SeedRevisionedAsync()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "one" };
        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Ct);
        return doc;
    }

    [Fact]
    public async Task a_versioned_write_behind_a_no_result_operation_commits()
    {
        await StartAsync();
        var doc = await SeedRevisionedAsync();

        var session = BatchSession();
        session.Delete<Bystander>(Guid.NewGuid());
        doc.Name = "two";
        session.UpdateRevision(doc, doc.Version + 1);

        await _batch.ExecuteAsync(Ct);

        doc.Version.ShouldBe(2);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");
    }

    [Fact]
    public async Task a_real_conflict_behind_a_no_result_operation_fails_the_batch_and_commits_nothing()
    {
        await StartAsync();
        var doc = await SeedRevisionedAsync();
        var innocent = new Bystander { Id = Guid.NewGuid(), Name = "innocent" };

        var session = BatchSession();
        session.Delete<Bystander>(Guid.NewGuid());
        doc.Name = "stale";
        session.UpdateRevision(doc, doc.Version);
        session.Store(innocent);

        var ex = await Should.ThrowAsync<AggregateException>(() => _batch.ExecuteAsync(Ct));
        ex.InnerExceptions.ShouldHaveSingleItem().ShouldBeOfType<ConcurrencyException>();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("one");
        (await query.LoadAsync<Bystander>(innocent.Id, Ct)).ShouldBeNull();
    }

    /// <summary>
    ///     The mix only the daemon has: raised-event appends read <c>IEvent.Sequence</c> back from a
    ///     result set, in the same batch as a projection's document operations. Behind a delete, the
    ///     first append used to read the second's sequences and the second read nothing at all.
    /// </summary>
    [Fact]
    public async Task raised_event_appends_behind_a_no_result_operation_each_read_their_own_sequences()
    {
        await StartAsync();

        await using var builder = theStore.LightweightSession();
        var first = builder.Events.StartStream(Guid.NewGuid(), new BatchHappened(1), new BatchHappened(2));
        var second = builder.Events.StartStream(Guid.NewGuid(), new BatchHappened(3));

        BatchSession().Delete<Bystander>(Guid.NewGuid());
        _batch.QuickAppendEvents(first);
        _batch.QuickAppendEvents(second);

        await _batch.ExecuteAsync(Ct);

        await using var query = theStore.QuerySession();
        var firstStored = await query.Events.FetchStreamAsync(first.Id, token: Ct);
        var secondStored = await query.Events.FetchStreamAsync(second.Id, token: Ct);

        first.Events.Select(e => e.Sequence).ShouldBe(firstStored.Select(e => e.Sequence));
        second.Events.Select(e => e.Sequence).ShouldBe(secondStored.Select(e => e.Sequence));
    }

    /// <summary>
    ///     The daemon's loop had no drain. Nothing here follows the failing upsert, so no advance
    ///     reaches past its result set unless something drains the reader; without that the error sat
    ///     unread, <c>DisposeAsync</c> swallowed it, and the transaction committed the rest.
    /// </summary>
    [Fact]
    public async Task a_unique_violation_in_the_last_command_fails_the_batch_and_commits_nothing()
    {
        await StartAsync();

        var taken = new UniqueDoc { Id = Guid.NewGuid(), Email = "taken@example.com" };
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(taken);
            await seed.SaveChangesAsync(Ct);
        }

        var plain = new Bystander { Id = Guid.NewGuid(), Name = "stored" };

        var session = BatchSession();
        session.Store(plain);
        session.Store(new UniqueDoc { Id = Guid.NewGuid(), Email = taken.Email });

        var ex = await Should.ThrowAsync<SqlException>(() => _batch.ExecuteAsync(Ct));
        ex.Number.ShouldBe(2601);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bystander>(plain.Id, Ct)).ShouldBeNull();
    }

    /// <summary>
    ///     The whole path, with nothing hand-built: a running daemon driving an
    ///     <see cref="EventProjection" /> that mixes the queued SQL <c>docs/events/projections/flat.md</c>
    ///     recommends with a versioned document. The projection used to fail on every event.
    /// </summary>
    [Fact]
    public async Task an_async_projection_mixing_queued_sql_with_a_versioned_document_catches_up()
    {
        await StartAsync(opts => opts.Projections.Add(new MixedWriteProjection(), ProjectionLifecycle.Async));

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new BatchHappened(1), new BatchHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(MixedWriteProjection.IdFor(streamId, 1), Ct))
            .ShouldNotBeNull().Name.ShouldBe("projected");
        (await query.LoadAsync<RevisionedDoc>(MixedWriteProjection.IdFor(streamId, 2), Ct))
            .ShouldNotBeNull().Name.ShouldBe("projected");
    }

    public record BatchHappened(int Number);

    public class Bystander
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class UniqueDoc
    {
        public Guid Id { get; set; }
        public string Email { get; set; } = string.Empty;
    }

    public partial class MixedWriteProjection : EventProjection
    {
        public static Guid IdFor(Guid streamId, int number)
        {
            var bytes = streamId.ToByteArray();
            bytes[0] = (byte)number;
            return new Guid(bytes);
        }

        public void Project(IEvent<BatchHappened> e, IDocumentSession ops)
        {
            ops.Delete<Bystander>(Guid.NewGuid());
            ops.QueueSqlCommand("DECLARE @x int = 1;");
            ops.Store(new RevisionedDoc { Id = IdFor(e.StreamId, e.Data.Number), Name = "projected" });
        }
    }
}
