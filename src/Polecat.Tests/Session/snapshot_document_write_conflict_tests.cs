using System.Data;
using JasperFx;
using JasperFx.Metadata;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Session;

/// <summary>
///     #854. A document upsert was <c>MERGE ... WITH (HOLDLOCK)</c> at every isolation level, and SQL
///     Server raises no snapshot update conflict for a statement carrying that hint: it reads the latest
///     committed row and writes over it. So a <c>Snapshot</c> session's <c>Store</c> and <c>Update</c>
///     silently overwrote a concurrent write, while every other write in the same session raised 3960.
/// </summary>
public class snapshot_document_write_conflict_tests : OneOffConfigurationsContext
{
    private const int SnapshotUpdateConflict = 3960;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public class Note
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class VersionedNote : IVersioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public Guid Version { get; set; }
    }

    public class RevisionedNote : IRevisioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public int Version { get; set; }
    }

    public class TenantedNote
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts => opts.Schema.For<TenantedNote>().MultiTenanted());
        await base.InitializeAsync();
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // A Snapshot transaction cannot see a table created after it began, so make these now.
        await using var seed = theStore.LightweightSession();
        seed.Store(new Note { Id = Guid.NewGuid() });
        seed.Store(new VersionedNote { Id = Guid.NewGuid() });
        seed.Store(new RevisionedNote { Id = Guid.NewGuid() });
        await seed.SaveChangesAsync(Ct);
    }

    public const string Lightweight = nameof(Lightweight);
    public const string IdentityMap = nameof(IdentityMap);

    private static SessionOptions AtSnapshot => new() { IsolationLevel = IsolationLevel.Snapshot };

    private IDocumentSession OpenAtSnapshot(string kind)
        => kind == Lightweight ? theStore.LightweightSession(AtSnapshot) : theStore.IdentitySession(AtSnapshot);

    private async Task SeedAsync(Note note)
    {
        await using var seed = theStore.LightweightSession();
        seed.Store(note);
        await seed.SaveChangesAsync(Ct);
    }

    private async Task WriteConcurrentlyAsync(Guid id, string name = "theirs")
    {
        await using var writer = theStore.LightweightSession();
        writer.Store(new Note { Id = id, Name = name });
        await writer.SaveChangesAsync(Ct);
    }

    private async Task<string?> StoredNameAsync(Guid id)
    {
        await using var query = theStore.QuerySession();
        return (await query.LoadAsync<Note>(id, Ct))?.Name;
    }

    [Theory]
    [InlineData(Lightweight)]
    [InlineData(IdentityMap)]
    public async Task store_raises_a_conflict_over_a_concurrent_update(string kind)
    {
        var id = Guid.NewGuid();
        await SeedAsync(new Note { Id = id, Name = "original" });

        await using var session = OpenAtSnapshot(kind);
        var mine = await session.LoadAsync<Note>(id, Ct);

        await WriteConcurrentlyAsync(id);

        mine!.Name = "mine";
        session.Store(mine);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        (await StoredNameAsync(id)).ShouldBe("theirs");
    }

    [Theory]
    [InlineData(Lightweight)]
    [InlineData(IdentityMap)]
    public async Task update_raises_a_conflict_over_a_concurrent_update(string kind)
    {
        var id = Guid.NewGuid();
        await SeedAsync(new Note { Id = id, Name = "original" });

        await using var session = OpenAtSnapshot(kind);
        var mine = await session.LoadAsync<Note>(id, Ct);

        await WriteConcurrentlyAsync(id);

        mine!.Name = "mine";
        session.Update(mine);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        (await StoredNameAsync(id)).ShouldBe("theirs");
    }

    [Fact]
    public async Task store_raises_a_conflict_over_a_row_inserted_since_the_read()
    {
        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession(AtSnapshot);
        (await session.LoadAsync<Note>(id, Ct)).ShouldBeNull();

        await WriteConcurrentlyAsync(id);

        session.Store(new Note { Id = id, Name = "mine" });

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        (await StoredNameAsync(id)).ShouldBe("theirs");
    }

    [Fact]
    public async Task store_raises_a_conflict_rather_than_resurrecting_a_deleted_row()
    {
        var id = Guid.NewGuid();
        await SeedAsync(new Note { Id = id, Name = "original" });

        await using var session = theStore.LightweightSession(AtSnapshot);
        var mine = await session.LoadAsync<Note>(id, Ct);

        await using (var deleter = theStore.LightweightSession())
        {
            deleter.Delete<Note>(id);
            await deleter.SaveChangesAsync(Ct);
        }

        mine!.Name = "mine";
        session.Store(mine);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        (await StoredNameAsync(id)).ShouldBeNull();
    }

    [Fact]
    public async Task a_write_through_a_tenant_scope_raises_the_conflict_too()
    {
        const string tenant = "tenant_a";
        var id = Guid.NewGuid();

        await using (var seed = theStore.LightweightSession(new SessionOptions { TenantId = tenant }))
        {
            seed.Store(new TenantedNote { Id = id, Name = "original" });
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession(AtSnapshot);
        var scoped = session.ForTenant(tenant);
        var mine = await scoped.LoadAsync<TenantedNote>(id, Ct);

        await using (var writer = theStore.LightweightSession(new SessionOptions { TenantId = tenant }))
        {
            writer.Store(new TenantedNote { Id = id, Name = "theirs" });
            await writer.SaveChangesAsync(Ct);
        }

        mine!.Name = "mine";
        scoped.Store(mine);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);
    }

    [Fact]
    public async Task a_conflict_finishes_the_session_like_any_other()
    {
        var id = Guid.NewGuid();
        await SeedAsync(new Note { Id = id, Name = "original" });

        await using var session = theStore.LightweightSession(AtSnapshot);
        var mine = await session.LoadAsync<Note>(id, Ct);

        await WriteConcurrentlyAsync(id);

        session.Store(mine!);
        await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task with_nothing_concurrent_store_and_update_work_at_snapshot()
    {
        var id = Guid.NewGuid();
        await SeedAsync(new Note { Id = id, Name = "original" });

        await using var session = theStore.LightweightSession(AtSnapshot);

        var mine = await session.LoadAsync<Note>(id, Ct);
        mine!.Name = "updated";
        session.Update(mine);

        var added = new Note { Id = Guid.NewGuid(), Name = "added" };
        session.Store(added);

        await session.SaveChangesAsync(Ct);

        (await StoredNameAsync(id)).ShouldBe("updated");
        (await StoredNameAsync(added.Id)).ShouldBe("added");
    }

    /// <summary>
    ///     An insert is not lost by the hint it keeps: it reads the row the snapshot cannot see and
    ///     reports the collision by name.
    /// </summary>
    [Fact]
    public async Task insert_still_names_the_collision()
    {
        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession(AtSnapshot);
        (await session.LoadAsync<Note>(id, Ct)).ShouldBeNull();

        await WriteConcurrentlyAsync(id);

        session.Insert(new Note { Id = id, Name = "mine" });

        await Should.ThrowAsync<DocumentAlreadyExistsException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     A guarded document keeps its guard, and the guard can only fire on the current row, so its
    ///     write keeps reading one. Otherwise this would become a 3960 in place of the exception the
    ///     guard promises.
    /// </summary>
    [Fact]
    public async Task a_revisioned_document_still_reports_its_own_concurrency_exception()
    {
        var id = Guid.NewGuid();
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(new RevisionedNote { Id = id, Name = "original" });
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession(AtSnapshot);
        var mine = await session.LoadAsync<RevisionedNote>(id, Ct);

        await using (var writer = theStore.LightweightSession())
        {
            var theirs = await writer.LoadAsync<RevisionedNote>(id, Ct);
            theirs!.Name = "theirs";

            // The guard is strictly-greater (#559), so a revision has to move for a write to land
            theirs.Version++;
            writer.Store(theirs);
            await writer.SaveChangesAsync(Ct);
        }

        mine!.Name = "mine";
        mine.Version++;
        session.Store(mine);

        await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     The other half of the design: <c>HOLDLOCK</c> is what makes a first write safe at
    ///     <c>ReadCommitted</c>, and that level must not lose it. Measured raw, the same race without the
    ///     hint fails a third of its writers with 2627.
    /// </summary>
    [Fact]
    public async Task concurrent_first_writes_of_one_id_all_succeed_at_read_committed()
    {
        for (var round = 0; round < 25; round++)
        {
            var id = Guid.NewGuid();

            await Task.WhenAll(Enumerable.Range(0, 8).Select(n => Task.Run(async () =>
            {
                await using var session = theStore.LightweightSession();
                session.Store(new Note { Id = id, Name = $"writer {n}" });
                await session.SaveChangesAsync(Ct);
            }, Ct)));

            (await StoredNameAsync(id)).ShouldNotBeNull();
        }
    }
}
