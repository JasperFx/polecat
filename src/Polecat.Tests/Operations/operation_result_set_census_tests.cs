using JasperFx;
using JasperFx.Events;
using JasperFx.Metadata;
using Microsoft.Data.SqlClient;
using Polecat.Events.Operations;
using Polecat.Events.Projections;
using Polecat.Events.Protected;
using Polecat.Events.Storage;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Patching;
using Polecat.Projections.Flattened;
using Polecat.Storage.ClosedShape;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Tests.Operations;

/// <summary>
///     #831. <see cref="OperationResultSets" /> matches a batch's result sets to its operations by what
///     each operation declares, so a declaration that is wrong misaligns every read behind it. This
///     class measures the declaration rather than trusting it: each operation shape goes into a real
///     batch ahead of a sentinel, the result sets the reader walks through before the sentinel are
///     counted, and the count has to agree with <see cref="OperationResultSets.ReturnsResultSet" />.
/// </summary>
/// <remarks>
///     <para>
///         What this has measured, on SQL Server 2025. Every closed-shape write (upsert, insert, update,
///         overwrite, in all three concurrency modes) contributes exactly one result set, and that
///         holds for the write a guard refused too: the set is empty, not absent, which is what makes
///         "no row" a usable conflict signal. The full-text <c>@pc_output</c> variant contributes one
///         as well, through its trailing <c>SELECT</c> rather than the <c>OUTPUT</c>, and its trigger
///         contributes nothing. Everything else — deletes, patches, tombstones, flat-table
///         <c>MERGE</c>s, progression — contributes none, including a <c>DELETE</c> that fires the
///         full-text trigger.
///     </para>
///     <para>
///         <c>QueueSqlCommand</c> is the one shape this cannot hold: what it contributes depends on the
///         text it was given. It is declared as returning nothing, which is what its documentation
///         requires of the caller.
///     </para>
/// </remarks>
public class operation_result_set_census_tests : OneOffConfigurationsContext
{
    private const int Sentinel = 424242;
    private const string NaturalKeyTable = "pc_census_natural_key";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string Schema => theStore.Options.DatabaseSchemaName;

    /// <summary>
    ///     The sweep that keeps the census honest. An operation with no marker is assumed to return a
    ///     result set, so a new DML operation added without one would misalign every versioned write
    ///     behind it — the exact bug, reintroduced by omission. Every operation type in the assembly
    ///     has to either carry <see cref="NoDataReturnedCall" /> or be named here as one whose SQL
    ///     really does return rows.
    /// </summary>
    [Fact]
    public void every_operation_type_declares_whether_it_returns_a_result_set()
    {
        var returnsResultSet = new[] { typeof(PolecatQuickAppendEventsOperation) };

        var undeclared = typeof(DocumentStore).Assembly
            .GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false })
            .Where(t => typeof(Weasel.Core.IStorageOperation).IsAssignableFrom(t))
            .Where(t => !typeof(IStorageOperationAdapter).IsAssignableFrom(t))
            .Where(t => !typeof(NoDataReturnedCall).IsAssignableFrom(t))
            .Except(returnsResultSet)
            .Select(t => t.FullName)
            .ToArray();

        undeclared.ShouldBeEmpty(
            "these operations carry no NoDataReturnedCall marker, so the result-set walk gives each one a "
            + "turn it may not own. Measure the SQL in the census below, then either add the marker or "
            + "list the type here: " + string.Join(", ", undeclared));
    }

    [Fact]
    public async Task every_operation_shape_contributes_exactly_what_it_declares()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<Searchable>().FullTextIndex(x => x.Body);
            opts.Schema.For<SearchableVersioned>().FullTextIndex(x => x.Body);
            opts.Schema.For<SearchableRevisioned>().FullTextIndex(x => x.Body);
            opts.Events.RegisterTagType<CensusTag>("census_tag");
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EnsureNaturalKeyTableAsync();

        var plain = new Plain { Id = Guid.NewGuid(), Name = "seed" };
        var versioned = new Versioned { Id = Guid.NewGuid(), Name = "seed" };
        var revisioned = new Revisioned { Id = Guid.NewGuid(), Name = "seed" };
        var searchable = new Searchable { Id = Guid.NewGuid(), Name = "seed", Body = "alpha beta" };
        var searchableVersioned = new SearchableVersioned { Id = Guid.NewGuid(), Body = "alpha beta" };
        var searchableRevisioned = new SearchableRevisioned { Id = Guid.NewGuid(), Body = "alpha beta" };
        var soft = new SoftDoc { Id = Guid.NewGuid(), Name = "seed" };
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(plain);
            session.Store(versioned);
            session.Store(revisioned);
            session.Store(searchable);
            session.Store(searchableVersioned);
            session.Store(searchableRevisioned);
            session.Store(soft);
            session.Events.StartStream(streamId, new CensusHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        IEvent seeded;
        await using (var query = theStore.QuerySession())
        {
            seeded = (await query.Events.FetchStreamAsync(streamId, token: Ct)).Single();
        }

        var flat = new CensusFlatProjection(Schema);
        var events = theStore.Events;
        var disagreements = new List<string>();

        async Task check(string name, DocumentSessionBase session, Weasel.Storage.IStorageOperation operation)
        {
            var measured = await resultSetsContributedByAsync(session, operation);
            var declared = OperationResultSets.ReturnsResultSet(operation) ? 1 : 0;
            if (measured != declared)
            {
                disagreements.Add($"{name}: declared {declared}, SQL Server returned {measured}");
            }
        }

        // Queued the way the application queues it, so what gets measured is the real operation,
        // adapter included.
        async Task queued(string name, Action<DocumentSessionBase> queue)
        {
            await using var session = (DocumentSessionBase)theStore.LightweightSession();
            queue(session);
            await check(name, session, session.WorkTracker.Operations.ShouldHaveSingleItem());
        }

        // The shapes no session method queues directly: the projected writes the bulk and daemon paths
        // use, and the event operations those paths and the protected-event API build themselves.
        async Task built(string name, Func<DocumentSessionBase, Weasel.Storage.IStorageOperation> build)
        {
            await using var session = (DocumentSessionBase)theStore.LightweightSession();
            await check(name, session, build(session));
        }

        // Upsert / insert / update, unversioned. A refused write contributes an empty set, not no set.
        await queued("Store new", s => s.Store(new Plain { Id = Guid.NewGuid() }));
        await queued("Store existing", s => s.Store(plain));
        await queued("Insert new", s => s.Insert(new Plain { Id = Guid.NewGuid() }));
        await queued("Insert duplicate id", s => s.Insert(new Plain { Id = plain.Id }));
        await queued("Update existing", s => s.Update(plain));
        await queued("Update missing", s => s.Update(new Plain { Id = Guid.NewGuid() }));

        // Optimistic
        await queued("Store IVersioned new", s => s.Store(new Versioned { Id = Guid.NewGuid() }));
        await queued("Store IVersioned existing", s => s.Store(versioned));
        await queued("Store IVersioned stale",
            s => s.Store(new Versioned { Id = versioned.Id, Version = Guid.NewGuid() }));
        await queued("Update IVersioned", s => s.Update(versioned));
        await queued("Insert IVersioned", s => s.Insert(new Versioned { Id = Guid.NewGuid() }));

        // Numeric
        await queued("Store IRevisioned new", s => s.Store(new Revisioned { Id = Guid.NewGuid() }));
        await queued("UpdateRevision valid", s => s.UpdateRevision(revisioned, revisioned.Version + 1));
        await queued("UpdateRevision stale", s => s.UpdateRevision(revisioned, revisioned.Version));
        await queued("Insert IRevisioned", s => s.Insert(new Revisioned { Id = Guid.NewGuid() }));

        // The full-text @pc_output variant, where the trigger forces OUTPUT ... INTO plus a SELECT.
        await queued("full-text Store new", s => s.Store(new Searchable { Id = Guid.NewGuid(), Body = "x y" }));
        await queued("full-text Store existing", s => s.Store(searchable));
        await queued("full-text Insert duplicate id", s => s.Insert(new Searchable { Id = searchable.Id }));
        await queued("full-text Update missing", s => s.Update(new Searchable { Id = Guid.NewGuid() }));
        await queued("full-text Store IVersioned", s => s.Store(searchableVersioned));
        await queued("full-text Store IVersioned stale",
            s => s.Store(new SearchableVersioned { Id = searchableVersioned.Id, Version = Guid.NewGuid() }));
        await queued("full-text UpdateRevision valid",
            s => s.UpdateRevision(searchableRevisioned, searchableRevisioned.Version + 1));
        await queued("full-text UpdateRevision stale",
            s => s.UpdateRevision(searchableRevisioned, searchableRevisioned.Version));

        // Bulk and daemon writes
        await built("OverwriteProjected", s => Storage<Plain>(s).OverwriteProjected(plain, s.TenantId));
        await built("OverwriteProjected IVersioned",
            s => Storage<Versioned>(s).OverwriteProjected(versioned, s.TenantId));
        await built("OverwriteProjected IRevisioned",
            s => Storage<Revisioned>(s).OverwriteProjected(revisioned, s.TenantId));
        await built("full-text OverwriteProjected",
            s => Storage<SearchableVersioned>(s).OverwriteProjected(searchableVersioned, s.TenantId));
        await built("InsertProjected",
            s => Storage<Plain>(s).InsertProjected(new Plain { Id = Guid.NewGuid() }, s.TenantId));
        await built("UpsertProjected IVersioned", s => Storage<Versioned>(s).UpsertProjected(versioned, s.TenantId));

        // Deletes, of a row that is there and of one that is not
        await queued("Delete by id, no such row", s => s.Delete<Plain>(Guid.NewGuid()));
        await queued("Delete by id", s => s.Delete<Plain>(plain.Id));
        await queued("Delete document", s => s.Delete(plain));
        await queued("HardDelete", s => s.HardDelete<Plain>(plain.Id));
        await queued("DeleteWhere", s => s.DeleteWhere<Plain>(x => x.Name == "seed"));
        await queued("HardDeleteWhere", s => s.HardDeleteWhere<Plain>(x => x.Name == "seed"));
        await queued("soft Delete", s => s.Delete<SoftDoc>(soft.Id));
        await queued("soft DeleteWhere", s => s.DeleteWhere<SoftDoc>(x => x.Name == "seed"));
        await queued("UndoDeleteWhere", s => s.UndoDeleteWhere<SoftDoc>(x => x.Name == "seed"));
        await built("UnDelete by id", s => new UnDeleteByIdOperation(
            ((IPolecatDeletionStorage)((Weasel.Storage.IStorageSession)s).StorageFor<SoftDoc>()).UndeleteFragment,
            false, soft.Id, s.TenantId, typeof(SoftDoc)));

        // The trigger fires on these and must not add a result set of its own.
        await queued("full-text Delete", s => s.Delete<Searchable>(searchable.Id));
        await queued("full-text DeleteWhere", s => s.DeleteWhere<Searchable>(x => x.Name == "seed"));
        await queued("full-text Patch", s => s.Patch<Searchable>(searchable.Id).Set(x => x.Body, "gamma"));

        await queued("Patch by id", s => s.Patch<Plain>(plain.Id).Set(x => x.Name, "patched"));
        await queued("Patch where", s => s.Patch<Plain>(x => x.Name == "seed").Set(x => x.Name, "patched"));
        await queued("QueueSqlCommand DML", s => s.QueueSqlCommand(
            $"UPDATE [{Schema}].[pc_doc_plain] SET last_modified = last_modified WHERE 1 = 0"));

        // Events
        await built("ArchiveStream", s => events.ArchiveStreamOperation(streamId, s.TenantId, true));
        await built("TombstoneStream", s => events.TombstoneStreamOperation(streamId, s.TenantId));
        await built("UpdateProgress merge", _ => events.UpdateProgressOperation("Census:All", 1, upsert: true));
        await built("UpdateProgress update", _ => events.UpdateProgressOperation("Census:All", 2, upsert: false));

        await queued("AssignTagWhere",
            s => s.Events.AssignTagWhere(e => e.StreamId == streamId, new CensusTag(Guid.NewGuid())));

        await built("OverwriteEvent", s => new OverwriteEventOperation(events, seeded, "{}", null, null, s.TenantId));
        await built("ReplaceEvent", s => new ReplaceEventOperation(events, seeded.Sequence, "{}", null,
            seeded.EventTypeName, seeded.DotNetTypeName, s.TenantId));
        await built("SetCompactedVersion", s => new SetCompactedVersionOperation(events, streamId, 1, s.TenantId));
        await built("DeleteEvents", s => new DeleteEventsOperation(events, [seeded.Sequence], s.TenantId));

        var naturalKeys = $"[{Schema}].[{NaturalKeyTable}]";
        await built("NaturalKeyUpsert", _ => new NaturalKeyUpsertOperation(naturalKeys, "key", Guid.NewGuid(),
            isGuidStream: true, typeof(Plain)));
        await built("NaturalKeyRetire", _ => new NaturalKeyRetireOperation(naturalKeys, "key", Guid.NewGuid(),
            isGuidStream: true));
        await built("NaturalKeyArchive", _ => new NaturalKeyArchiveOperation(naturalKeys, Guid.NewGuid(),
            isGuidStream: true));

        await using (var session = (DocumentSessionBase)theStore.LightweightSession())
        {
            // Registering the projection would run it on every save above. Applying it to a stream the
            // session never flushes yields just its two statements.
            var stream = session.Events.Append(streamId, new CensusHappened(2), new CensusErased());
            await flat.ApplyAsync(session, [stream], Ct);

            var operations = session.WorkTracker.Operations;
            operations.Count.ShouldBe(2);
            await check("flat table MERGE", session, operations[0]);
            await check("flat table DELETE", session, operations[1]);
        }

        // Appends read back IEvent.Sequence: one set however many statements the command holds, for a
        // new stream's INSERT and an existing stream's UPDATE, tagged or not.
        await built("Append new stream", s =>
        {
            s.Events.StartStream(Guid.NewGuid(), new CensusHappened(1), new CensusHappened(2));
            return BuildAppend(s);
        });
        await built("Append existing stream", s =>
        {
            s.Events.Append(streamId, new CensusHappened(3));
            return BuildAppend(s);
        });
        await built("Append tagged", s =>
        {
            var tagged = theStore.Events.BuildEvent(new CensusHappened(4));
            tagged.AddTag(new CensusTag(Guid.NewGuid()));
            s.Events.Append(streamId, tagged);
            return BuildAppend(s);
        });

        disagreements.ShouldBeEmpty();
    }

    private static Weasel.Storage.IDocumentStorage<T> Storage<T>(DocumentSessionBase session) where T : notnull
        => (Weasel.Storage.IDocumentStorage<T>)((Weasel.Storage.IStorageSession)session).StorageFor<T>();

    private static Weasel.Storage.IStorageOperation BuildAppend(DocumentSessionBase session)
        => session.BuildRaisedEventAppendsAsync(session.WorkTracker.Streams.ToList(), Ct).GetAwaiter().GetResult()
            .ShouldHaveSingleItem();

    /// <summary>
    ///     Runs the operation ahead of a sentinel, inside a transaction it rolls back so one measurement
    ///     cannot disturb the next. The count of result sets walked before the sentinel IS what the
    ///     operation contributed.
    /// </summary>
    private async Task<int> resultSetsContributedByAsync(DocumentSessionBase session,
        Weasel.Storage.IStorageOperation operation)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(Ct);

        await using var batch = new SqlBatch(conn) { Transaction = tx };
        var builder = new BatchBuilder(batch);
        StorageOperationExecution.Configure(operation, builder, session);
        builder.StartNewCommand();
        builder.Append($"SELECT {Sentinel} AS marker;");
        builder.Compile();

        var contributed = 0;
        await using (var reader = await batch.ExecuteReaderAsync(Ct))
        {
            while (reader.FieldCount == 0 || reader.GetName(0) != "marker")
            {
                (await reader.NextResultAsync(Ct)).ShouldBeTrue("the sentinel never appeared");
                contributed++;
            }
        }

        await tx.RollbackAsync(Ct);
        return contributed;
    }

    private async Task EnsureNaturalKeyTableAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE [{Schema}].[{NaturalKeyTable}] (
                natural_key_value nvarchar(200) NOT NULL PRIMARY KEY,
                stream_id uniqueidentifier NOT NULL,
                is_archived bit NOT NULL DEFAULT 0);
            """;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public record CensusHappened(int Number);

    public record CensusErased;

    public record CensusTag(Guid Value);

    public class Plain
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class Versioned : IVersioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid Version { get; set; }
    }

    public class Revisioned : IRevisioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public class Searchable
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    public class SearchableVersioned : IVersioned
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public Guid Version { get; set; }
    }

    public class SearchableRevisioned : IRevisioned
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public class SoftDoc : ISoftDeleted
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Deleted { get; set; }
        public DateTimeOffset? DeletedAt { get; set; }
    }

    private class CensusFlatProjection : FlatTableProjection
    {
        public CensusFlatProjection(string schema) : base("census_flat", schema)
        {
            Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();

            Project<CensusHappened>(map => map.Map(x => x.Number, "number"));
            Delete<CensusErased>();
        }
    }
}
