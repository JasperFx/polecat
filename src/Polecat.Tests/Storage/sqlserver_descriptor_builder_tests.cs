using System.Data.Common;
using Microsoft.Data.SqlClient;
using OperationRole = Weasel.Core.OperationRole;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.Core.Identity;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Tests.Storage;

/// <summary>
///     End-to-end verification of SqlServerDocumentStorageDescriptorBuilder (#273 phase D
///     slice 1) through the SHARED closed-shape runtime pieces: descriptors executed by the
///     shared write operations (Weasel.Storage 9.13.0 INC-4), bound through a real Polecat
///     IStorageSession, against real pc tables, read back via the shared selectors — all
///     without Polecat's bespoke operations touching the rows.
/// </summary>
public class sqlserver_descriptor_builder_tests : OneOffConfigurationsContext
{
    private DocumentStorageDescriptor<TDoc, Guid> descriptorFor<TDoc>() where TDoc : notnull
    {
        var provider = theStore.Options.Providers.GetProvider(typeof(TDoc));
        var identification = new SequentialGuidIdentification<TDoc>(typeof(TDoc).GetProperty("Id")!);
        return SqlServerDocumentStorageDescriptorBuilder.Build<TDoc, Guid>(
            provider.Mapping, identification, theStore.Options);
    }

    private async Task<int> executeAsync(Weasel.Storage.IStorageOperation operation, IStorageSession session)
    {
        var builder = new BatchBuilder { TenantId = session.TenantId };
        operation.ConfigureCommand(builder, session);
        var batch = builder.Compile();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        batch.Connection = conn;

        var exceptions = new List<Exception>();
        await using (var reader = await batch.ExecuteReaderAsync())
        {
            await operation.PostprocessAsync(reader, exceptions, CancellationToken.None);
        }

        foreach (var ex in exceptions)
        {
            throw ex;
        }

        return exceptions.Count;
    }

    private async Task<TDoc?> loadViaSharedSelectorAsync<TDoc>(
        DocumentStorageDescriptor<TDoc, Guid> descriptor, Guid id, IStorageSession session)
        where TDoc : notnull
    {
        var readColumns = new List<string> { "id", "data" };
        readColumns.AddRange(descriptor.ReadBinders.Select(b => b.ColumnName));

        var command = descriptor.Dialect.BuildLoadCommand(
            $"SELECT {string.Join(", ", readColumns)} FROM {descriptor.TableName} WHERE id = @id",
            id, tenant: null);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        command.Connection = (SqlConnection)conn;

        var selector = new TestLightweightSelector<TDoc>(session, descriptor);
        await using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return default;
        }

        return await selector.ResolveAsync(reader, CancellationToken.None);
    }

    private sealed class TestLightweightSelector<TDoc> : UnversionedClosedShapeLightweightSelector<TDoc, Guid>
        where TDoc : notnull
    {
        public TestLightweightSelector(IStorageSession session, DocumentStorageDescriptor<TDoc, Guid> descriptor)
            : base(session, descriptor)
        {
        }

        protected override TDoc ReadDocument(DbDataReader reader)
            => _serializer.FromJson<TDoc>(reader, DataColumn);

        protected override async ValueTask<TDoc> ReadDocumentAsync(DbDataReader reader, CancellationToken token)
            => await _serializer.FromJsonAsync<TDoc>(reader, DataColumn, token);
    }

    [Fact]
    public void builder_produces_merge_sql_with_slot_aligned_parameters()
    {
        // Force table + provider creation
        theStore.Options.Providers.GetProvider(typeof(Target)).ShouldNotBeNull();

        var descriptor = descriptorFor<Target>();

        descriptor.ConcurrencyMode.ShouldBe(ConcurrencyMode.Off);
        descriptor.IsConjoined.ShouldBeFalse();

        // #234: single-tenant tables have no tenant_id column, so there is no tenant write binder
        // and the MERGE never references tenant_id.
        descriptor.ClientSideWriteBinders.Select(b => b.ColumnName)
            .ShouldNotContain("tenant_id");
        descriptor.UpsertSql.ShouldNotContain("tenant_id");

        // ? slots in the USING row = id + data + client-side binders
        var expectedSlots = 2 + descriptor.ClientSideWriteBinders.Length;
        descriptor.UpsertSql.Count(c => c == '?').ShouldBe(expectedSlots);

        descriptor.UpsertSql.ShouldContain("MERGE");
        descriptor.UpsertSql.ShouldContain("WHEN MATCHED THEN UPDATE");
        descriptor.UpsertSql.ShouldContain("WHEN NOT MATCHED THEN INSERT");
        descriptor.UpsertSql.ShouldContain("SYSDATETIMEOFFSET()");
        descriptor.UpsertSql.ShouldContain("version = t.version + 1");
        descriptor.InsertSql.ShouldNotContain("WHEN MATCHED");
    }

    [Fact]
    public async Task shared_upsert_operation_round_trips_through_polecat_session()
    {
        await using var bootstrap = theStore.LightweightSession();
        bootstrap.Store(new Target { Number = 1 }); // force table creation
        await bootstrap.SaveChangesAsync();

        var descriptor = descriptorFor<Target>();
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        var doc = new Target { Id = Guid.NewGuid(), Number = 42, Color = "teal" };
        var upsert = new UnversionedClosedShapeUpsertOperation<Target, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert);
        await executeAsync(upsert, session);

        // Update the same row through a second shared upsert
        doc.Number = 43;
        var second = new UnversionedClosedShapeUpsertOperation<Target, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert);
        await executeAsync(second, session);

        var loaded = await loadViaSharedSelectorAsync(descriptor, doc.Id, session);
        loaded.ShouldNotBeNull();
        loaded.Number.ShouldBe(43);
        loaded.Color.ShouldBe("teal");

        // And Polecat's own bespoke pipeline sees the same row
        await using var check = theStore.QuerySession();
        var viaPolecat = await check.LoadAsync<Target>(doc.Id);
        viaPolecat.ShouldNotBeNull();
        viaPolecat.Number.ShouldBe(43);
    }

    [Fact]
    public async Task optimistic_descriptor_enforces_guid_version_guard()
    {
        await using var bootstrap = theStore.LightweightSession();
        bootstrap.Store(new VersionedDoc { Name = "seed" }); // force table creation
        await bootstrap.SaveChangesAsync();

        var descriptor = descriptorFor<VersionedDoc>();
        descriptor.ConcurrencyMode.ShouldBe(ConcurrencyMode.Optimistic);
        descriptor.VersionBinder.ShouldNotBeNull();

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "first" };
        var versions = new Dictionary<Guid, Guid>();

        // Initial save: no expectation, row doesn't exist -> INSERT branch fires
        var insert = new OptimisticClosedShapeUpsertOperation<VersionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, versions);
        await executeAsync(insert, session);
        versions.ContainsKey(doc.Id).ShouldBeTrue();

        // Update with the correct expected version succeeds
        doc.Name = "second";
        var goodUpdate = new OptimisticClosedShapeUpsertOperation<VersionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, versions);
        await executeAsync(goodUpdate, session);

        // Update with a stale expected version raises ConcurrencyException
        var stale = new Dictionary<Guid, Guid> { [doc.Id] = Guid.NewGuid() };
        var badUpdate = new OptimisticClosedShapeUpsertOperation<VersionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, stale);
        await Should.ThrowAsync<JasperFx.ConcurrencyException>(() => executeAsync(badUpdate, session));
    }

    // ---- Slice 2: numeric revisions, soft delete, hierarchy ----

    [Fact]
    public async Task numeric_descriptor_auto_increments_and_guards_revisions()
    {
        await using var bootstrap = theStore.LightweightSession();
        bootstrap.Store(new RevisionedDoc { Name = "seed" }); // force table creation
        await bootstrap.SaveChangesAsync();

        var descriptor = descriptorFor<RevisionedDoc>();
        descriptor.ConcurrencyMode.ShouldBe(ConcurrencyMode.Numeric);
        descriptor.RevisionBinder.ShouldNotBeNull();

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "r1" };
        var revisions = new Dictionary<Guid, long>();

        // Auto-increment: Revision = 0 -> initial revision 1, written back to the document
        var first = new NumericClosedShapeUpsertOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, revisions) { Revision = 0 };
        await executeAsync(first, session);
        doc.Version.ShouldBe(1);
        revisions[doc.Id].ShouldBe(1);

        // Auto-increment again -> 2
        doc.Name = "r2";
        var second = new NumericClosedShapeUpsertOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, revisions) { Revision = 0 };
        await executeAsync(second, session);
        doc.Version.ShouldBe(2);

        // POLECAT numeric semantics (#273 E2c): the explicit revision is an EQUALITY
        // expectation against the current version (bespoke-pipeline parity), and version
        // always auto-increments — unlike Marten's explicit-greater rule.
        var stale = new NumericClosedShapeUpsertOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, revisions) { Revision = 1 };
        await Should.ThrowAsync<JasperFx.ConcurrencyException>(() => executeAsync(stale, session));

        // A jump past the current version is also a mismatch
        var jump = new NumericClosedShapeUpsertOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, revisions) { Revision = 10 };
        await Should.ThrowAsync<JasperFx.ConcurrencyException>(() => executeAsync(jump, session));

        // Matching the current version succeeds and increments
        var match = new NumericClosedShapeUpsertOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert, revisions) { Revision = 2 };
        await executeAsync(match, session);
        doc.Version.ShouldBe(3);
    }

    [Fact]
    public async Task soft_delete_descriptor_resave_undeletes()
    {
        await using var bootstrap = theStore.LightweightSession();
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "keep", Number = 5 };
        bootstrap.Store(doc);
        await bootstrap.SaveChangesAsync();

        // Soft-delete through Polecat's bespoke pipeline
        await using (var deleter = theStore.LightweightSession())
        {
            deleter.Delete<SoftDeletedDoc>(doc.Id);
            await deleter.SaveChangesAsync();
        }

        var descriptor = descriptorFor<SoftDeletedDoc>();
        descriptor.WriteBinders.Select(b => b.ColumnName).ShouldContain("is_deleted");
        descriptor.WriteBinders.Select(b => b.ColumnName).ShouldContain("deleted_at");

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        // Re-save via the shared upsert: writes is_deleted = 0 / deleted_at = NULL -> undelete
        var resave = new UnversionedClosedShapeUpsertOperation<SoftDeletedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert);
        await executeAsync(resave, session);

        await using var check = theStore.QuerySession();
        var loaded = await check.LoadAsync<SoftDeletedDoc>(doc.Id);
        loaded.ShouldNotBeNull(); // visible again — undeleted
    }

    [Fact]
    public void hierarchical_descriptor_carries_doc_type_dispatch()
    {
        ConfigureStore(opts => opts.Schema.For<Target>().AddSubClass<SubTarget>());

        var descriptor = descriptorFor<Target>();

        descriptor.ResolveDocumentType.ShouldNotBeNull();
        descriptor.DocTypeReadIndex.ShouldBe(0); // doc_type reads first
        descriptor.WriteBinders[0].ColumnName.ShouldBe("doc_type");
        descriptor.UpsertSql.ShouldContain("doc_type");
        var mapping = theStore.Options.Providers.GetProvider(typeof(Target)).Mapping;
        descriptor.ResolveDocumentType!(mapping.AliasFor(typeof(SubTarget))).ShouldBe(typeof(SubTarget));
    }

    public class SubTarget : Target
    {
        public string Extra { get; set; } = string.Empty;
    }

    // Partition functions/schemes are database-global objects named from the table; a
    // dedicated doc type keeps this test's objects distinct from document_range_partitioning_tests.
    public class DescriptorMetricsSample
    {
        public Guid Id { get; set; }
        public DateTimeOffset BucketEnd { get; set; }
        public string Metric { get; set; } = string.Empty;
        public double Value { get; set; }
    }

    // ---- Slice 3: update-op slot order + partitioned documents ----

    [Fact]
    public async Task shared_update_operation_binds_the_update_slot_order()
    {
        // The update ops bind data -> binders -> id -> [tenant] -> [partition] -> guard,
        // NOT the upsert order — this test locks the UpdateSql slot layout.
        await using var bootstrap = theStore.LightweightSession();
        var doc = new Target { Id = Guid.NewGuid(), Number = 1, Color = "red" };
        bootstrap.Store(doc);
        await bootstrap.SaveChangesAsync();

        var descriptor = descriptorFor<Target>();
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        doc.Number = 99;
        var update = new UnversionedClosedShapeUpdateOperation<Target, Guid>(
            doc, doc.Id, session.TenantId, descriptor);
        await executeAsync(update, session);

        var loaded = await loadViaSharedSelectorAsync(descriptor, doc.Id, session);
        loaded.ShouldNotBeNull();
        loaded.Number.ShouldBe(99);

        // Updating a nonexistent document -> zero rows -> missing-document error
        var ghost = new Target { Id = Guid.NewGuid(), Number = 1 };
        var missing = new UnversionedClosedShapeUpdateOperation<Target, Guid>(
            ghost, ghost.Id, session.TenantId, descriptor);
        await Should.ThrowAsync<Exception>(() => executeAsync(missing, session));
    }

    [Fact]
    public async Task shared_numeric_update_operation_guards_and_increments()
    {
        await using var bootstrap = theStore.LightweightSession();
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "u1" };
        bootstrap.Store(doc);
        await bootstrap.SaveChangesAsync(); // bespoke pipeline writes revision 1

        var descriptor = descriptorFor<RevisionedDoc>();
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        doc.Name = "u2";
        var revisions = new Dictionary<Guid, long>();
        var update = new NumericClosedShapeUpdateOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, revisions) { Revision = 0 };
        await executeAsync(update, session);
        doc.Version.ShouldBe(2); // auto-incremented past the bespoke pipeline's 1

        var stale = new NumericClosedShapeUpdateOperation<RevisionedDoc, Guid>(
            doc, doc.Id, session.TenantId, descriptor, revisions) { Revision = 1 };
        await Should.ThrowAsync<JasperFx.ConcurrencyException>(() => executeAsync(stale, session));
    }

    [Fact]
    public async Task partitioned_descriptor_round_trips_and_updates_in_partition()
    {
        var jan = new DateTimeOffset(2026, 1, 31, 0, 0, 0, TimeSpan.Zero);
        var feb = new DateTimeOffset(2026, 2, 28, 0, 0, 0, TimeSpan.Zero);
        ConfigureStore(opts =>
            opts.Schema.For<DescriptorMetricsSample>().PartitionByRange(x => x.BucketEnd, jan, feb));

        await using var bootstrap = theStore.LightweightSession();
        bootstrap.Store(new DescriptorMetricsSample { BucketEnd = jan, Metric = "seed" });
        await bootstrap.SaveChangesAsync(); // force partitioned table creation

        var descriptor = descriptorFor<DescriptorMetricsSample>();
        descriptor.PartitionPkBinders.Length.ShouldBe(1);
        descriptor.PartitionPkBinders[0].ColumnName.ShouldBe("bucket_end");
        descriptor.UpsertSql.ShouldContain("t.bucket_end = s.bucket_end");
        descriptor.UpdateSql.ShouldContain("t.bucket_end = s.bucket_end_pk");

        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        var doc = new DescriptorMetricsSample
        {
            Id = Guid.NewGuid(), BucketEnd = jan, Metric = "cpu", Value = 0.5
        };
        var upsert = new UnversionedClosedShapeUpsertOperation<DescriptorMetricsSample, Guid>(
            doc, doc.Id, session.TenantId, descriptor, OperationRole.Upsert);
        await executeAsync(upsert, session);

        // Shared update targets the same partition row
        doc.Value = 0.9;
        var update = new UnversionedClosedShapeUpdateOperation<DescriptorMetricsSample, Guid>(
            doc, doc.Id, session.TenantId, descriptor);
        await executeAsync(update, session);

        await using var check = theStore.QuerySession();
        var loaded = await check.LoadAsync<DescriptorMetricsSample>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.Value.ShouldBe(0.9);
    }

    [Fact]
    public async Task insert_only_descriptor_sql_signals_id_collision_with_zero_rows()
    {
        await using var bootstrap = theStore.LightweightSession();
        bootstrap.Store(new Target { Number = 1 });
        await bootstrap.SaveChangesAsync();

        var descriptor = descriptorFor<Target>();
        await using var raw = theStore.LightweightSession();
        var session = (IStorageSession)raw;

        var doc = new Target { Id = Guid.NewGuid(), Number = 7 };
        var insert = new UnversionedClosedShapeInsertOperation<Target, Guid>(
            doc, doc.Id, session.TenantId, descriptor);
        await executeAsync(insert, session);

        // Second insert with the same id: zero rows from OUTPUT -> the shared operation
        // surfaces a DocumentAlreadyExistsException (or reports via exceptions list)
        var duplicate = new UnversionedClosedShapeInsertOperation<Target, Guid>(
            doc, doc.Id, session.TenantId, descriptor);
        await Should.ThrowAsync<Exception>(() => executeAsync(duplicate, session));
    }
}
