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

        // Single-tenant: tenant travels as an ordinary client-side binder
        descriptor.ClientSideWriteBinders.Select(b => b.ColumnName)
            .ShouldContain("tenant_id");

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
