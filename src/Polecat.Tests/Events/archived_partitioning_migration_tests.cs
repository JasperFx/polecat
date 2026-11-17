using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Events;

public record MigStudentId(Guid Value);
public record MigEnrolled(string Name);
public record MigProgressed(int Step);

/// <summary>
///     #1041: turning <c>UseArchivedStreamPartitioning</c> on for a store that already holds events.
///     The table has to be moved onto the partition scheme with every row kept, under the default
///     <c>AutoCreate.CreateOrUpdate</c>, and <c>AutoCreate.All</c> must not drop it to get there.
/// </summary>
public class archived_partitioning_migration_tests : IAsyncLifetime
{
    private const int StreamCount = 300;
    private const int EventsPerStream = 10;

    private readonly string _schema = "arch_mig_" + Guid.NewGuid().ToString("N")[..8];
    private readonly List<DocumentStore> _stores = new();

    private readonly List<string> _databases = new();

    // Where the assertions look: the test database, or one tenant's database under database-per-tenant
    private string _queryConnection = ConnectionSource.ConnectionString;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores) store.Dispose();

        foreach (var database in _databases) await dropDatabaseAsync(database);

        await TestSchema.DropSchemaTablesAsync(_schema);
        await TestSchema.DropSequencesAsync(_schema);
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"IF SCHEMA_ID('{_schema}') IS NOT NULL EXEC('DROP SCHEMA [{_schema}]')";
        await cmd.ExecuteNonQueryAsync();
    }

    private DocumentStore BuildStore(bool partitioned, AutoCreate autoCreate = AutoCreate.CreateOrUpdate,
        TenancyStyle tenancy = TenancyStyle.Single, Dictionary<string, string>? tenantDatabases = null)
    {
        var store = DocumentStore.For(opts =>
        {
            if (tenantDatabases == null)
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
            }
            else
            {
                opts.MultiTenantedDatabases(tenancy =>
                {
                    foreach (var (tenantId, connectionString) in tenantDatabases)
                    {
                        tenancy.AddTenant(tenantId, connectionString);
                    }
                });
            }

            opts.DatabaseSchemaName = _schema;
            opts.AutoCreateSchemaObjects = autoCreate;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = tenancy;
            opts.EventGraph.UseArchivedStreamPartitioning = partitioned;
            opts.Events.RegisterTagType<MigStudentId>("mig_student");
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        _stores.Add(store);
        return store;
    }

    private async Task<List<object[]>> QueryAsync(string sql)
    {
        await using var conn = new SqlConnection(_queryConnection);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var rows = new List<object[]>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values);
        }

        return rows;
    }

    private async Task<string> createDatabaseAsync(string name)
    {
        var database = ConnectionSource.Scoped(name);
        await dropDatabaseAsync(database);

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE [{database}]";
        await cmd.ExecuteNonQueryAsync(Ct);

        _databases.Add(database);
        return ConnectionSource.ConnectionStringFor(database);
    }

    private static async Task dropDatabaseAsync(string database)
    {
        SqlConnection.ClearAllPools();
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF DB_ID('{database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{database}];
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    private record Populated(List<Guid> Live, List<Guid> Archived, List<(long SeqId, Guid Id)> Events,
        MigStudentId TaggedStudent, OrderNumber LiveOrder);

    private async Task<Populated> PopulateAsync(DocumentStore store, params string[] tenants)
    {
        if (tenants.Length == 0) tenants = [StorageConstants.DefaultTenantId];

        var live = new List<Guid>();
        var archived = new List<Guid>();
        var student = new MigStudentId(Guid.NewGuid());
        var liveOrder = new OrderNumber("MIG-LIVE-" + Guid.NewGuid().ToString("N")[..6]);

        foreach (var tenant in tenants)
        {
            var archivedHere = new List<Guid>();
            for (var batch = 0; batch < StreamCount / 50; batch++)
            {
                await using var session = store.LightweightSession(tenant);
                for (var i = 0; i < 50; i++)
                {
                    var streamId = Guid.NewGuid();
                    var events = new List<object> { new MigEnrolled("stream " + i) };
                    for (var step = 1; step < EventsPerStream; step++) events.Add(new MigProgressed(step));

                    session.Events.StartStream(streamId, events.ToArray());

                    // every fifth stream ends up archived
                    if (i % 5 == 0) archivedHere.Add(streamId);
                    else live.Add(streamId);
                }

                await session.SaveChangesAsync(Ct);
            }

            await using (var tagged = store.LightweightSession(tenant))
            {
                var enrolled = tagged.Events.BuildEvent(new MigEnrolled("tagged"));
                enrolled.WithTag(student);
                var taggedStream = Guid.NewGuid();
                tagged.Events.StartStream(taggedStream, enrolled);
                live.Add(taggedStream);

                var orderStream = Guid.NewGuid();
                tagged.Events.StartStream(orderStream, new NkOrderCreated(liveOrder, "Alice"),
                    new NkOrderItemAdded("Widget", 3m));
                live.Add(orderStream);

                await tagged.SaveChangesAsync(Ct);
            }

            await using (var archiving = store.LightweightSession(tenant))
            {
                foreach (var id in archivedHere) archiving.Events.ArchiveStream(id);
                await archiving.SaveChangesAsync(Ct);
            }

            archived.AddRange(archivedHere);
        }

        var rows = await QueryAsync($"select seq_id, id from [{_schema}].[pc_events] order by seq_id");
        return new Populated(live, archived, rows.Select(x => ((long)x[0], (Guid)x[1])).ToList(), student,
            liveOrder);
    }

    private async Task<int> PartitionCountAsync()
    {
        var rows = await QueryAsync($"""
            select count(*) from sys.partitions p
            where p.object_id = object_id('[{_schema}].[pc_events]') and p.index_id <= 1
            """);
        return (int)rows[0][0];
    }

    private async Task<long> RowsInPartitionAsync(int partitionNumber)
    {
        var rows = await QueryAsync($"""
            select coalesce(sum(p.rows), 0) from sys.partitions p
            where p.object_id = object_id('[{_schema}].[pc_events]') and p.index_id <= 1
              and p.partition_number = {partitionNumber}
            """);
        return Convert.ToInt64(rows[0][0]);
    }

    private async Task AssertMigratedAsync(DocumentStore store, Populated before, string tenant)
    {
        // Every event is still there, with its sequence number
        var after = await QueryAsync($"select seq_id, id from [{_schema}].[pc_events] order by seq_id");
        after.Select(x => ((long)x[0], (Guid)x[1])).ShouldBe(before.Events);

        (await PartitionCountAsync()).ShouldBe(2);

        var archivedEvents = (long)(await QueryAsync(
            $"select count_big(*) from [{_schema}].[pc_events] where is_archived = 1"))[0][0];
        archivedEvents.ShouldBeGreaterThan(0);
        (await RowsInPartitionAsync(2)).ShouldBe(archivedEvents);
        (await RowsInPartitionAsync(1)).ShouldBe(before.Events.Count - archivedEvents);

        await using (var query = store.QuerySession(tenant))
        {
            (await query.Events.FetchStreamAsync(before.Live[0], token: Ct)).Count.ShouldBe(EventsPerStream);
            (await query.Events.FetchStreamAsync(before.Archived[0], token: Ct)).Count.ShouldBe(0);
        }

        await using (var tags = store.LightweightSession(tenant))
        {
            (await tags.Events.EventsExistAsync(new EventTagQuery().Or<MigStudentId>(before.TaggedStudent), Ct))
                .ShouldBeTrue();
        }

        // Appends after the move keep numbering where the table left off
        await using (var session = store.LightweightSession(tenant))
        {
            session.Events.Append(before.Live[0], new MigProgressed(99));
            await session.SaveChangesAsync(Ct);
        }

        var maxSeq = (long)(await QueryAsync($"select max(seq_id) from [{_schema}].[pc_events]"))[0][0];
        maxSeq.ShouldBeGreaterThan(before.Events.Max(x => x.SeqId));

        // Archiving after the move puts the events in the archived partition
        await using (var session = store.LightweightSession(tenant))
        {
            session.Events.ArchiveStream(before.Live[1]);
            await session.SaveChangesAsync(Ct);
        }

        (await RowsInPartitionAsync(2)).ShouldBe(archivedEvents + EventsPerStream);

        await using (var session = store.LightweightSession(tenant))
        {
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(before.LiveOrder, Ct);
            stream.Aggregate.ShouldNotBeNull();
        }

        // ...and every table that exists now matches the configuration, so the next start does nothing
        // to them (a feature nothing has touched yet, such as the dead letter table, is still a Create)
        var database = store.Options.Tenancy?.GetDatabase(tenant) ?? store.Database;
        var migration = await database.CreateMigrationAsync(Ct);
        migration.Deltas.Where(x => x.Difference != SchemaPatchDifference.Create)
            .Where(x => x.Difference != SchemaPatchDifference.None)
            .Select(x => x.SchemaObject.Identifier.QualifiedName)
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task existing_store_moves_onto_the_partition_scheme_under_create_or_update()
    {
        var original = BuildStore(partitioned: false);
        var before = await PopulateAsync(original);
        before.Events.Count.ShouldBeGreaterThan(3000);

        var partitioned = BuildStore(partitioned: true);
        await partitioned.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await AssertMigratedAsync(partitioned, before, StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task existing_store_migrates_on_first_use_with_no_explicit_apply()
    {
        var original = BuildStore(partitioned: false);
        var before = await PopulateAsync(original);

        var partitioned = BuildStore(partitioned: true);

        // The first session to touch the event store migrates it
        await using (var session = partitioned.LightweightSession())
        {
            (await session.Events.FetchStreamAsync(before.Live[2], token: Ct)).Count.ShouldBe(EventsPerStream);
        }

        await AssertMigratedAsync(partitioned, before, StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task auto_create_all_never_drops_the_events_table()
    {
        var original = BuildStore(partitioned: false);
        var before = await PopulateAsync(original);

        var partitioned = BuildStore(partitioned: true, AutoCreate.All);
        await partitioned.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await AssertMigratedAsync(partitioned, before, StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task conjoined_store_with_several_tenants_migrates()
    {
        var original = BuildStore(partitioned: false, tenancy: TenancyStyle.Conjoined);
        var before = await PopulateAsync(original, "red", "green", "blue");
        before.Events.Count.ShouldBeGreaterThan(9000);

        var partitioned = BuildStore(partitioned: true, tenancy: TenancyStyle.Conjoined);
        await partitioned.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // The first tenant's streams lead each list
        await AssertMigratedAsync(partitioned, before, "red");
    }

    [Fact]
    public async Task the_migration_script_can_be_run_twice()
    {
        var original = BuildStore(partitioned: false);
        var before = await PopulateAsync(original);

        var partitioned = BuildStore(partitioned: true);
        var migration = await partitioned.Database.CreateMigrationAsync(Ct);
        var script = new StringWriter();
        migration.WriteAllUpdates(script, partitioned.Database.Migrator, AutoCreate.CreateOrUpdate);

        // db-patch's script never drops the events table
        script.ToString().ShouldNotContain("DROP TABLE", Case.Insensitive);

        await RunScriptAsync(script.ToString());
        await RunScriptAsync(script.ToString());

        await AssertMigratedAsync(partitioned, before, StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task an_interrupted_migration_finishes_on_the_next_run()
    {
        var original = BuildStore(partitioned: false);
        var before = await PopulateAsync(original);

        var partitioned = BuildStore(partitioned: true);
        var migration = await partitioned.Database.CreateMigrationAsync(Ct);
        var script = new StringWriter();
        migration.WriteAllUpdates(script, partitioned.Database.Migrator, AutoCreate.CreateOrUpdate);

        // Stop right after the events table lost its old primary key, the worst place to be cut off:
        // the table is a heap at that point and nothing keys it.
        var lines = script.ToString().Split('\n');
        var keyDropped = Array.FindIndex(lines, x =>
            x.Contains("drop constraint if exists pkey_pc_events_seq_id;", StringComparison.OrdinalIgnoreCase));
        keyDropped.ShouldBeGreaterThan(0);

        // Through the end of the IF ... BEGIN ... END block it sits in, which is where a batch can end
        var blockEnd = Array.FindIndex(lines, keyDropped, x => x.Trim() == "END");
        await RunScriptAsync(string.Join('\n', lines.Take(blockEnd + 1)) + "\n");

        // The next start picks it up from wherever it stopped
        var restarted = BuildStore(partitioned: true);
        await restarted.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await AssertMigratedAsync(restarted, before, StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task every_tenant_database_migrates_under_database_per_tenant()
    {
        var databases = new Dictionary<string, string>
        {
            ["tenant_a"] = await createDatabaseAsync("arch_mig_a"),
            ["tenant_b"] = await createDatabaseAsync("arch_mig_b")
        };

        var original = BuildStore(partitioned: false, tenantDatabases: databases);
        var before = new Dictionary<string, Populated>();
        foreach (var (tenantId, connectionString) in databases)
        {
            _queryConnection = connectionString;
            before[tenantId] = await PopulateAsync(original, tenantId);
        }

        var partitioned = BuildStore(partitioned: true, tenantDatabases: databases);
        foreach (var (tenantId, connectionString) in databases)
        {
            _queryConnection = connectionString;

            // Each database migrates the first time a session for one of its tenants needs the event store
            await using (var session = partitioned.LightweightSession(tenantId))
            {
                (await session.Events.FetchStreamAsync(before[tenantId].Live[2], token: Ct)).Count
                    .ShouldBe(EventsPerStream);
            }

            await AssertMigratedAsync(partitioned, before[tenantId], tenantId);
        }
    }

    private static async Task RunScriptAsync(string script)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        foreach (var batch in System.Text.RegularExpressions.Regex.Split(script, @"^\s*GO\s*$",
                     System.Text.RegularExpressions.RegexOptions.Multiline))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = batch;
            cmd.CommandTimeout = 300;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
