using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #1136: two stores in different schemas of one database, both with archived-stream partitioning, provisioning
///     at the same moment. The partition function <c>pf_pc_events_is_archived</c> and scheme
///     <c>ps_pc_events_is_archived</c> belong to the DATABASE, so both stores declare the very same two objects,
///     and Weasel's DDL for them is check-then-act: drop them while no table is on them, then create each one that
///     is missing. The per-schema migration lock kept the two stores apart from nothing, so one store dropped what
///     the other had just created, or failed its <c>DROP PARTITION FUNCTION</c> on the scheme the other had just
///     put on it.
/// </summary>
/// <remarks>
///     <para>
///         Since Weasel 10.0 (weasel#920, #1140) a newly partitioned table gets objects named for its schema
///         (<c>pf_{schema}_pc_events_is_archived</c>), so two stores provisioning fresh tables no longer share them,
///         and this test now passes with or without Polecat's database-wide lock. It stays as the guard that two
///         partitioned stores can provision at once. The lock stays for tables partitioned before 6.0, which keep the
///         shared objects they are bound to. It failed 3-10 of 12 rounds without the lock on Weasel 10.0.0-rc.1.
///     </para>
///     <para>
///         The race only exists while no table is on the scheme yet: once one is, the drop is skipped and the
///         creates are no-ops. So every round drops both schemas' tables first, leaving the function and the
///         scheme behind unused, which is exactly the state a fresh deploy of two stores into one database starts
///         from after the first ever store's tables went away, and a strictly harder one than a brand new database.
///     </para>
///     <para>
///         The database is a sibling of the process's own, created and dropped here, because the partition objects
///         are database-wide: running this in the shared catalog would race every other test there that uses
///         archived partitioning.
///     </para>
/// </remarks>
public class archived_partitioning_shared_objects_race_tests : IAsyncLifetime
{
    private const int Rounds = 12;
    private static readonly string[] Schemas = ["shared_part_a", "shared_part_b"];

    private readonly string _database = ConnectionSource.Scoped("shared_partitions");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropDatabaseAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE [{_database}];";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        await DropDatabaseAsync();
    }

    [Fact]
    public async Task two_stores_applying_their_whole_schema_at_once_both_end_up_partitioned()
    {
        await raceAsync(store => store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct));
    }

    [Fact]
    public async Task two_stores_ensuring_the_event_store_on_first_append_both_end_up_partitioned()
    {
        await raceAsync(async store =>
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Shared partitions"));
            await session.SaveChangesAsync(Ct);
        });
    }

    private async Task raceAsync(Func<DocumentStore, Task> provision)
    {
        var failures = new List<string>();

        for (var round = 0; round < Rounds; round++)
        {
            await dropSchemaTablesAsync();

            var stores = Schemas.Select(buildStore).ToArray();
            try
            {
                // Both start from the same instant, so their catalog reads, drops and creates interleave
                using var barrier = new Barrier(stores.Length);
                var attempts = stores.Select(store => Task.Run(async () =>
                {
                    barrier.SignalAndWait(Ct);
                    await provision(store);
                }, Ct)).ToArray();

                foreach (var attempt in attempts)
                {
                    try
                    {
                        await attempt;
                    }
                    catch (Exception e)
                    {
                        failures.Add($"round {round}: {e.GetType().Name}: {e.Message}");
                    }
                }

                foreach (var schema in Schemas)
                {
                    var partitions = await partitionCountAsync(schema);
                    if (partitions != 2)
                    {
                        failures.Add($"round {round}: {schema}.pc_events has {partitions} partition(s), expected 2");
                    }
                }
            }
            finally
            {
                foreach (var store in stores) store.Dispose();
            }
        }

        failures.ShouldBeEmpty(string.Join(Environment.NewLine, failures));
    }

    private DocumentStore buildStore(string schema) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionStringFor(_database);
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.EventGraph.UseArchivedStreamPartitioning = true;

        // A retried statement can paper over the very failure this is looking for
        opts.ConfigurePolly(_ => { });
    });

    private async Task<int> partitionCountAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(_database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = @schema AND o.name = 'pc_events' AND p.index_id <= 1
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    // Test cleanup, not provisioning: leaves the database-wide partition function and scheme in place, unused
    private async Task dropSchemaTablesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(_database));
        await conn.OpenAsync(Ct);

        foreach (var schema in Schemas)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DECLARE @sql nvarchar(max) = N'';
                SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
                FROM sys.foreign_keys fk
                JOIN sys.tables t ON fk.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = @schema;

                SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = @schema;

                SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
                FROM sys.sequences sq
                JOIN sys.schemas s ON sq.schema_id = s.schema_id
                WHERE s.name = @schema;
                EXEC sp_executesql @sql;
                """;
            cmd.Parameters.AddWithValue("@schema", schema);
            await cmd.ExecuteNonQueryAsync(Ct);
        }
    }

    private async Task DropDatabaseAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF DB_ID('{_database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{_database}];
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
