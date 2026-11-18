using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     jasperfx#1150 under per-tenant event partitioning, which <c>PendingEventAgeCompliance</c> cannot
///     switch on. Each tenant draws its own sequence there, so a tenant's position is only meaningful in
///     that tenant's own numbering: another tenant's higher sequences must neither answer for it nor hold
///     it behind.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_pending_event_age_tests : IAsyncLifetime
{
    private const string Schema = "pt_pending_age";

    public async ValueTask InitializeAsync()
    {
        await DropSchemaObjectsAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });
    }

    [Fact]
    public async Task each_tenant_is_answered_from_its_own_sequence()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        // Red 1..3, then Blue 1..2, then Red 4..5
        await AppendAsync(store, "Red", 3, token);
        await Task.Delay(50, token);
        await AppendAsync(store, "Blue", 2, token);
        await Task.Delay(50, token);
        await AppendAsync(store, "Red", 2, token);

        var ages = await ((IEventDatabase)store.Database).FetchFirstEventTimestampsAfterAsync(
        [
            ("Red", 0),
            ("Red", 3),
            ("Blue", 0),
            ("Blue", 2),
            ("Red", 5),
            ("Green", 0),
            (null, 4),
            (null, 0)
        ], token);

        ages.Select(x => (x.TenantId, x.Sequence)).ShouldBe(
        [
            ("Red", 0L), ("Red", 3L), ("Blue", 0L), ("Blue", 2L), ("Red", 5L), ("Green", 0L), (null, 4L), (null, 0L)
        ]);

        ages[0].Timestamp.ShouldBe(await TimestampOfAsync("Red", 1, token));
        ages[1].Timestamp.ShouldBe(await TimestampOfAsync("Red", 4, token));
        ages[2].Timestamp.ShouldBe(await TimestampOfAsync("Blue", 1, token));

        // Red's sequence 4 and 5 lie past Blue's position, but they are Red's numbers, not Blue's
        ages[3].Timestamp.ShouldBeNull();
        ages[4].Timestamp.ShouldBeNull();

        // A tenant with no partition has appended nothing
        ages[5].Timestamp.ShouldBeNull();

        // No tenant: the lowest sequence past the position in any tenant, which only Red has
        ages[6].Timestamp.ShouldBe(await TimestampOfAsync("Red", 5, token));

        // Both tenants have a sequence 1, and the older of the two answers
        ages[7].Timestamp.ShouldBe(await TimestampOfAsync("Red", 1, token));
    }

    private static async Task<DateTimeOffset> TimestampOfAsync(string tenant, long sequence, CancellationToken token)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT timestamp FROM [{Schema}].pc_events WHERE tenant_id = @tenant AND seq_id = @seq;";
        cmd.Parameters.AddWithValue("@tenant", tenant);
        cmd.Parameters.AddWithValue("@seq", sequence);
        return (DateTimeOffset)(await cmd.ExecuteScalarAsync(token))!;
    }

    private static async Task AppendAsync(DocumentStore store, string tenant, int count, CancellationToken token)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(Guid.NewGuid(),
            Enumerable.Range(1, count).Select(i => (object)new MembersJoined(i, tenant, [$"{tenant}{i}"])).ToArray());
        await session.SaveChangesAsync(token);
    }

    private static async Task DropSchemaObjectsAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
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
        await cmd.ExecuteNonQueryAsync();
    }
}
