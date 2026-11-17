using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Schema;

public record UnicodeTicketOpened(string Code, string Title);

public record UnicodeTicketRetitled(string Title);

public record UnicodeTag(string Value);

public partial class UnicodeTicket
{
    public string Id { get; set; } = string.Empty;

    [NaturalKey]
    public string Code { get; set; } = string.Empty;

    public string Title { get; set; } = string.Empty;

    [NaturalKeySource]
    public void Apply(UnicodeTicketOpened e)
    {
        Code = e.Code;
        Title = e.Title;
    }

    public void Apply(UnicodeTicketRetitled e) => Title = e.Title;
}

public class UnicodeCustomer
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

public class UnicodeOrder
{
    public string Id { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

/// <summary>
///     #935: a store built the way Polecat 5 builds one, with <c>varchar</c> keys and data in it, turns
///     <see cref="AdvancedOptions.UseUnicodeKeys" /> on and migrates through the ordinary schema delta. Every
///     row it had is still there afterwards, and the keys then take what <c>varchar</c> couldn't hold.
/// </summary>
/// <remarks>
///     A database of its own, because the point is the whole store at once: the cross-table foreign keys
///     (<c>pc_events</c> and the natural key table to <c>pc_streams</c>, one document to another) have to
///     change on both sides in the same migration, or the key comes back on columns of different types.
/// </remarks>
public class unicode_keys_upgrade_tests : IAsyncLifetime
{
    private const string Schema = "unicode_upgrade";
    private static readonly string Database = ConnectionSource.Scoped("unicode_upgrade");

    private static readonly string[] Tenants = ["red", "café"];

    private readonly List<DocumentStore> _stores = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await dropAsync();
        await masterAsync($"CREATE DATABASE [{Database}];");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores) store.Dispose();
        await dropAsync();
    }

    private static async Task dropAsync()
    {
        SqlConnection.ClearAllPools();
        await masterAsync($"""
            IF DB_ID('{Database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{Database}];
            END
            """);
    }

    private static async Task masterAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    internal static void Configure(StoreOptions opts)
    {
        opts.DatabaseSchemaName = Schema;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.Events.StreamIdentity = StreamIdentity.AsString;
        opts.Events.EnableCorrelationId = true;
        opts.Events.EnableCausationId = true;
        opts.Events.RegisterTagType<UnicodeTag>("unicode_tag");
        opts.Projections.Add<SingleStreamProjection<UnicodeTicket, string>>(ProjectionLifecycle.Inline);

        opts.Schema.For<UnicodeCustomer>().MultiTenanted();
        opts.Schema.For<UnicodeOrder>().MultiTenanted().ForeignKey<UnicodeCustomer>(x => x.CustomerId);
    }

    private DocumentStore storeFor(bool unicode, AutoCreate autoCreate = AutoCreate.All, bool? partitioned = null)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionStringFor(Database),
            AutoCreateSchemaObjects = autoCreate
        };
        Configure(options);
        options.Advanced.UseUnicodeKeys = unicode;
        if (partitioned is { } on) options.EventGraph.UseArchivedStreamPartitioning = on;

        var store = new DocumentStore(options);
        _stores.Add(store);
        return store;
    }

    private static async Task writeAsync(DocumentStore store, string tenant, string key)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.CorrelationId = $"correlation {key}";
        session.CausationId = $"causation {key}";

        session.Store(new UnicodeCustomer { Id = $"customer {key}", Name = $"named {key}" });
        session.Store(new UnicodeOrder { Id = $"order {key}", CustomerId = $"customer {key}", Amount = 10 });

        var opened = session.Events.BuildEvent(new UnicodeTicketOpened($"code {key}", $"title {key}"));
        opened.WithTag(new UnicodeTag($"tag {key}"));
        session.Events.StartStream<UnicodeTicket>($"ticket {key}", opened);

        await session.SaveChangesAsync(Ct);
    }

    private static async Task readBackAsync(DocumentStore store, string tenant, string key)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });

        // The order first: on first use that migrates the table holding the foreign key before the one it
        // references
        (await session.LoadAsync<UnicodeOrder>($"order {key}", Ct))!.CustomerId.ShouldBe($"customer {key}");
        (await session.LoadAsync<UnicodeCustomer>($"customer {key}", Ct))!.Name.ShouldBe($"named {key}");

        var events = await session.Events.FetchStreamAsync($"ticket {key}", token: Ct);
        events.Count.ShouldBe(1);
        events[0].CorrelationId.ShouldBe($"correlation {key}");
        events[0].CausationId.ShouldBe($"causation {key}");
        events[0].TenantId.ShouldBe(tenant);

        var state = await session.Events.FetchStreamStateAsync($"ticket {key}", Ct);
        state.ShouldNotBeNull();
        state.Key.ShouldBe($"ticket {key}");

        (await session.Events.FetchLatest<UnicodeTicket, string>($"code {key}", Ct))!.Title.ShouldBe($"title {key}");
        (await session.LoadAsync<UnicodeTicket>($"ticket {key}", Ct))!.Code.ShouldBe($"code {key}");

        var tagged = await session.Events.QueryByTagsAsync(new EventTagQuery().Or(new UnicodeTag($"tag {key}")), Ct);
        tagged.Select(x => x.StreamKey).ShouldBe([$"ticket {key}"]);

        // A write against what was there, so the boundary row and the natural key row are used as well
        var stream = await session.Events.FetchForWriting<UnicodeTicket, string>($"code {key}", Ct);
        stream.AppendOne(new UnicodeTicketRetitled($"retitled {key}"));
        await session.SaveChangesAsync(Ct);

        (await session.LoadAsync<UnicodeTicket>($"ticket {key}", Ct))!.Title.ShouldBe($"retitled {key}");
    }

    private static async Task<Dictionary<string, string>> keyColumnTypesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT t.name + '.' + c.name, TYPE_NAME(c.user_type_id)
            FROM sys.columns c
            JOIN sys.tables t ON t.object_id = c.object_id
            WHERE SCHEMA_NAME(t.schema_id) = '{Schema}'
              AND c.name IN ('tenant_id', 'id', 'stream_id', 'stream_key', 'value', 'tag_value', 'correlation_id',
                             'causation_id', 'dotnet_type', 'name')
              AND c.is_computed = 0
              AND TYPE_NAME(c.user_type_id) LIKE '%char';
            """;
        var types = new Dictionary<string, string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) types[reader.GetString(0)] = reader.GetString(1);
        return types;
    }

    [Fact]
    public async Task a_varchar_store_migrates_to_nvarchar_keys_and_keeps_every_row()
    {
        var before = storeFor(unicode: false);
        await before.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var tenant in Tenants)
        {
            await writeAsync(before, tenant, "one");
            await writeAsync(before, tenant, "two");
        }

        (await keyColumnTypesAsync()).Values.ShouldAllBe(x => x == "varchar");

        var after = storeFor(unicode: true);
        await after.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var types = await keyColumnTypesAsync();
        types.Where(x => x.Value != "nvarchar").Select(x => x.Key)
            .Except(["pc_event_tag_unicode_tag.value"]).ShouldBeEmpty(); // checked below by name
        types["pc_streams.id"].ShouldBe("nvarchar");
        types["pc_events.stream_id"].ShouldBe("nvarchar");
        types["pc_event_tag_unicode_tag.value"].ShouldBe("nvarchar");
        types["pc_dcb_tag_version.tag_value"].ShouldBe("nvarchar");

        // Nothing left to do: the migration it ran is the one it wanted
        (await after.Database.CreateMigrationAsync(Ct)).Difference.ShouldBe(SchemaPatchDifference.None);

        foreach (var tenant in Tenants)
        {
            await readBackAsync(after, tenant, "one");
            await readBackAsync(after, tenant, "two");
        }

        // ...and the keys now hold what varchar couldn't
        await writeAsync(after, "東京", "Москва 😀");
        await readBackAsync(after, "東京", "Москва 😀");
    }

    [Fact]
    public async Task a_varchar_store_migrates_on_first_use()
    {
        var before = storeFor(unicode: false);
        await before.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var tenant in Tenants) await writeAsync(before, tenant, "one");

        // No ApplyAllConfiguredChangesToDatabaseAsync: each table is migrated by the first session that needs it
        var after = storeFor(unicode: true, AutoCreate.CreateOrUpdate);

        foreach (var tenant in Tenants) await readBackAsync(after, tenant, "one");

        await writeAsync(after, "東京", "Москва 😀");
        await readBackAsync(after, "東京", "Москва 😀");

        // Every table this used. The dead letter table is migrated by the first thing that writes a dead letter.
        (await keyColumnTypesAsync()).Where(x => x.Value != "nvarchar" && !x.Key.StartsWith("pc_doc_deadletterevent."))
            .Select(x => x.Key).ShouldBeEmpty();

        // The foreign key between the two documents is back and holds
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.foreign_keys WHERE name = 'fk_pc_doc_unicodeorder_cc_customerid'";
        ((int)(await cmd.ExecuteScalarAsync(Ct))!).ShouldBe(1);
    }
    private static async Task archiveAsync(DocumentStore store, string tenant, string key)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.ArchiveStream($"ticket {key}");
        await session.SaveChangesAsync(Ct);
    }

    private static async Task<long> scalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    private static Task<long> eventPartitionsAsync() => scalarAsync($"""
        select count(*) from sys.partitions p
        where p.object_id = object_id('[{Schema}].[pc_events]') and p.index_id <= 1
        """);

    private static Task<long> eventsInPartitionAsync(int partitionNumber) => scalarAsync($"""
        select coalesce(sum(p.rows), 0) from sys.partitions p
        where p.object_id = object_id('[{Schema}].[pc_events]') and p.index_id <= 1
          and p.partition_number = {partitionNumber}
        """);

    /// <summary>
    ///     A store the way Polecat 5 left it, <c>varchar</c> keys and <c>pc_events</c> off the partition scheme,
    ///     upgraded to 6.0 with <see cref="AdvancedOptions.UseUnicodeKeys" /> on: archived-stream partitioning is
    ///     on by default there, so one migration both widens the keys and moves <c>pc_events</c> onto the scheme.
    /// </summary>
    private async Task<long> v5StoreWithArchivedStreamsAsync()
    {
        var before = storeFor(unicode: false, partitioned: false);
        await before.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var tenant in Tenants)
        {
            await writeAsync(before, tenant, "one");
            await writeAsync(before, tenant, "archived");
            await archiveAsync(before, tenant, "archived");
        }

        (await keyColumnTypesAsync()).Values.ShouldAllBe(x => x == "varchar");
        (await eventPartitionsAsync()).ShouldBe(1);

        return await scalarAsync($"select count_big(*) from [{Schema}].[pc_events]");
    }

    private async Task assertOnTheSchemeWithUnicodeKeysAsync(DocumentStore after, long eventsBefore)
    {
        // Reads and writes first: on first use they are what migrates each table
        foreach (var tenant in Tenants)
        {
            await readBackAsync(after, tenant, "one");

            await using var query = after.QuerySession(new SessionOptions { TenantId = tenant });
            (await query.Events.FetchStreamAsync("ticket archived", token: Ct)).Count.ShouldBe(0);
        }

        var types = await keyColumnTypesAsync();
        types["pc_streams.id"].ShouldBe("nvarchar");
        types["pc_events.stream_id"].ShouldBe("nvarchar");
        types["pc_events.tenant_id"].ShouldBe("nvarchar");
        types["pc_event_tag_unicode_tag.value"].ShouldBe("nvarchar");

        // Every event kept (plus the retitle readBackAsync appended per tenant), and the archived streams'
        // events in the archived partition
        (await scalarAsync($"select count_big(*) from [{Schema}].[pc_events]")).ShouldBe(eventsBefore + Tenants.Length);
        (await eventPartitionsAsync()).ShouldBe(2);
        var archived = await scalarAsync($"select count_big(*) from [{Schema}].[pc_events] where is_archived = 1");
        archived.ShouldBe(Tenants.Length);
        (await eventsInPartitionAsync(2)).ShouldBe(archived);

        // A unicode stream archived after the move lands in the archived partition too
        await writeAsync(after, "東京", "Москва 😀");
        await readBackAsync(after, "東京", "Москва 😀");
        await archiveAsync(after, "東京", "Москва 😀");

        var archivedNow = await scalarAsync($"select count_big(*) from [{Schema}].[pc_events] where is_archived = 1");
        archivedNow.ShouldBe(archived + 2); // the stream's opening event and the retitle readBackAsync appended
        (await eventsInPartitionAsync(2)).ShouldBe(archivedNow);
        (await scalarAsync($"""
            select count_big(*) from [{Schema}].[pc_events]
            where stream_id = N'ticket Москва 😀' and tenant_id = N'東京' and is_archived = 1
            """)).ShouldBe(2);
    }

    [Fact]
    public async Task a_v5_store_takes_unicode_keys_and_the_archived_partitions_in_one_migration()
    {
        var eventsBefore = await v5StoreWithArchivedStreamsAsync();

        var after = storeFor(unicode: true);
        after.Options.EventGraph.UseArchivedStreamPartitioning.ShouldBeTrue(); // the 6.0 default
        await after.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // Nothing left to do: the migration it ran is the one it wanted
        (await after.Database.CreateMigrationAsync(Ct)).Difference.ShouldBe(SchemaPatchDifference.None);

        await assertOnTheSchemeWithUnicodeKeysAsync(after, eventsBefore);
    }

    [Fact]
    public async Task a_v5_store_takes_unicode_keys_and_the_archived_partitions_on_first_use()
    {
        var eventsBefore = await v5StoreWithArchivedStreamsAsync();

        var after = storeFor(unicode: true, AutoCreate.CreateOrUpdate);

        await assertOnTheSchemeWithUnicodeKeysAsync(after, eventsBefore);
    }
    [Fact]
    public async Task a_new_store_with_unicode_keys_is_on_the_archived_partitions()
    {
        var store = storeFor(unicode: true);
        store.Options.EventGraph.UseArchivedStreamPartitioning.ShouldBeTrue();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await eventPartitionsAsync()).ShouldBe(2);
        (await keyColumnTypesAsync())["pc_events.stream_id"].ShouldBe("nvarchar");

        await writeAsync(store, "東京", "Москва 😀");
        await writeAsync(store, "東京", "Санкт-Петербург");
        await archiveAsync(store, "東京", "Москва 😀");

        (await eventsInPartitionAsync(2)).ShouldBe(1);
        (await eventsInPartitionAsync(1)).ShouldBe(1);

        await readBackAsync(store, "東京", "Санкт-Петербург");
        await using var query = store.QuerySession(new SessionOptions { TenantId = "東京" });
        (await query.Events.FetchStreamAsync("ticket Москва 😀", token: Ct)).Count.ShouldBe(0);
    }
}
