using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Xml.Linq;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Schema;

public class Документ
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

public class Приложение : Документ
{
    public string Note { get; set; } = string.Empty;
}

public record Событие(string Name);

/// <summary>
///     #935: <see cref="AdvancedOptions.UseUnicodeKeys" /> on a new store. A database of its own, under the
///     server's default collation, whose code page has none of the characters below.
/// </summary>
public class unicode_keys_tests : IAsyncLifetime
{
    private const string Schema = "unicode_keys";
    private static readonly string Database = ConnectionSource.Scoped("unicode_keys");

    // Outside code page 1252, and a character outside the basic plane
    private const string Tenant = "東京";
    private const string Key = "Москва 😀";

    private readonly List<DocumentStore> _stores = [];
    private readonly RecordingLogger _logger = new();

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

    private DocumentStore storeFor(bool unicode = true, Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionStringFor(Database),
            AutoCreateSchemaObjects = AutoCreate.All
        };

        unicode_keys_upgrade_tests.Configure(options);
        options.DatabaseSchemaName = Schema;
        options.Events.EnableUserName = true;
        options.Events.AddEventType<Событие>();
        options.Schema.For<Документ>().MultiTenanted().AddSubClass<Приложение>("приложение").Metadata(m =>
        {
            m.CorrelationId.Enabled = true;
            m.CausationId.Enabled = true;
            m.LastModifiedBy.Enabled = true;
        });
        options.Advanced.UseUnicodeKeys = unicode;
        options.DotNetLogger = _logger;
        configure?.Invoke(options);

        var store = new DocumentStore(options);
        _stores.Add(store);
        return store;
    }

    private static async Task<List<string?[]>> rowsAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        var rows = new List<string?[]>();
        while (await reader.ReadAsync(Ct))
        {
            var row = new string?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i).ToString();
            rows.Add(row);
        }

        return rows;
    }

    [Fact]
    public void the_setting_is_off_unless_asked_for()
    {
        new StoreOptions().Advanced.UseUnicodeKeys.ShouldBeFalse();
    }

    [Fact]
    public async Task every_widened_column_keeps_what_varchar_could_not()
    {
        var store = storeFor();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = Tenant }))
        {
            session.CorrelationId = $"correlation {Key}";
            session.CausationId = $"causation {Key}";
            session.LastModifiedBy = $"user {Key}";

            session.Store(new Документ { Id = $"doc {Key}", Name = "root" });
            session.Store(new Приложение { Id = $"sub {Key}", Name = "sub", Note = "note" });

            var opened = session.Events.BuildEvent(new UnicodeTicketOpened($"code {Key}", "title"));
            opened.WithTag(new UnicodeTag($"tag {Key}"));
            session.Events.StartStream<UnicodeTicket>($"ticket {Key}", opened, new Событие("named"));

            await session.SaveChangesAsync(Ct);
        }

        var docs = await rowsAsync($"""
            SELECT tenant_id, id, doc_type, correlation_id, causation_id, last_modified_by, dotnet_type
            FROM {Schema}.pc_doc_документ ORDER BY id
            """);
        docs.Count.ShouldBe(2);
        docs[0].ShouldBe([Tenant, $"doc {Key}", "base", $"correlation {Key}", $"causation {Key}", $"user {Key}",
            typeof(Документ).FullName]);
        docs[1][1].ShouldBe($"sub {Key}");
        docs[1][2].ShouldBe("приложение");
        docs[1][6].ShouldBe(typeof(Приложение).FullName);

        (await rowsAsync($"SELECT tenant_id, id FROM {Schema}.pc_streams")).Single()
            .ShouldBe([Tenant, $"ticket {Key}"]);

        var events = await rowsAsync($"""
            SELECT tenant_id, stream_id, correlation_id, causation_id, user_name, dotnet_type
            FROM {Schema}.pc_events ORDER BY version
            """);
        events.Count.ShouldBe(2);
        events.ShouldAllBe(x => x[0] == Tenant && x[1] == $"ticket {Key}" && x[2] == $"correlation {Key}"
                                && x[3] == $"causation {Key}" && x[4] == $"user {Key}");
        events[1][5]!.ShouldStartWith(typeof(Событие).FullName!);

        (await rowsAsync($"SELECT value, tenant_id FROM {Schema}.pc_event_tag_unicode_tag")).Single()
            .ShouldBe([$"tag {Key}", Tenant]);
        (await rowsAsync($"SELECT tag_value, tenant_id FROM {Schema}.pc_dcb_tag_version WHERE tag_table = 'unicode_tag'")).Single()
            .ShouldBe([$"tag {Key}", Tenant]);
        (await rowsAsync($"SELECT natural_key_value, stream_key, tenant_id FROM {Schema}.pc_natural_key_unicodeticket"))
            .Single().ShouldBe([$"code {Key}", $"ticket {Key}", Tenant]);

        // ...and everything reads back through the API under the same keys
        await using var query = store.LightweightSession(new SessionOptions { TenantId = Tenant });
        (await query.LoadAsync<Документ>($"doc {Key}", Ct))!.Name.ShouldBe("root");
        (await query.LoadAsync<Приложение>($"sub {Key}", Ct))!.Note.ShouldBe("note");
        (await query.Query<Документ>().Where(x => x.Id == $"doc {Key}").ToListAsync(Ct)).Single().Name.ShouldBe("root");
        (await query.Events.FetchStreamAsync($"ticket {Key}", token: Ct)).Count.ShouldBe(2);
        (await query.Events.FetchLatest<UnicodeTicket, UnicodeTicketCode>(new($"code {Key}"), Ct))!.Id.ShouldBe($"ticket {Key}");
        (await query.Events.QueryByTagsAsync(new EventTagQuery().Or(new UnicodeTag($"tag {Key}")), Ct)).Count.ShouldBe(1);

        // Another tenant's id that varchar would have made the same ?? is somebody else
        await using var other = store.QuerySession(new SessionOptions { TenantId = "大阪" });
        (await other.LoadAsync<Документ>($"doc {Key}", Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task two_keys_that_varchar_would_make_one_are_two()
    {
        var store = storeFor();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = Tenant }))
        {
            session.Store(new Документ { Id = "東京", Name = "tokyo" });
            session.Store(new Документ { Id = "大阪", Name = "osaka" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = Tenant });
        (await query.LoadAsync<Документ>("東京", Ct))!.Name.ShouldBe("tokyo");
        (await query.LoadAsync<Документ>("大阪", Ct))!.Name.ShouldBe("osaka");
        (await query.LoadManyAsync<Документ>(["東京", "大阪"], Ct)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_string_id_load_seeks_with_an_nvarchar_parameter()
    {
        var store = storeFor();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = Tenant }))
        {
            for (var i = 0; i < 200; i++) session.Store(new Документ { Id = $"doc {i}", Name = $"{i}" });
            await session.SaveChangesAsync(Ct);
        }

        // The command the load really sends, parameters and all, run again under SET STATISTICS XML
        var load = await LoadCommandCapture.CaptureAsync("pc_doc_документ", async () =>
        {
            await using var query = store.QuerySession(new SessionOptions { TenantId = Tenant });
            (await query.LoadAsync<Документ>("doc 17", Ct))!.Name.ShouldBe("17");
        });

        load.Parameters.Cast<SqlParameter>().Where(x => x.Value is string)
            .Select(x => (x.ParameterName.TrimStart('@'), x.SqlDbType))
            .ShouldBe([("id", System.Data.SqlDbType.NVarChar), ("tenant_id", System.Data.SqlDbType.NVarChar)], ignoreOrder: true);

        var plan = await LoadCommandCapture.ActualPlanAsync(ConnectionSource.ConnectionStringFor(Database), load, Ct);
        plan.ShouldNotContain("CONVERT_IMPLICIT");

        var accesses = Polecat.Tests.Linq.QueryPlanProbe.ReadAccesses(plan);
        accesses.ShouldContain(x => x.Operator == "Clustered Index Seek" && x.Table == "pc_doc_документ");
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }

    [Fact]
    public async Task a_key_past_450_characters_is_refused_naming_the_tenant_and_id()
    {
        var store = storeFor();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // 210 + 250 characters is 920 bytes in (tenant_id, id)
        var tenant = new string('t', 210);
        var id = new string('東', 250);

        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Store(new Документ { Id = id, Name = "too long" });

        var refused = await Should.ThrowAsync<KeyTooLongException>(() => session.SaveChangesAsync(Ct));
        refused.TenantId.ShouldBe(tenant);
        refused.Id.ShouldBe(id);
        refused.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(1946);
        refused.Message.ShouldContain("450 characters");

        // One just under the limit is fine
        await using var fits = store.LightweightSession(new SessionOptions { TenantId = tenant });
        fits.Store(new Документ { Id = new string('東', 240), Name = "fits" });
        await fits.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_stream_key_past_the_limit_is_refused_naming_the_tenant_and_key()
    {
        var store = storeFor();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var tenant = new string('t', 210);
        var key = new string('東', 250);

        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(key, new Событие("too long"));

        var refused = await Should.ThrowAsync<KeyTooLongException>(() => session.SaveChangesAsync(Ct));
        refused.TenantId.ShouldBe(tenant);
        refused.Id.ShouldBe(key);
    }

    [Fact]
    public async Task the_keys_declared_past_the_limit_are_logged_once_when_the_store_starts()
    {
        storeFor();

        var warning = _logger.Warnings.Where(x => x.Contains("UseUnicodeKeys")).ShouldHaveSingleItem();
        warning.ShouldContain("1945");
        warning.ShouldContain($"{Schema}.pc_streams (tenant_id, id): 1000 bytes, limit 900");
        warning.ShouldContain($"{Schema}.pc_doc_документ (tenant_id, id): 1000 bytes, limit 900");
        warning.ShouldContain($"{Schema}.pc_dcb_tag_version (tag_table, tag_value, tenant_id): 1250 bytes, limit 900");
        warning.ShouldNotContain("pc_events (seq_id)");
    }

    [Fact]
    public void nothing_is_logged_without_the_setting()
    {
        storeFor(unicode: false);
        _logger.Warnings.ShouldNotContain(x => x.Contains("UseUnicodeKeys"));
    }

    /// <summary>
    ///     Events and documents both partitioned by tenant. A store of its own, without the documents
    ///     <see cref="unicode_keys_upgrade_tests.Configure" /> adds, since a foreign key between two
    ///     tenant-partitioned document tables isn't supported.
    /// </summary>
    private DocumentStore partitionedStoreFor(bool unicode = true)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionStringFor(Database),
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = Schema,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        options.Events.TenancyStyle = TenancyStyle.Conjoined;
        options.Events.StreamIdentity = StreamIdentity.AsString;
        options.Events.AddEventType<UnicodeTicketOpened>();
        options.EventGraph.UseTenantPartitionedEvents = true;
        options.Schema.For<Документ>().MultiTenanted();
        options.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
        options.Advanced.UseUnicodeKeys = unicode;

        var store = new DocumentStore(options);
        _stores.Add(store);
        return store;
    }

    /// <summary>
    ///     #1042: the partition registry is nvarchar too, so two tenants that varchar would make one (both are
    ///     ?? in the default code page) get partitions of their own, and their events and documents stay apart.
    /// </summary>
    [Fact]
    public async Task two_tenants_varchar_would_make_one_get_partitions_of_their_own()
    {
        var store = partitionedStoreFor();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var tenant in new[] { "東京", "大阪" })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Store(new Документ { Id = Key, Name = tenant });
            session.Events.StartStream(Key, new UnicodeTicketOpened($"code {tenant}", tenant));
            await session.SaveChangesAsync(Ct);
        }

        (await rowsAsync($"""
            SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{Schema}' AND TABLE_NAME = 'pc_tenant_partitions'
              AND COLUMN_NAME IN ('tenant_id', 'bucket')
            """)).Select(x => x[0]).ShouldBe(["nvarchar", "nvarchar"]);

        var registry = await rowsAsync($"SELECT tenant_id, ordinal FROM {Schema}.pc_tenant_partitions ORDER BY ordinal");
        registry.Select(x => x[0]).ShouldBe(["東京", "大阪"]);
        registry.Select(x => x[1]).Distinct().Count().ShouldBe(2);

        (await rowsAsync($"SELECT DISTINCT tenant_ordinal FROM {Schema}.pc_events")).Count.ShouldBe(2);
        (await rowsAsync($"SELECT DISTINCT tenant_ordinal FROM {Schema}.pc_doc_документ")).Count.ShouldBe(2);

        foreach (var tenant in new[] { "東京", "大阪" })
        {
            await using var query = store.QuerySession(new SessionOptions { TenantId = tenant });
            (await query.LoadAsync<Документ>(Key, Ct))!.Name.ShouldBe(tenant);
            (await query.Query<Документ>().ToListAsync(Ct)).Single().Name.ShouldBe(tenant);

            var events = await query.Events.FetchStreamAsync(Key, token: Ct);
            events.Single().Data.ShouldBe(new UnicodeTicketOpened($"code {tenant}", tenant));
        }
    }

    /// <summary>
    ///     #1042: turning the setting on for a store that already partitions by tenant migrates the registry,
    ///     and the tenants already in it keep their ordinals.
    /// </summary>
    [Fact]
    public async Task turning_the_setting_on_widens_an_existing_partition_registry()
    {
        var narrow = partitionedStoreFor(unicode: false);
        await narrow.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = narrow.LightweightSession(new SessionOptions { TenantId = "plain" }))
        {
            session.Store(new Документ { Id = "kept", Name = "kept" });
            await session.SaveChangesAsync(Ct);
        }

        var before = await rowsAsync($"SELECT tenant_id, ordinal FROM {Schema}.pc_tenant_partitions");

        var wide = partitionedStoreFor();
        await wide.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await rowsAsync($"""
            SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{Schema}' AND TABLE_NAME = 'pc_tenant_partitions' AND COLUMN_NAME = 'tenant_id'
            """)).Single()[0].ShouldBe("nvarchar");
        (await rowsAsync($"SELECT tenant_id, ordinal FROM {Schema}.pc_tenant_partitions")).ShouldBe(before);

        await using (var session = wide.LightweightSession(new SessionOptions { TenantId = Tenant }))
        {
            session.Store(new Документ { Id = "new", Name = Tenant });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = wide.QuerySession(new SessionOptions { TenantId = "plain" });
        (await query.LoadAsync<Документ>("kept", Ct))!.Name.ShouldBe("kept");

        (await rowsAsync($"SELECT tenant_id FROM {Schema}.pc_tenant_partitions ORDER BY ordinal"))
            .Select(x => x[0]).ShouldBe(["plain", Tenant]);
    }

    [Fact]
    public async Task turning_the_setting_off_again_is_refused()
    {
        var unicode = storeFor();
        await unicode.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = unicode.LightweightSession(new SessionOptions { TenantId = Tenant }))
        {
            session.Store(new Документ { Id = Key, Name = "kept" });
            await session.SaveChangesAsync(Ct);
        }

        var narrowed = storeFor(unicode: false);

        var refused = await Should.ThrowAsync<InvalidOperationException>(() =>
            narrowed.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct));
        refused.Message.ShouldContain("from nvarchar to varchar");
        refused.Message.ShouldContain("UseUnicodeKeys");

        // The same on first use, and from the migration db-patch writes
        await using (var session = narrowed.LightweightSession(new SessionOptions { TenantId = "red" }))
        {
            session.Store(new Документ { Id = "another", Name = "refused" });
            (await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct)))
                .Message.ShouldContain("from nvarchar to varchar");
        }

        await Should.ThrowAsync<InvalidOperationException>(() => narrowed.Database.CreateMigrationAsync(Ct));

        // Nothing was narrowed
        (await rowsAsync($"SELECT tenant_id, id FROM {Schema}.pc_doc_документ")).Single().ShouldBe([Tenant, Key]);
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<string> _warnings = new();

        public IReadOnlyList<string> Warnings => _warnings.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning) _warnings.Enqueue(formatter(state, exception));
        }
    }
}
