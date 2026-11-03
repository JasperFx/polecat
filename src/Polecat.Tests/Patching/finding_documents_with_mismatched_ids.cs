using System.Text.Json.Serialization;
using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Tests.Harness;
using Polecat.Tests.StrongTypedId.VogenIds;
using Polecat.TestUtils;

namespace Polecat.Tests.Patching;

/// <summary>
///     #907: on native <c>json</c>, a <c>JSON_MODIFY</c> that adds a key can make the next one through an
///     object overwrite the wrong member, and that was usually the document's id. The damage is left in the
///     rows of a store patched by 5.47.0 or earlier, and the <c>id</c> column is what gives it away.
/// </summary>
public class finding_documents_with_mismatched_ids : OneOffConfigurationsContext
{
    private const string Schema = nameof(finding_documents_with_mismatched_ids);
    internal const string NotAnId = "{\"x\":1}";

    public class Plain
    {
        public Guid Id { get; set; }
        public Dictionary<string, int> Map { get; set; } = new() { ["x"] = 1 };
        public string Name { get; set; } = "name";
    }

    public class ByString
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = "name";
    }

    public class ByInt
    {
        public int Id { get; set; }
        public string Name { get; set; } = "name";
    }

    public class ByLong
    {
        public long Id { get; set; }
        public string Name { get; set; } = "name";
    }

    public readonly record struct WrappedGuid(Guid Value);

    public class ByWrappedGuid
    {
        public WrappedGuid Id { get; set; }
        public string Name { get; set; } = "name";
    }

    public readonly record struct WrappedString(string Value);

    public class ByWrappedString
    {
        public WrappedString Id { get; set; }
        public string Name { get; set; } = "name";
    }

    public class Renamed
    {
        [JsonPropertyName("the key")]
        public Guid Id { get; set; }

        public string Name { get; set; } = "name";
    }

    public class BodyWithoutId
    {
        [JsonIgnore]
        public Guid Id { get; set; }

        public string Name { get; set; } = "name";
    }

    public class SoftDeleted : ISoftDeleted
    {
        public Guid Id { get; set; }
        public bool Deleted { get; set; }
        public DateTimeOffset? DeletedAt { get; set; }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, Action<StoreOptions>? configure = null)
    {
        if (nativeJson) Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "needs the native json type");

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            configure?.Invoke(opts);
        });
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static string TableOf<T>() => $"[{Schema}].[pc_doc_{typeof(T).Name.ToLowerInvariant()}]";

    /// <summary>Puts a document in the state #907 leaves it in. Through text, so it works on either storage.</summary>
    private Task DamageAsync<T>(object id, string path = "$.id", string replacement = "{\"x\":1,\"child\":2}")
    {
        var value = id is string or Guid ? $"'{id}'" : id.ToString();
        return ExecuteAsync($"""
            UPDATE {TableOf<T>()}
            SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), N'{path}', JSON_QUERY(N'{replacement}'))
            WHERE id = {value};
            """);
    }

    private async Task StoreAsync<T>(params T[] documents) where T : notnull
    {
        await using var session = theStore.LightweightSession();
        session.Store<T>(documents);
        await session.SaveChangesAsync(Ct);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task finds_the_damaged_document_and_nothing_else(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var damaged = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(damaged, new Plain { Id = Guid.NewGuid() }, new Plain { Id = Guid.NewGuid() });
        await DamageAsync<Plain>(damaged.Id);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(Ct);

        var mismatch = found.ShouldHaveSingleItem();
        mismatch.DocumentType.ShouldBe(typeof(Plain));
        mismatch.Id.ShouldBe(damaged.Id);
        mismatch.StoredId.ShouldBe("{\"x\":1,\"child\":2}");
        mismatch.TenantId.ShouldBeNull();
        mismatch.Database.ShouldBe(ConnectionSource.DatabaseName);
    }

    /// <summary>
    ///     The real thing: the raw T-SQL from the issue, on a row Polecat stored. It stops being a test the
    ///     day the server is fixed, and the rows it left behind are what this is for regardless.
    /// </summary>
    private async Task ApplyTheBugAsync<T>(object id)
    {
        await ExecuteAsync($"""
            UPDATE {TableOf<T>()} SET data = JSON_MODIFY(data, N'$.text', N'added') WHERE id = '{id}';
            UPDATE {TableOf<T>()} SET data = JSON_MODIFY(data, N'$.map.child', 2) WHERE id = '{id}';
            """);
    }

    [Fact]
    public async Task finds_what_the_bug_leaves_in_the_wrong_member()
    {
        ConfigureFor(true);
        var document = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(document);
        await ApplyTheBugAsync<Plain>(document.Id);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(Ct);

        var mismatch = found.ShouldHaveSingleItem();
        mismatch.Id.ShouldBe(document.Id);
        mismatch.IsCorrupted.ShouldBeFalse();
        mismatch.StoredId.ShouldBe("{\"x\":1,\"child\":2}");
    }

    /// <summary>The repair the docs give, for an id that was overwritten.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_documented_repair_works(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var document = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(document);
        await DamageAsync<Plain>(document.Id);

        await ExecuteAsync($"""
            UPDATE {TableOf<Plain>()}
            SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.id', LOWER(CONVERT(nvarchar(36), id)))
            WHERE id = '{document.Id}';
            """);

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(Ct)).ShouldBeEmpty();

        await using var session = theStore.QuerySession();
        (await session.LoadAsync<Plain>(document.Id, Ct)).ShouldNotBeNull().Id.ShouldBe(document.Id);
    }

    /// <summary>
    ///     This document takes the stray write differently: the member is left unreadable. A read of the
    ///     id raises 13643, a read of the whole column drops the connection, and DBCC CHECKTABLE sees it.
    /// </summary>
    public class LeftCorrupted
    {
        public Guid Id { get; set; }
        public string Alpha { get; set; } = "a";
        public List<Guid> Guids { get; set; } = [];
        public List<long> Longs { get; set; } = [1];
        public Dictionary<string, int> Map { get; set; } = new() { ["x"] = 1 };
        public Dictionary<string, int> Other { get; set; } = new();
        public List<string> Tags { get; set; } = ["t"];
        public string Zulu { get; set; } = "z";
        public int Number { get; set; } = 5;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(100)]
    public async Task a_row_the_server_cannot_read_is_reported_and_the_rest_are_still_read(int batchSize)
    {
        ConfigureFor(true);

        var documents = Enumerable.Range(0, 9).Select(_ => new LeftCorrupted { Id = Guid.NewGuid() }).ToArray();
        await StoreAsync(documents);

        var sorted = await IdsInKeyOrderAsync<LeftCorrupted>();
        var corrupted = sorted[4];
        var mismatched = sorted[7];

        await ApplyTheBugAsync<LeftCorrupted>(corrupted);
        await DamageAsync<LeftCorrupted>(mismatched);

        await using (var conn = await OpenConnectionAsync())
        await using (var probe = conn.CreateCommand())
        {
            probe.CommandText = $"SELECT JSON_VALUE(data, '$.id') FROM {TableOf<LeftCorrupted>()} WHERE id = '{corrupted}'";
            var error = await Should.ThrowAsync<SqlException>(() => probe.ExecuteScalarAsync(Ct));
            error.Number.ShouldBe(13643);
        }

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<LeftCorrupted>(batchSize, Ct);

        found.Count.ShouldBe(2);
        found.Single(x => x.IsCorrupted).Id.ShouldBe(corrupted);
        found.Single(x => x.IsCorrupted).StoredId.ShouldBeNull();
        found.Single(x => !x.IsCorrupted).Id.ShouldBe(mismatched);
    }

    private async Task<Guid[]> IdsInKeyOrderAsync<T>()
    {
        var ids = new List<Guid>();
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT id FROM {TableOf<T>()} ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) ids.Add(reader.GetGuid(0));
        return ids.ToArray();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_string_id_is_compared_by_its_characters(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        await StoreAsync(new ByString { Id = "abc" }, new ByString { Id = "fine" }, new ByString { Id = "it's" });

        // The database's collation would call these equal
        await ExecuteAsync($"UPDATE {TableOf<ByString>()} SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.id', 'ABC') WHERE id = 'abc';");

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByString>(Ct);

        var mismatch = found.ShouldHaveSingleItem();
        mismatch.Id.ShouldBe("abc");
        mismatch.StoredId.ShouldBe("ABC");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task numeric_ids(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        await StoreAsync(new ByInt { Id = 1 }, new ByInt { Id = 2 });
        await StoreAsync(new ByLong { Id = 9_000_000_000 }, new ByLong { Id = 9_000_000_001 });
        await DamageAsync<ByInt>(2);
        await DamageAsync<ByLong>(9_000_000_001);

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByInt>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(2);
        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByLong>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(9_000_000_001L);
    }

    /// <summary>A wrapper with no converter is an object in the body, and one with a converter is a scalar.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task strong_typed_ids_of_either_shape(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var damagedGuid = new WrappedGuid(Guid.NewGuid());
        await StoreAsync(new ByWrappedGuid { Id = damagedGuid },
            new ByWrappedGuid { Id = new WrappedGuid(Guid.NewGuid()) });
        await DamageAsync<ByWrappedGuid>(damagedGuid.Value);

        var damagedString = new WrappedString("damaged");
        await StoreAsync(new ByWrappedString { Id = damagedString },
            new ByWrappedString { Id = new WrappedString("fine") });
        await DamageAsync<ByWrappedString>(damagedString.Value);

        var vogen = VogenInvoiceId.From(Guid.NewGuid());
        await StoreAsync(new VogenInvoice { Id = vogen }, new VogenInvoice { Id = VogenInvoiceId.From(Guid.NewGuid()) });
        await DamageAsync<VogenInvoice>(vogen.Value);

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByWrappedGuid>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(damagedGuid);
        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByWrappedString>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(damagedString);
        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<VogenInvoice>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(vogen);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task reads_the_key_the_serializer_wrote(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var damaged = new Renamed { Id = Guid.NewGuid() };
        await StoreAsync(damaged, new Renamed { Id = Guid.NewGuid() });
        await DamageAsync<Renamed>(damaged.Id, "$.\"the key\"");

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Renamed>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(damaged.Id);
    }

    /// <summary>Nothing in the body to disagree with the column, which is not the same as a mismatch.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_body_with_no_id_is_not_reported(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        await StoreAsync(new BodyWithoutId { Id = Guid.NewGuid() });

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<BodyWithoutId>(Ct)).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_id_is_reported(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var damaged = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(damaged);
        await ExecuteAsync($"""
            UPDATE {TableOf<Plain>()} SET data = REPLACE(CAST(data AS nvarchar(max)), '"{damaged.Id}"', 'null');
            """);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(Ct);

        var mismatch = found.ShouldHaveSingleItem();
        mismatch.Id.ShouldBe(damaged.Id);
        mismatch.StoredId.ShouldBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_soft_deleted_document_is_still_stored_and_is_reported(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var damaged = new SoftDeleted { Id = Guid.NewGuid() };
        await StoreAsync(damaged);
        await DamageAsync<SoftDeleted>(damaged.Id);

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<SoftDeleted>(damaged.Id);
            await session.SaveChangesAsync(Ct);
        }

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<SoftDeleted>(Ct))
            .ShouldHaveSingleItem().Id.ShouldBe(damaged.Id);
    }

    /// <summary>Damage on either side of every page boundary, and an exact number of pages to finish on.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    public async Task pages_lose_nothing(int batchSize)
    {
        ConfigureFor(false);

        var documents = Enumerable.Range(0, 6).Select(_ => new Plain { Id = Guid.NewGuid() }).ToArray();
        await StoreAsync(documents);

        foreach (var document in documents) await DamageAsync<Plain>(document.Id);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(batchSize, Ct);

        found.Select(x => (Guid)x.Id).OrderBy(x => x)
            .ShouldBe(documents.Select(x => x.Id).OrderBy(x => x));
    }

    /// <summary>A partitioned table's key carries the partition column, so one id can be two rows.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task one_id_on_two_rows_is_two_rows_wherever_the_page_ends(int batchSize)
    {
        ConfigureFor(false);

        var documents = Enumerable.Range(0, 4).Select(_ => new Plain { Id = Guid.NewGuid() }).ToArray();
        await StoreAsync(documents);

        await ExecuteAsync($"""
            DECLARE @pk sysname = (SELECT name FROM sys.key_constraints
                                   WHERE parent_object_id = OBJECT_ID('{TableOf<Plain>()}') AND type = 'PK');
            EXEC('ALTER TABLE {TableOf<Plain>()} DROP CONSTRAINT [' + @pk + ']');
            ALTER TABLE {TableOf<Plain>()} ADD part int NOT NULL DEFAULT 1;
            """);
        await ExecuteAsync($"""
            ALTER TABLE {TableOf<Plain>()} ADD CONSTRAINT pk_widened PRIMARY KEY (id, part);
            INSERT INTO {TableOf<Plain>()} (id, part, data, version, last_modified, created_at)
            SELECT id, 2, data, version, last_modified, created_at FROM {TableOf<Plain>()};
            UPDATE {TableOf<Plain>()}
            SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.id', JSON_QUERY(N'{NotAnId}'));
            """);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(batchSize, Ct);

        found.Count.ShouldBe(8);
        found.Select(x => (Guid)x.Id).Distinct().Count().ShouldBe(4);
    }

    [Fact]
    public async Task a_page_size_has_to_mean_something()
    {
        ConfigureFor(false);

        await Should.ThrowAsync<ArgumentOutOfRangeException>(
            () => theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(0, Ct));
    }

    /// <summary>The bodies aren't read into the client, and nothing is created by asking.</summary>
    [Fact]
    public async Task a_table_that_was_never_created_is_an_empty_answer()
    {
        ConfigureFor(false);
        await ExecuteAsync($"DROP TABLE IF EXISTS {TableOf<ByLong>()};");

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<ByLong>(Ct)).ShouldBeEmpty();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT OBJECT_ID('{TableOf<ByLong>()}')";
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe(DBNull.Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task conjoined_tenants_are_told_apart(bool nativeJson)
    {
        ConfigureFor(nativeJson, opts => opts.Schema.For<Plain>().MultiTenanted());

        var id = Guid.NewGuid();
        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Store(new Plain { Id = id });
            await session.SaveChangesAsync(Ct);
        }

        await ExecuteAsync($"""
            UPDATE {TableOf<Plain>()}
            SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.id', JSON_QUERY(N'{NotAnId}'))
            WHERE tenant_id = 'blue';
            """);

        foreach (var batchSize in new[] { 1, 2, 5 })
        {
            var mismatch = (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(batchSize, Ct))
                .ShouldHaveSingleItem();
            mismatch.Id.ShouldBe(id);
            mismatch.TenantId.ShouldBe("blue");
        }
    }

    [Fact]
    public async Task the_store_wide_form_walks_every_configured_type()
    {
        ConfigureFor(false, opts =>
        {
            opts.Schema.For<Plain>();
            opts.Schema.For<ByInt>();
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var plain = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(plain);
        await StoreAsync(new ByInt { Id = 1 });
        await DamageAsync<Plain>(plain.Id);
        await DamageAsync<ByInt>(1);

        var found = await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync(Ct);

        found.Select(x => x.DocumentType).ShouldBe([typeof(ByInt), typeof(Plain)], ignoreOrder: true);
    }

    /// <summary>
    ///     What a mismatch can't see, pinned so the docs stay honest. The stray write lands on another
    ///     member of the same object, and which one depends on the keys.
    /// </summary>
    [Fact]
    public async Task damage_to_another_member_goes_unnoticed()
    {
        ConfigureFor(false);
        var document = new Plain { Id = Guid.NewGuid() };
        await StoreAsync(document);
        await DamageAsync<Plain>(document.Id, "$.name");

        (await theStore.Advanced.FindDocumentsWithMismatchedIdsAsync<Plain>(Ct)).ShouldBeEmpty();
    }
}

public class finding_documents_with_mismatched_ids_across_databases : IAsyncLifetime
{
    private const string Schema = "id_audit";

    private static readonly string DbA = ConnectionSource.Scoped("id_audit_a");
    private static readonly string DbB = ConnectionSource.Scoped("id_audit_b");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropDatabasesAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync(Ct);
        }
    }

    public ValueTask DisposeAsync() => new(DropDatabasesAsync());

    private static async Task DropDatabasesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task every_tenant_database_is_read()
    {
        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(DbA);
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("alpha", ConnectionSource.ConnectionStringFor(DbA));
                tenancy.AddTenant("beta", ConnectionSource.ConnectionStringFor(DbB));
            });
        });

        var id = Guid.NewGuid();
        foreach (var tenant in new[] { "alpha", "beta" })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Store(new finding_documents_with_mismatched_ids.Plain { Id = id });
            await session.SaveChangesAsync(Ct);
        }

        await using (var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(DbB)))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                UPDATE [{Schema}].[pc_doc_plain]
                SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.id', JSON_QUERY(N'{finding_documents_with_mismatched_ids.NotAnId}'));
                """;
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var found = await store.Advanced.FindDocumentsWithMismatchedIdsAsync<finding_documents_with_mismatched_ids.Plain>(Ct);

        found.ShouldHaveSingleItem().Database.ShouldBe(DbB);
    }
}
