using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Documents;

/// <summary>
///     #919: what a string id may be is the code page's business, not ASCII's. These run on databases of
///     their own, because a database's default collation is what a column takes and what a <c>varchar</c>
///     parameter is encoded with.
/// </summary>
public class string_id_by_collation_tests: IAsyncLifetime
{
    private const string Utf8Collation = "Latin1_General_100_CI_AS_SC_UTF8";
    private const string JapaneseCollation = "Japanese_CI_AS";
    private const string DefaultCollation = "SQL_Latin1_General_CP1_CI_AS";

    private static readonly string Utf8Database = ConnectionSource.Scoped("ids_utf8");
    private static readonly string JapaneseDatabase = ConnectionSource.Scoped("ids_japanese");
    private static readonly string LatinDatabase = ConnectionSource.Scoped("ids_latin");

    private readonly List<DocumentStore> _stores = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await recreateAsync(Utf8Database, Utf8Collation);
        await recreateAsync(JapaneseDatabase, JapaneseCollation);
        await recreateAsync(LatinDatabase, DefaultCollation);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores)
        {
            store.Dispose();
        }

        await dropAsync(Utf8Database);
        await dropAsync(JapaneseDatabase);
        await dropAsync(LatinDatabase);
    }

    private static async Task recreateAsync(string database, string collation)
    {
        await dropAsync(database);
        await masterAsync($"CREATE DATABASE [{database}] COLLATE {collation};");
    }

    private static async Task dropAsync(string database)
    {
        SqlConnection.ClearAllPools();
        await masterAsync($"""
            IF DB_ID('{database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{database}];
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

    private DocumentStore storeOn(string database, Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionStringFor(database),
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = "ids",
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        configure?.Invoke(options);
        var store = new DocumentStore(options);
        _stores.Add(store);
        return store;
    }

    private static StringDoc Doc(string id) => new() { Id = id, Name = $"named {id}" };

    private static async Task storeAsync(DocumentStore store, string id)
    {
        await using var session = store.LightweightSession();
        session.Store(Doc(id));
        await session.SaveChangesAsync(Ct);
    }

    private static async Task<string?> nameOfAsync(DocumentStore store, string id)
    {
        await using var query = store.QuerySession();
        return (await query.LoadAsync<StringDoc>(id, Ct))?.Name;
    }

    [Theory]
    [InlineData("東京")]
    [InlineData("大阪")]
    [InlineData("Москва")]
    [InlineData("😀")]
    [InlineData("ā")]
    [InlineData("café")]
    public async Task a_utf8_database_refuses_nothing(string id)
    {
        var store = storeOn(Utf8Database);

        await storeAsync(store, id);

        (await nameOfAsync(store, id)).ShouldBe($"named {id}");

        await using var query = store.QuerySession();
        (await query.LoadManyAsync<StringDoc>([id], Ct)).Single().Id.ShouldBe(id);
        (await query.Query<StringDoc>().Where(x => x.Id == id).ToListAsync(Ct)).Single().Id.ShouldBe(id);
    }

    [Fact]
    public async Task a_utf8_database_keeps_two_ids_two_rows()
    {
        var store = storeOn(Utf8Database);

        await storeAsync(store, "東京");
        await storeAsync(store, "大阪");

        await using var query = store.QuerySession();
        (await query.Query<StringDoc>().CountAsync(Ct)).ShouldBe(2);
        (await nameOfAsync(store, "東京")).ShouldBe("named 東京");
    }

    [Fact]
    public async Task a_utf8_database_stores_a_stream_key()
    {
        var store = storeOn(Utf8Database, opts => opts.Events.StreamIdentity = StreamIdentity.AsString);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream("東京", new Polecat.Tests.Events.CityVisited("tokyo"));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = store.QuerySession();
        (await query.Events.FetchStreamAsync("東京", token: Ct)).Count.ShouldBe(1);
        (await query.Events.FetchStreamAsync("大阪", token: Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task bulk_insert_on_a_utf8_database_refuses_nothing()
    {
        var store = storeOn(Utf8Database);

        await store.Advanced.BulkInsertAsync([Doc("東京"), Doc("大阪")], Ct);

        await using var query = store.QuerySession();
        (await query.Query<StringDoc>().CountAsync(Ct)).ShouldBe(2);
    }

    [Fact]
    public async Task a_japanese_database_holds_what_its_own_code_page_holds()
    {
        var store = storeOn(JapaneseDatabase);

        await storeAsync(store, "東京");
        await storeAsync(store, "大阪");

        (await nameOfAsync(store, "東京")).ShouldBe("named 東京");
        (await nameOfAsync(store, "大阪")).ShouldBe("named 大阪");
    }

    [Fact]
    public async Task a_japanese_database_refuses_what_a_western_one_would_keep()
    {
        var store = storeOn(JapaneseDatabase);

        await using var session = store.LightweightSession();
        session.Store(Doc("café"));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.CodePage.ShouldBe(932);
        refused.Message.ShouldContain("code page 932");
    }

    [Fact]
    public async Task each_tenant_database_answers_for_itself()
    {
        var store = storeOn(LatinDatabase, opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("western", ConnectionSource.ConnectionStringFor(LatinDatabase));
                tenancy.AddTenant("japanese", ConnectionSource.ConnectionStringFor(JapaneseDatabase));
                tenancy.AddTenant("unicode", ConnectionSource.ConnectionStringFor(Utf8Database));
            });
        });

        // In this order, so that a store-wide answer would be the wrong one for the next two
        await using (var western = store.LightweightSession("western"))
        {
            western.Store(Doc("東京"));
            var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => western.SaveChangesAsync(Ct));
            refused.CodePage.ShouldBe(1252);
        }

        await using (var japanese = store.LightweightSession("japanese"))
        {
            japanese.Store(Doc("東京"));
            await japanese.SaveChangesAsync(Ct);
        }

        await using (var japanese = store.LightweightSession("japanese"))
        {
            japanese.Store(Doc("café"));
            var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => japanese.SaveChangesAsync(Ct));
            refused.CodePage.ShouldBe(932);
        }

        await using (var unicode = store.LightweightSession("unicode"))
        {
            unicode.Store(Doc("東京"), Doc("café"));
            await unicode.SaveChangesAsync(Ct);
        }

        await using var westernQuery = store.QuerySession("western");
        (await westernQuery.LoadAsync<StringDoc>("東京", Ct)).ShouldBeNull();

        await using var japaneseQuery = store.QuerySession("japanese");
        (await japaneseQuery.LoadAsync<StringDoc>("東京", Ct))!.Id.ShouldBe("東京");
    }

    [Fact]
    public async Task bulk_insert_answers_for_the_tenants_database()
    {
        var store = storeOn(LatinDatabase, opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("western", ConnectionSource.ConnectionStringFor(LatinDatabase));
                tenancy.AddTenant("japanese", ConnectionSource.ConnectionStringFor(JapaneseDatabase));
            });
        });

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() =>
            store.Advanced.BulkInsertAsync([Doc("東京")], BulkInsertMode.InsertsOnly, 200, "western", Ct));

        await store.Advanced.BulkInsertAsync([Doc("東京")], BulkInsertMode.InsertsOnly, 200, "japanese", Ct);

        await using var query = store.QuerySession("japanese");
        (await query.LoadAsync<StringDoc>("東京", Ct)).ShouldNotBeNull();
    }

    /// <summary>
    ///     <c>ALTER DATABASE ... COLLATE</c> is the obvious thing to reach for after the refusal, and it
    ///     changes the default and leaves every existing column as it was. Going by the database alone
    ///     would let the id straight back into a column that can't hold it.
    /// </summary>
    [Fact]
    public async Task a_column_that_kept_its_old_collation_is_still_refused()
    {
        var store = storeOn(LatinDatabase);
        await storeAsync(store, "plain");

        SqlConnection.ClearAllPools();
        await masterAsync($"""
            ALTER DATABASE [{LatinDatabase}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            ALTER DATABASE [{LatinDatabase}] COLLATE {Utf8Collation};
            ALTER DATABASE [{LatinDatabase}] SET MULTI_USER;
            """);

        var altered = storeOn(LatinDatabase);

        await using (var session = altered.LightweightSession())
        {
            session.Store(Doc("東京"));
            var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
            refused.CodePage.ShouldBe(1252);
        }

        await using var query = altered.QuerySession();
        (await query.Query<StringDoc>().CountAsync(Ct)).ShouldBe(1);
    }

    /// <summary>
    ///     The other way around: a <c>varchar</c> parameter is encoded with the database's code page before
    ///     the column gets a say, so a column declared with a wider one can't save the value.
    /// </summary>
    [Fact]
    public async Task a_column_wider_than_the_database_cannot_be_reached_through_a_varchar_parameter()
    {
        var store = storeOn(LatinDatabase);
        await storeAsync(store, "plain");

        await using (var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(LatinDatabase)))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DECLARE @pk sysname = (SELECT name FROM sys.key_constraints
                    WHERE parent_object_id = OBJECT_ID('ids.pc_doc_stringdoc') AND type = 'PK');
                EXEC('ALTER TABLE ids.pc_doc_stringdoc DROP CONSTRAINT ' + @pk);
                ALTER TABLE ids.pc_doc_stringdoc ALTER COLUMN id varchar(250) COLLATE Japanese_CI_AS NOT NULL;
                ALTER TABLE ids.pc_doc_stringdoc ADD CONSTRAINT pk_widened PRIMARY KEY (id);
                """;
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var altered = storeOn(LatinDatabase, opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        await using var session = altered.LightweightSession();
        session.Store(Doc("東京"));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.CodePage.ShouldBe(1252);
    }
}
