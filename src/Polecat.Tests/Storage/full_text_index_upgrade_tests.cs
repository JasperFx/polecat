using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Tests.Storage;

/// <summary>
///     What happens to a database an earlier version created when #835 and #839 reach it. The tokenizer
///     lives in the trigger, so the fix is a schema change, and it changes what a stored row should hold.
/// </summary>
/// <remarks>
///     <para>
///         Each fact installs the trigger as version 5.47.0 stored it, copied from
///         <c>sys.sql_modules</c>, and writes documents through it, so the rows are the rows that version
///         really produced. A fresh create would prove nothing here, and a catalog left over from an
///         earlier run would prove less: the shared database keeps old tables, so these drop their schema
///         first.
///     </para>
///     <para>
    ///         ⚠️ <b>Nothing repairs an established index at startup, and that is the design.</b> A document
    ///         with SOME tokens is invisible to the backfill. One with NONE is what it looks for, and left to
    ///         it a store with 8,000 long documents timed out inside the first request, rolled back, and did
    ///         it again on the next one. <see cref="AdvancedOperations.RebuildFullTextIndexAsync{T}(CancellationToken)" /> is the
    ///         one step for both.
    ///     </para>
/// </remarks>
public class full_text_index_upgrade_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    public class TaggedNote
    {
        public string Id { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    public class NumberedNote
    {
        public int Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private const string Schema = "full_text_index_upgrade_tests";
    private const string Trigger = $"[{Schema}].[tr_pc_ft_article]";
    private const string TokenTable = $"[{Schema}].[pc_ft_article]";

    private static readonly string LongBody = new string('x', 2500) + " needle " + new string('y', 2500);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static void Declare(StoreOptions opts, bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        opts.UseNativeJsonType = nativeJson;
        opts.Schema.For<Article>().FullTextIndex(x => x.Body);
    }

    /// <summary>
    ///     A store on a database an earlier version created, with a process that has not touched it yet.
    /// </summary>
    private DocumentStore anUpgradedStore(bool nativeJson, AutoCreate autoCreate = AutoCreate.All)
    {
        ConfigureStore(opts =>
        {
            Declare(opts, nativeJson);
            opts.AutoCreateSchemaObjects = autoCreate;
        });

        return theStore;
    }

    private const string OldPunctuation = """
                                          '!"#$%&()*+,-./:;<=>?@[\]^_{|}~'''
                                          """;

    /// <summary>The trigger as version 5.47.0 stored it.</summary>
    private static string OldTrigger(string member) =>
        $"""
         CREATE TRIGGER {Trigger} ON [{Schema}].[pc_doc_article] AFTER INSERT, UPDATE, DELETE AS BEGIN
             SET NOCOUNT ON;
             DELETE ft FROM {TokenTable} ft INNER JOIN deleted d ON ft.doc_id = d.id;
             INSERT INTO {TokenTable} (doc_id, tenant_id, member, term, pos)
             SELECT i.id, '*DEFAULT*', 'Body', s.value, s.ordinal
             FROM inserted i
             CROSS APPLY STRING_SPLIT(
             LOWER(TRANSLATE(
                 CAST(JSON_VALUE(i.data, '{member}') AS nvarchar(max)),
                 {OldPunctuation},
                 '                               ')), N' ', 1) s
         WHERE LEN(s.value) > 0
         END
         """;

    private async Task execAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    /// <summary>
    ///     The state 5.47.0 left: its trigger in place, and these documents written through it.
    /// </summary>
    private async Task anOldDatabaseHoldingAsync(bool nativeJson, params (Guid Id, string Body)[] documents)
    {
        ConfigureStore(opts => Declare(opts, nativeJson));

        // First-use ensure runs once per store, so running it here, before the swap, is what keeps this
        // store from putting the current trigger straight back on the first write below.
        await using (var query = theStore.QuerySession())
        {
            await ((Polecat.Internal.QuerySession)query).EnsureDocumentTableAsync(typeof(Article), Ct);
        }

        await execAsync($"DROP TRIGGER {Trigger}");
        await execAsync(OldTrigger("$.body"));

        await using var session = theStore.LightweightSession();
        foreach (var (id, body) in documents) session.Store(new Article { Id = id, Body = body });
        await session.SaveChangesAsync(Ct);
    }

    private async Task<string[]> termsAsync(Guid id)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT term FROM {TokenTable} WHERE doc_id = @id ORDER BY pos";
        cmd.Parameters.AddWithValue("@id", id);

        var terms = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) terms.Add(reader.GetString(0));
        return terms.ToArray();
    }

    private static async Task<Guid[]> searchAsync(IDocumentStore store, string text)
    {
        await using var session = store.QuerySession();
        return (await session.Query<Article>().Where(x => x.Body.PlainTextSearch(text)).ToListAsync(Ct))
            .Select(x => x.Id).ToArray();
    }

    private static async Task<SchemaPatchDifference> triggerDifferenceAsync(IDocumentStore store)
    {
        var mapping = ((DocumentStore)store).Options.Providers.GetProvider(typeof(Article)).Mapping;

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        var migration = await SchemaMigration.DetermineAsync(conn, Ct, new FullTextTrigger(mapping, mapping.FullTextIndexes));
        return migration.Difference;
    }

    // ---- the schema -----------------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_old_trigger_is_seen_as_different_and_replaced_by_first_use(bool nativeJson)
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (id, "plain words"));

        var store = anUpgradedStore(nativeJson);
        (await triggerDifferenceAsync(store)).ShouldBe(SchemaPatchDifference.Update);

        await searchAsync(store, "plain");

        // None, and it stays None: a body SQL Server stores differently from how it was declared reports
        // drift on every pass, and every pass would then drop and re-create the trigger.
        (await triggerDifferenceAsync(store)).ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_old_trigger_is_replaced_by_a_whole_database_migration(bool nativeJson)
    {
        await anOldDatabaseHoldingAsync(nativeJson);

        var store = anUpgradedStore(nativeJson);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await triggerDifferenceAsync(store)).ShouldBe(SchemaPatchDifference.None);
        await store.Database.AssertDatabaseMatchesConfigurationAsync(Ct);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_token_table_needs_no_migration(bool nativeJson)
    {
        await anOldDatabaseHoldingAsync(nativeJson);

        var store = anUpgradedStore(nativeJson);
        var mapping = store.Options.Providers.GetProvider(typeof(Article)).Mapping;

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        var migration = await SchemaMigration.DetermineAsync(conn, Ct, new FullTextTokenTable(mapping));

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_after_the_upgrade_uses_the_new_tokenizer(bool nativeJson)
    {
        await anOldDatabaseHoldingAsync(nativeJson);

        var store = anUpgradedStore(nativeJson);
        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = "quick\nbrown " + new string('z', 300) });
            await session.SaveChangesAsync(Ct);
        }

        (await searchAsync(store, "brown")).ShouldBe([id]);
    }

    // ---- what repairs itself, and what does not ---------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_document_with_no_tokens_is_left_for_the_rebuild(bool nativeJson)
    {
        var longOne = Guid.NewGuid();
        var other = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (longOne, LongBody), (other, "plain words"));

        (await termsAsync(longOne)).ShouldBeEmpty();

        var store = anUpgradedStore(nativeJson);

        // Not an endorsement of the result either. Left to the backfill, this is one statement over every
        // such document inside whichever request comes first, and at 8,000 of them that statement never
        // finishes. See the remarks on FullTextIndex.BackfillStatements.
        (await searchAsync(store, "plain")).ShouldBe([other]);
        (await searchAsync(store, "needle")).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_backfill_still_covers_what_it_always_could_on_an_established_index(bool nativeJson)
    {
        var old = Guid.NewGuid();
        var missed = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (old, "plain words"), (missed, "other words"));

        // A document with no tokens that is NOT one the old tokenizer failed on: written while the trigger
        // was absent, which is what the backfill exists for.
        await execAsync($"DELETE FROM {TokenTable} WHERE doc_id = '{missed}'");

        var store = anUpgradedStore(nativeJson);

        (await searchAsync(store, "other")).ShouldBe([missed]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task changing_the_tokenizer_still_backfills_a_long_member(bool nativeJson)
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (id, LongBody));

        // The Porter key has no rows, so this is a new index as far as the backfill can tell, and the
        // documented promise is that a switch of tokenizer covers everything.
        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<Article>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body);
        });

        (await searchAsync(theStore, "needle")).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_document_with_stale_tokens_is_left_alone_by_startup(bool nativeJson)
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (id, "quick\nbrown fox"));

        (await termsAsync(id)).ShouldBe(["quick\nbrown", "fox"]);

        var store = anUpgradedStore(nativeJson);

        // Not an endorsement of the result. This is the documented gap, and the reason the rebuild
        // exists: a fact that says so, so that closing it is a decision and not an accident.
        (await searchAsync(store, "brown")).ShouldBeEmpty();
        (await termsAsync(id)).ShouldBe(["quick\nbrown", "fox"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task writing_a_stale_document_again_repairs_just_that_document(bool nativeJson)
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (id, "quick\nbrown fox"));

        var store = anUpgradedStore(nativeJson);
        await using (var session = store.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = "quick\nbrown fox" });
            await session.SaveChangesAsync(Ct);
        }

        (await termsAsync(id)).ShouldBe(["quick", "brown", "fox"]);
    }

    // ---- the rebuild ------------------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_rebuild_replaces_stale_tokens(bool nativeJson)
    {
        var stale = Guid.NewGuid();
        var longOne = Guid.NewGuid();
        var clean = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson,
            (stale, "quick\nbrown fox"), (longOne, LongBody), (clean, "plain words"));

        var store = anUpgradedStore(nativeJson);
        await store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct);

        (await termsAsync(stale)).ShouldBe(["quick", "brown", "fox"]);
        (await termsAsync(clean)).ShouldBe(["plain", "words"]);
        (await searchAsync(store, "brown")).ShouldBe([stale]);
        (await searchAsync(store, "needle")).ShouldBe([longOne]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_rebuild_changes_nothing_a_reader_of_the_document_can_see(bool nativeJson)
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(nativeJson, (id, "quick\nbrown fox"));

        var store = anUpgradedStore(nativeJson);

        const string columns = $"""
                                SELECT CONVERT(nvarchar(max), data), version, last_modified, created_at, dotnet_type
                                FROM [{Schema}].[pc_doc_article]
                                """;

        var before = await rowAsync(columns);
        await store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct);

        (await rowAsync(columns)).ShouldBe(before);
    }

    private async Task<object?[]> rowAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        await reader.ReadAsync(Ct);

        var values = new object?[reader.FieldCount];
        reader.GetValues(values!);
        return values;
    }

    [Fact]
    public async Task a_rebuild_is_repeatable()
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(ConnectionSource.SupportsNativeJson, (id, "quick\nbrown fox"));

        var store = anUpgradedStore(ConnectionSource.SupportsNativeJson);
        await store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct);
        await store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct);

        (await termsAsync(id)).ShouldBe(["quick", "brown", "fox"]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(100)]
    public async Task a_rebuild_reaches_every_document_whatever_the_batch_size(int batchSize)
    {
        var ids = Enumerable.Range(0, 7).Select(_ => Guid.NewGuid()).ToArray();
        await anOldDatabaseHoldingAsync(ConnectionSource.SupportsNativeJson,
            ids.Select(x => (x, "alpha\nbravo")).ToArray());

        var store = anUpgradedStore(ConnectionSource.SupportsNativeJson);
        await store.Advanced.RebuildFullTextIndexAsync<Article>(batchSize, Ct);

        (await searchAsync(store, "bravo")).OrderBy(x => x).ShouldBe(ids.OrderBy(x => x));
    }

    [Fact]
    public async Task a_rebuild_pages_a_string_key()
    {
        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<TaggedNote>().FullTextIndex(x => x.Body);
        });

        var ids = new[] { "alpha", "bravo", "Charlie", "delta" };
        await using (var session = theStore.LightweightSession())
        {
            foreach (var id in ids) session.Store(new TaggedNote { Id = id, Body = "alpha bravo" });
            await session.SaveChangesAsync(Ct);
        }

        await execAsync($"DELETE FROM [{Schema}].[pc_ft_taggednote]");

        await theStore.Advanced.RebuildFullTextIndexAsync<TaggedNote>(batchSize: 1, token: Ct);

        await using var query = theStore.QuerySession();
        (await query.Query<TaggedNote>().Where(x => x.Body.PlainTextSearch("bravo")).CountAsync(Ct))
            .ShouldBe(ids.Length);
    }

    [Fact]
    public async Task a_rebuild_pages_a_numeric_key()
    {
        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<NumberedNote>().FullTextIndex(x => x.Body);
        });

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 1; i <= 5; i++) session.Store(new NumberedNote { Body = "alpha bravo" });
            await session.SaveChangesAsync(Ct);
        }

        await execAsync($"DELETE FROM [{Schema}].[pc_ft_numberednote]");

        await theStore.Advanced.RebuildFullTextIndexAsync<NumberedNote>(batchSize: 2, token: Ct);

        await using var query = theStore.QuerySession();
        (await query.Query<NumberedNote>().Where(x => x.Body.PlainTextSearch("bravo")).CountAsync(Ct))
            .ShouldBe(5);
    }

    [Fact]
    public async Task a_rebuild_keeps_a_tenant_to_its_own_tokens()
    {
        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<Article>().MultiTenanted().FullTextIndex(x => x.Body);
        });

        // One id in two tenants, which is what a rebuild paged on id alone would lose.
        var shared = Guid.NewGuid();
        var other = Guid.NewGuid();

        await using (var red = theStore.LightweightSession("red"))
        {
            red.Store(new Article { Id = shared, Body = "alpha red" });
            red.Store(new Article { Id = other, Body = "alpha crimson" });
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = theStore.LightweightSession("blue"))
        {
            blue.Store(new Article { Id = shared, Body = "alpha blue" });
            await blue.SaveChangesAsync(Ct);
        }

        await execAsync($"DELETE FROM {TokenTable}");

        await theStore.Advanced.RebuildFullTextIndexAsync<Article>(batchSize: 1, token: Ct);

        await using var redQuery = theStore.QuerySession("red");
        (await redQuery.Query<Article>().Where(x => x.Body.PlainTextSearch("alpha")).CountAsync(Ct)).ShouldBe(2);
        (await redQuery.Query<Article>().Where(x => x.Body.PlainTextSearch("blue")).CountAsync(Ct)).ShouldBe(0);

        await using var blueQuery = theStore.QuerySession("blue");
        (await blueQuery.Query<Article>().Where(x => x.Body.PlainTextSearch("alpha")).CountAsync(Ct)).ShouldBe(1);
        (await blueQuery.Query<Article>().Where(x => x.Body.PlainTextSearch("red")).CountAsync(Ct)).ShouldBe(0);
    }

    [Fact]
    public async Task a_rebuild_covers_every_database_of_a_tenancy()
    {
        var databases = new[] { ConnectionSource.Scoped("ft_rebuild_a"), ConnectionSource.Scoped("ft_rebuild_b") };

        await using var master = new SqlConnection(ConnectionSource.MasterConnectionString);
        await master.OpenAsync(Ct);

        async Task dropAndCreateAsync(bool create)
        {
            foreach (var database in databases)
            {
                await using var cmd = master.CreateCommand();
                cmd.CommandText = $"""
                                   IF DB_ID('{database}') IS NOT NULL
                                   BEGIN
                                       ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                                       DROP DATABASE [{database}];
                                   END
                                   {(create ? $"CREATE DATABASE [{database}];" : "")}
                                   """;
                await cmd.ExecuteNonQueryAsync(Ct);
            }
        }

        await dropAndCreateAsync(create: true);

        try
        {
            await using var store = new DocumentStore(optionsForTenants());

            foreach (var tenant in new[] { "a", "b" })
            {
                await using var session = store.LightweightSession(tenant);
                session.Store(new Article { Id = Guid.NewGuid(), Body = "alpha bravo" });
                await session.SaveChangesAsync(Ct);
            }

            foreach (var database in databases)
            {
                await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(database));
                await conn.OpenAsync(Ct);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"DELETE FROM {TokenTable}";
                await cmd.ExecuteNonQueryAsync(Ct);
            }

            await store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct);

            foreach (var tenant in new[] { "a", "b" })
            {
                await using var query = store.QuerySession(tenant);
                (await query.Query<Article>().Where(x => x.Body.PlainTextSearch("bravo")).CountAsync(Ct))
                    .ShouldBe(1);
            }
        }
        finally
        {
            SqlConnection.ClearAllPools();
            await dropAndCreateAsync(create: false);
        }

        StoreOptions optionsForTenants()
        {
            var options = new StoreOptions
            {
                ConnectionString = ConnectionSource.ConnectionStringFor(databases[0]),
                DatabaseSchemaName = Schema,
                AutoCreateSchemaObjects = AutoCreate.All,
                UseNativeJsonType = ConnectionSource.SupportsNativeJson
            };

            options.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("a", ConnectionSource.ConnectionStringFor(databases[0]));
                tenancy.AddTenant("b", ConnectionSource.ConnectionStringFor(databases[1]));
            });

            options.Schema.For<Article>().FullTextIndex(x => x.Body);
            return options;
        }
    }

    [Fact]
    public async Task a_rebuild_refuses_to_run_through_a_trigger_it_did_not_declare()
    {
        var id = Guid.NewGuid();
        await anOldDatabaseHoldingAsync(ConnectionSource.SupportsNativeJson, (id, "quick\nbrown fox"));

        // AutoCreate.None is the path where nothing replaces the trigger. Carrying on would rewrite every
        // row through the old tokenizer and report success, having changed nothing.
        var store = anUpgradedStore(ConnectionSource.SupportsNativeJson, AutoCreate.None);

        var refusal = await Should.ThrowAsync<InvalidOperationException>(
            () => store.Advanced.RebuildFullTextIndexAsync<Article>(token: Ct));

        refusal.Message.ShouldContain("ApplyAllConfiguredChangesToDatabaseAsync");
        (await termsAsync(id)).ShouldBe(["quick\nbrown", "fox"]);
    }

    [Fact]
    public async Task a_type_with_no_full_text_index_is_refused()
    {
        ConfigureStore(opts => opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson);

        await Should.ThrowAsync<InvalidOperationException>(
            () => theStore.Advanced.RebuildFullTextIndexAsync<TaggedNote>(token: Ct));
    }

    [Fact]
    public async Task a_batch_size_of_zero_is_refused()
    {
        ConfigureStore(opts => Declare(opts, ConnectionSource.SupportsNativeJson));

        await Should.ThrowAsync<ArgumentOutOfRangeException>(
            () => theStore.Advanced.RebuildFullTextIndexAsync<Article>(batchSize: 0, token: Ct));
    }
}
