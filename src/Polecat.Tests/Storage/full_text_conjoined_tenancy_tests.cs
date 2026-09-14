using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     gh-625: the full-text index under conjoined tenancy, where a document id is only unique
///     <em>per tenant</em>.
/// </summary>
/// <remarks>
///     <para>
///         <b>Every fact here needs two tenants holding a document with the SAME id</b>, because that
///         is the only shape the defects have. Guid ids make the collision rare in practice and string
///         or business-key ids make it ordinary — which is exactly why none of the four existing
///         search test files caught any of this: they are all single-tenant, so <c>doc_id</c> alone
///         happens to be a complete key and every id-only join is accidentally correct.
///     </para>
///     <para>
///         ⚠️ <b>All three failures are silent.</b> None throws, none logs, and each returns a
///         plausible answer — a document that does not contain the phrase, a search that has quietly
///         stopped finding a document, an index that never covered one. A test that only asserts
///         "some rows come back" passes against every one of them, so each fact below names the
///         specific document it must and must not see.
///     </para>
/// </remarks>
public class full_text_conjoined_tenancy_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public string Id { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    /// <summary>
    ///     A string id, deliberately. The collision these facts turn on is the ordinary case for a
    ///     business key and a coincidence for a Guid, and a test that has to manufacture a Guid
    ///     collision reads as contrived where this reads as the intended use.
    /// </summary>
    private const string SharedId = "article-1";

    private DocumentStore aConjoinedStore(bool withIndex = true)
    {
        ConfigureStore(opts =>
        {
            // DocumentMapping takes its TenancyStyle from the event graph, so this is what makes the
            // document table conjoined and puts tenant_id into its primary key.
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;

            if (withIndex)
            {
                opts.Schema.For<Article>().FullTextIndex(x => x.Body);
            }
        });

        return theStore;
    }

    private async Task writeAsync(DocumentStore store, string tenantId, string id, string body)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        session.Store(new Article { Id = id, Body = body });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task<List<string>> termsForAsync(string tenantId, string id)
    {
        await using (var session = theStore.QuerySession())
        {
            await ((Polecat.Internal.QuerySession)session)
                .EnsureDocumentTableAsync(typeof(Article), TestContext.Current.CancellationToken);
        }

        var mapping = theStore.Options.Providers.GetProvider(typeof(Article)).Mapping;
        var table = FullTextIndex.TableNameFor(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT term FROM [{mapping.DatabaseSchemaName}].[{table}] "
            + "WHERE doc_id = @id AND tenant_id = @tenant ORDER BY pos";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant", tenantId);

        var terms = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            terms.Add(reader.GetString(0));
        }

        return terms;
    }

    private async Task deleteTokensAsync(string tenantId, string id)
    {
        var mapping = theStore.Options.Providers.GetProvider(typeof(Article)).Mapping;
        var table = FullTextIndex.TableNameFor(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"DELETE FROM [{mapping.DatabaseSchemaName}].[{table}] WHERE doc_id = @id AND tenant_id = @tenant";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant", tenantId);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     gh-625 §1. A phrase is the terms adjacent and in order, and every one of them has to come
    ///     from the same tenant's document.
    /// </summary>
    /// <remarks>
    ///     ⚠️ The old SQL pinned only <c>f0</c> to the session tenant and left <c>f1..fn</c> free, so
    ///     the phrase could be assembled out of two tenants' documents: <c>f0</c> is Red's "quick" at
    ///     position 1 and <c>f1</c> is Blue's "brown" at position 2. Red then gets back a document
    ///     whose body is "quick fox" for a search for "quick brown".
    /// </remarks>
    [Fact]
    public async Task phrase_search_does_not_assemble_a_phrase_across_tenants()
    {
        var store = aConjoinedStore();

        await writeAsync(store, "Red", SharedId, "quick fox");
        await writeAsync(store, "Blue", SharedId, "lazy brown");

        await using var red = store.QuerySession(new SessionOptions { TenantId = "Red" });

        var straddling = await red.Query<Article>()
            .Where(x => x.Body.PhraseSearch("quick brown"))
            .ToListAsync(TestContext.Current.CancellationToken);

        straddling.ShouldBeEmpty();

        // The tenant's own phrase still matches, so this is not passing because the operator stopped
        // working.
        var ownPhrase = await red.Query<Article>()
            .Where(x => x.Body.PhraseSearch("quick fox"))
            .ToListAsync(TestContext.Current.CancellationToken);

        ownPhrase.Single().Body.ShouldBe("quick fox");
    }

    /// <summary>
    ///     gh-625 §2, and the most serious of the three: an ordinary write in one tenant silently
    ///     unindexed another tenant's document.
    /// </summary>
    /// <remarks>
    ///     The trigger deletes the token rows for the written ids and re-inserts from
    ///     <c>inserted</c>. Joining on <c>doc_id</c> alone deleted EVERY tenant's rows for that id
    ///     while only the writing tenant's were re-inserted, so the other tenant's document dropped
    ///     out of <c>PlainTextSearch</c>, <c>PhraseSearch</c>, the ranked search and the text leg of a
    ///     hybrid search at once — until it happened to be written again.
    /// </remarks>
    [Fact]
    public async Task writing_one_tenants_document_leaves_the_others_tokens_intact()
    {
        var store = aConjoinedStore();

        await writeAsync(store, "Red", SharedId, "red fox");
        await writeAsync(store, "Blue", SharedId, "blue turtle");

        // The second write is what fired the destructive trigger, so assert Red survived it.
        (await termsForAsync("Red", SharedId)).ShouldBe(["red", "fox"]);
        (await termsForAsync("Blue", SharedId)).ShouldBe(["blue", "turtle"]);

        // An UPDATE in Blue, which fires the trigger again for an id Red also holds.
        await writeAsync(store, "Blue", SharedId, "blue heron");

        (await termsForAsync("Red", SharedId)).ShouldBe(["red", "fox"]);

        await using var red = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var found = await red.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("fox"))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Single().Body.ShouldBe("red fox");
    }

    /// <summary>
    ///     The delete half of the same trigger: removing one tenant's document must not unindex the
    ///     other tenant's.
    /// </summary>
    [Fact]
    public async Task deleting_one_tenants_document_leaves_the_others_searchable()
    {
        var store = aConjoinedStore();

        await writeAsync(store, "Red", SharedId, "red fox");
        await writeAsync(store, "Blue", SharedId, "blue turtle");

        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Delete<Article>(SharedId);
            await blue.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await termsForAsync("Blue", SharedId)).ShouldBeEmpty();
        (await termsForAsync("Red", SharedId)).ShouldBe(["red", "fox"]);

        await using var red = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var found = await red.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("fox"))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Single().Body.ShouldBe("red fox");
    }

    /// <summary>
    ///     gh-625 §3. The backfill treats "this member already has tokens for this id" as "already
    ///     indexed", and that question has to be asked per tenant.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Written as a REPAIR rather than as a first declaration, because a first
    ///         declaration cannot fail this way and a test that used one would pass against the bug.</b>
    ///         The backfill is a single <c>INSERT … SELECT … WHERE NOT EXISTS</c>, and SQL Server
    ///         evaluates that subquery against the table as of statement start — so when NEITHER tenant
    ///         has tokens yet, both rows are inserted whether the predicate carries a tenant or not.
    ///         I wrote the obvious version of this test first and it passed against the unfixed code,
    ///         which is the whole argument for running a new test against the old code before trusting
    ///         it.
    ///     </para>
    ///     <para>
    ///         The state this pins is one tenant indexed and another not, which is exactly what the
    ///         §2 trigger bug left behind — and it is why the two fixes together are self-healing: the
    ///         corrected trigger stops the damage, and the corrected backfill restores what the old one
    ///         already did, the next time storage is ensured. With the id-only predicate the surviving
    ///         tenant's rows are read as proof that the missing tenant needs nothing, and the document
    ///         stays unsearchable forever.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task the_backfill_repairs_a_tenant_whose_tokens_are_missing()
    {
        var store = aConjoinedStore();

        await writeAsync(store, "Red", SharedId, "red fox");
        await writeAsync(store, "Blue", SharedId, "blue turtle");

        await deleteTokensAsync("Blue", SharedId);
        (await termsForAsync("Blue", SharedId)).ShouldBeEmpty();

        // Ensuring storage again re-runs the backfill. Red still holds tokens for this id, which is
        // what the id-only NOT EXISTS mistook for "indexed".
        aConjoinedStore();
        await using (var session = theStore.QuerySession())
        {
            await ((Polecat.Internal.QuerySession)session)
                .EnsureDocumentTableAsync(typeof(Article), TestContext.Current.CancellationToken);
        }

        (await termsForAsync("Blue", SharedId)).ShouldBe(["blue", "turtle"]);
        (await termsForAsync("Red", SharedId)).ShouldBe(["red", "fox"]);
    }

    /// <summary>
    ///     A first declaration over pre-existing rows still has to cover both tenants. This one passes
    ///     against the unfixed code — see the remarks above for why — and is kept because it is the
    ///     case a reader expects to find covered.
    /// </summary>
    [Fact]
    public async Task declaring_the_index_over_existing_rows_backfills_every_tenant()
    {
        var store = aConjoinedStore(withIndex: false);

        await writeAsync(store, "Red", SharedId, "red fox");
        await writeAsync(store, "Blue", SharedId, "blue turtle");

        // Re-declare with the index, which runs the backfill over rows that predate it.
        aConjoinedStore();

        (await termsForAsync("Red", SharedId)).ShouldBe(["red", "fox"]);
        (await termsForAsync("Blue", SharedId)).ShouldBe(["blue", "turtle"]);
    }

    /// <summary>
    ///     The ranked BM25 path filters every CTE by tenant already, so this pins behaviour rather
    ///     than fixing it — and it is worth pinning precisely because the three paths above looked
    ///     equally correct until someone read the SQL.
    /// </summary>
    [Fact]
    public async Task the_ranked_search_returns_only_the_session_tenant()
    {
        var store = aConjoinedStore();

        await writeAsync(store, "Red", SharedId, "red fox");
        await writeAsync(store, "Blue", SharedId, "blue fox");

        await using var red = store.QuerySession(new SessionOptions { TenantId = "Red" });

        var hits = await red.FullTextSearchWithScoresAsync<Article>(
            x => x.Body, "fox", 10, token: TestContext.Current.CancellationToken);

        hits.Select(x => x.Document.Body).ShouldBe(["red fox"]);
    }

    /// <summary>
    ///     The last question gh-625 raised: hybrid search fuses the two legs BY ID, so does the fusion
    ///     itself have an id-only join under conjoined tenancy?
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>No, and the reason is structural rather than lucky.</b> Both legs run through the
    ///         session's own query path and are tenant-filtered before they are fused, so every id
    ///         reaching <c>ReciprocalRankFusion.Fuse</c> already belongs to the session's tenant —
    ///         there is no id in play for the key to collide with. Pinned anyway, because the three
    ///         defects this file fixes all looked equally safe until someone read the SQL.
    ///     </para>
    ///     <para>
    ///         Skipped on the `edge` lane, which is Azure SQL Edge and has no <c>VECTOR</c> type. Same
    ///         reason and same spelling as <c>vector_search_tests</c>.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task hybrid_search_returns_only_the_session_tenant()
    {
        Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<Embedded>().FullTextIndex(x => x.Body);
            opts.Schema.For<Embedded>().VectorIndex(x => x.Embedding, 3);
        });

        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Store(new Embedded { Id = SharedId, Body = "red fox", Embedding = [1, 0, 0] });
            await red.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var blue = theStore.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Store(new Embedded { Id = SharedId, Body = "blue fox", Embedding = [1, 0, 0] });
            await blue.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession(new SessionOptions { TenantId = "Red" });

        var hits = await query.HybridSearchAsync<Embedded>(
            x => x.Embedding, "fox", new float[] { 1, 0, 0 }, 10,
            token: TestContext.Current.CancellationToken);

        hits.Select(x => x.Body).ShouldBe(["red fox"]);
    }

    public class Embedded
    {
        public string Id { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }
}
