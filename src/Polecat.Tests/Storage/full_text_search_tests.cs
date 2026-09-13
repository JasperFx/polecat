using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     The query half of gh-611: the LINQ operators, and BM25 ranking.
/// </summary>
public class full_text_search_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private static readonly Guid Fox = Guid.NewGuid();
    private static readonly Guid Turtle = Guid.NewGuid();
    private static readonly Guid Both = Guid.NewGuid();

    private async Task<IDocumentStore> aStoreWithArticlesAsync()
    {
        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Body));

        await using var session = theStore.LightweightSession();
        session.Store(new Article { Id = Fox, Body = "The quick brown fox jumps over the lazy dog" });
        session.Store(new Article { Id = Turtle, Body = "A slow green turtle naps in the sun" });
        session.Store(new Article { Id = Both, Body = "A fox and a turtle; the fox runs, the fox wins" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return theStore;
    }

    [Fact]
    public async Task plain_text_search_requires_every_term()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var fox = await session.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("fox")).ToListAsync(token);
        fox.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { Fox, Both }.OrderBy(x => x));

        // Both terms, so only the document carrying both.
        var foxAndTurtle = await session.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("fox turtle")).ToListAsync(token);
        foxAndTurtle.Single().Id.ShouldBe(Both);

        // Case and punctuation are normalized the same way on both sides.
        var shouty = await session.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("FOX, TURTLE!")).ToListAsync(token);
        shouty.Single().Id.ShouldBe(Both);
    }

    [Fact]
    public async Task phrase_search_wants_the_terms_adjacent_and_in_order()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var inOrder = await session.Query<Article>()
            .Where(x => x.Body.PhraseSearch("quick brown fox")).ToListAsync(token);
        inOrder.Single().Id.ShouldBe(Fox);

        // The same three terms are all present, but not in this order — which is the whole
        // difference between this and PlainTextSearch.
        var scrambled = await session.Query<Article>()
            .Where(x => x.Body.PhraseSearch("fox brown quick")).ToListAsync(token);
        scrambled.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_empty_search_matches_nothing_rather_than_everything()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();

        var results = await session.Query<Article>()
            .Where(x => x.Body.PlainTextSearch("   ")).ToListAsync(TestContext.Current.CancellationToken);

        results.ShouldBeEmpty();
    }

    [Fact]
    public async Task bm25_ranks_the_denser_match_first()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // "fox" three times in a shorter body outranks one mention in a longer one — term frequency
        // up, length normalization down, which is exactly what BM25 is for.
        var ranked = await session.FullTextSearchAsync<Article>(x => x.Body, "fox", token: token);

        ranked.Select(x => x.Id).ShouldBe([Both, Fox]);
    }

    [Fact]
    public async Task scores_come_back_positive_and_ordered()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var scored = await session.FullTextSearchWithScoresAsync<Article>(x => x.Body, "fox", token: token);

        scored.Count.ShouldBe(2);
        scored[0].Score.ShouldBeGreaterThan(scored[1].Score);
        scored.ShouldAllBe(x => x.Score > 0);
    }

    [Fact]
    public async Task the_query_tokenizer_matches_the_stored_one()
    {
        // Two implementations of one rule — C# for the query, T-SQL for the trigger. When they
        // disagree the result is not an error, it is an empty answer, so nothing but this fact holds
        // them together.
        var store = await aStoreWithArticlesAsync();
        var token = TestContext.Current.CancellationToken;
        const string tricky = "Hello, WORLD! It's a re-entrant (mostly) test/case; isn't it?";

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = tricky });
            await session.SaveChangesAsync(token);
        }

        var mapping = theStore.Options.Providers.GetProvider(typeof(Article)).Mapping;
        var table = FullTextIndex.TableNameFor(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT term FROM [{mapping.DatabaseSchemaName}].[{table}] WHERE doc_id = @id ORDER BY pos";
        cmd.Parameters.AddWithValue("@id", id);

        var stored = new List<string>();
        await using (var reader = await cmd.ExecuteReaderAsync(token))
        {
            while (await reader.ReadAsync(token)) stored.Add(reader.GetString(0));
        }

        stored.ShouldBe(FullTextIndex.Tokenize(tricky));
    }

    [Fact]
    public async Task refusals_name_the_problem()
    {
        ConfigureStore(_ => { });
        await using var session = theStore.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var undeclared = await Should.ThrowAsync<InvalidOperationException>(() =>
            session.FullTextSearchAsync<Article>(x => x.Body, "fox", token: token));

        undeclared.Message.ShouldContain("declares no full-text index");
    }
}
