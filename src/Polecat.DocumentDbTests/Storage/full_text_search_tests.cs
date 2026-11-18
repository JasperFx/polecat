using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     The query half of gh-611: the LINQ operators, and BM25 ranking. Extended by gh-630 with
///     <c>PrefixSearch</c> and the tunable BM25 constants.
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

    private static readonly Guid ShortArticle = Guid.NewGuid();
    private static readonly Guid LongArticle = Guid.NewGuid();

    /// <summary>
    ///     Two articles mentioning "fox" exactly once, one of them twenty times longer. Term
    ///     frequency and document frequency are identical across the pair by construction, so BM25's
    ///     LENGTH normalization is the only term that can separate them — which is what makes
    ///     <c>b = 0</c> observable at all.
    /// </summary>
    private async Task<IDocumentStore> aStoreWithOneShortAndOneLongArticleAsync()
    {
        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Body));

        var padding = string.Join(' ', Enumerable.Range(0, 19).Select(i => $"filler{i}"));

        await using var session = theStore.LightweightSession();
        session.Store(new Article { Id = ShortArticle, Body = "fox" });
        session.Store(new Article { Id = LongArticle, Body = "fox " + padding });
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
    public async Task web_style_search_handles_quotes_exclusions_and_or()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // Bare words are required, like PlainTextSearch.
        var bare = await session.Query<Article>()
            .Where(x => x.Body.WebStyleSearch("fox")).ToListAsync(token);
        bare.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { Fox, Both }.OrderBy(x => x));

        // A quoted run is a phrase — the terms adjacent and in order.
        var phrase = await session.Query<Article>()
            .Where(x => x.Body.WebStyleSearch("\"quick brown fox\"")).ToListAsync(token);
        phrase.Single().Id.ShouldBe(Fox);

        // A leading - excludes. Both documents carry "fox"; only one also carries "turtle".
        var excluded = await session.Query<Article>()
            .Where(x => x.Body.WebStyleSearch("fox -turtle")).ToListAsync(token);
        excluded.Single().Id.ShouldBe(Fox);

        // `or` separates alternatives.
        var either = await session.Query<Article>()
            .Where(x => x.Body.WebStyleSearch("turtle or dog")).ToListAsync(token);
        either.Select(x => x.Id).OrderBy(x => x)
            .ShouldBe(new[] { Fox, Turtle, Both }.OrderBy(x => x));
    }

    [Fact]
    public async Task a_web_style_query_of_nothing_but_exclusions_matches_nothing()
    {
        // "Not this" is not a search. The alternative — every document that happens to lack the term
        // — is a surprising amount of data back from a search box containing one word.
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();

        var results = await session.Query<Article>()
            .Where(x => x.Body.WebStyleSearch("-turtle"))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.ShouldBeEmpty();
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
    public async Task prefix_search_matches_from_the_start_of_a_term()
    {
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // "qui" is a prefix of "quick", which only the first article carries.
        var quick = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("qui")).ToListAsync(token);
        quick.Single().Id.ShouldBe(Fox);

        // A prefix reaches more than one document when more than one term starts with it.
        var turtles = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("tur")).ToListAsync(token);
        turtles.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { Turtle, Both }.OrderBy(x => x));

        // ⚠️ The START of a term, not anywhere inside one. "uic" is in "quick" and matches nothing,
        // and that is the difference between this operator and the ngram/trigram search Polecat does
        // not have — a substring search against a whole-term index is an empty result, not a slow one.
        var inside = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("uic")).ToListAsync(token);
        inside.ShouldBeEmpty();

        // A whole term is a prefix of itself, so PrefixSearch is a superset of PlainTextSearch.
        var whole = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("fox")).ToListAsync(token);
        whole.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { Fox, Both }.OrderBy(x => x));
    }

    [Fact]
    public async Task prefix_search_requires_every_word()
    {
        // Marten's operator of the same name treats each word as a prefix and requires all of them;
        // this is the fact that holds Polecat to the same reading rather than "any of them".
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var both = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("qui bro")).ToListAsync(token);
        both.Single().Id.ShouldBe(Fox);

        // "tur" matches the turtle article and "wi" does not, so requiring both leaves only the
        // article carrying "turtle" and "wins".
        var narrowed = await session.Query<Article>()
            .Where(x => x.Body.PrefixSearch("tur wi")).ToListAsync(token);
        narrowed.Single().Id.ShouldBe(Both);
    }

    [Fact]
    public async Task an_empty_prefix_matches_nothing_rather_than_everything()
    {
        // Same answer PlainTextSearch gives an empty search, and the more important one here: a
        // search-as-you-type box is empty on first render, and "everything" is the wrong first page.
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("")).ToListAsync(token))
            .ShouldBeEmpty();

        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("   ")).ToListAsync(token))
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task prefix_search_cannot_smuggle_a_like_wildcard()
    {
        // ⚠️ PrefixSearch is the one operator that renders a LIKE, so it is the one place a user's
        // text could become pattern syntax. Nothing escapes it, because nothing has to: '%' is in
        // FullTextIndex.Punctuation, so the tokenizer maps it to a space and no term can carry one.
        //
        // This asserts the consequence rather than the mechanism. Were '%' to survive tokenization
        // the rendered predicate would be `term LIKE '%%'`, which matches every token row and hands
        // back the WHOLE TABLE from a search box containing one character — a silent data leak
        // wearing the shape of a search. tokenize_strips_the_like_metacharacters pins the property
        // this rests on; the two move together.
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("%")).ToListAsync(token))
            .ShouldBeEmpty();

        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("_")).ToListAsync(token))
            .ShouldBeEmpty();

        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("[a-z]")).ToListAsync(token))
            .ShouldBeEmpty();
    }

    [Fact]
    public void tokenize_strips_the_like_metacharacters()
    {
        // The property prefix_search_cannot_smuggle_a_like_wildcard depends on, asserted directly so
        // that widening the tokenizer fails here with an obvious message rather than turning a
        // prefix search into a wildcard several files away.
        FullTextIndex.Tokenize("a%b_c[d]e").ShouldBe(["a", "b", "c", "d", "e"]);
        FullTextIndex.Tokenize("%").ShouldBeEmpty();
    }

    [Fact]
    public async Task b_of_zero_turns_length_normalization_off()
    {
        // Two documents mentioning "fox" exactly once, one of them twenty times longer. Length
        // normalization is the only thing that separates them, so switching it off has to make the
        // scores equal — and leaving it on has to keep the short one ahead.
        var store = await aStoreWithOneShortAndOneLongArticleAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var normalized = await session.FullTextSearchWithScoresAsync<Article>(
            x => x.Body, "fox", token: token);

        normalized.Count.ShouldBe(2);
        normalized[0].Document.Id.ShouldBe(ShortArticle);
        normalized[0].Score.ShouldBeGreaterThan(normalized[1].Score);

        var flat = await session.FullTextSearchWithScoresAsync<Article>(
            x => x.Body, "fox", options: new FullTextSearchOptions(B: 0), token: token);

        flat.Count.ShouldBe(2);
        flat[0].Score.ShouldBe(flat[1].Score, 1e-9);
    }

    [Fact]
    public async Task k1_of_zero_reduces_the_ranking_to_idf()
    {
        // The other constant, and it needs its own fact: b = 0 would still pass if k1 were left
        // hard-coded. With k1 = 0 the term-frequency factor collapses to 1 for every document, so
        // the article saying "fox" three times ties the one saying it once.
        var store = await aStoreWithArticlesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var saturated = await session.FullTextSearchWithScoresAsync<Article>(
            x => x.Body, "fox", token: token);

        saturated.Count.ShouldBe(2);
        saturated[0].Score.ShouldBeGreaterThan(saturated[1].Score);

        var idfOnly = await session.FullTextSearchWithScoresAsync<Article>(
            x => x.Body, "fox", options: new FullTextSearchOptions(K1: 0), token: token);

        idfOnly.Count.ShouldBe(2);
        idfOnly[0].Score.ShouldBe(idfOnly[1].Score, 1e-9);
    }

    [Fact]
    public void out_of_range_bm25_parameters_are_refused_by_name()
    {
        // The whole value of refusing these is that the parameter name reaches the caller. A ranking
        // computed with b = 4 is not an error anywhere downstream — it is a plausible-looking order
        // that is simply wrong, which is the failure this converts into an exception.
        Should.Throw<ArgumentOutOfRangeException>(() => new FullTextSearchOptions(K1: -0.1))
            .ParamName.ShouldBe("K1");

        Should.Throw<ArgumentOutOfRangeException>(() => new FullTextSearchOptions(K1: double.NaN))
            .ParamName.ShouldBe("K1");

        Should.Throw<ArgumentOutOfRangeException>(() => new FullTextSearchOptions(B: -0.1))
            .ParamName.ShouldBe("B");

        Should.Throw<ArgumentOutOfRangeException>(() => new FullTextSearchOptions(B: 1.1))
            .ParamName.ShouldBe("B");

        // ⚠️ `with` runs the copy constructor, not the primary constructor body — so validating in
        // the constructor body rather than in the init accessor would let this one through.
        var valid = new FullTextSearchOptions();
        Should.Throw<ArgumentOutOfRangeException>(() => valid with { B = 4 }).ParamName.ShouldBe("B");

        // The legal edges, so the guard cannot be "refuse everything".
        new FullTextSearchOptions(K1: 0, B: 0).K1.ShouldBe(0);
        new FullTextSearchOptions(B: 1).B.ShouldBe(1);
        new FullTextSearchOptions().K1.ShouldBe(FullTextSearchOptions.DefaultK1);
        new FullTextSearchOptions().B.ShouldBe(FullTextSearchOptions.DefaultB);
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
