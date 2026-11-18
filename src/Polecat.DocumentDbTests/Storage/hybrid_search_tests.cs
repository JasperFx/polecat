using JasperFx.Events.Vectors;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     gh-612: reciprocal rank fusion over the full-text leg (gh-611) and the vector leg (gh-605).
/// </summary>
public class hybrid_search_tests: OneOffConfigurationsContext
{
    public class Passage
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }

    public class TwoTexts
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }

    private static readonly Guid BothLegs = Guid.NewGuid();
    private static readonly Guid TextOnly = Guid.NewGuid();
    private static readonly Guid VectorOnly = Guid.NewGuid();

    /// <summary>
    ///     The vector leg needs SQL Server 2025. The CI matrix runs an `edge` lane that has no VECTOR
    ///     type, so every fact reaching the database has to say so rather than fail.
    /// </summary>
    private static void requiresVectorSupport()
        => Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

    private async Task<IDocumentStore> aStoreWithPassagesAsync()
    {
        requiresVectorSupport();

        ConfigureStore(opts =>
        {
            opts.Schema.For<Passage>()
                .FullTextIndex(x => x.Body)
                .VectorIndex(x => x.Embedding, 3);
        });

        await using var session = theStore.LightweightSession();

        // Found by both legs: the word, and a vector near the query.
        session.Store(new Passage { Id = BothLegs, Body = "a fox in the snow", Embedding = [0, 0, 1] });

        // Text only: carries the word, vector points elsewhere.
        session.Store(new Passage { Id = TextOnly, Body = "a fox and nothing else", Embedding = [1, 0, 0] });

        // Vector only: near the query vector, no matching word.
        session.Store(new Passage { Id = VectorOnly, Body = "a turtle naps", Embedding = [0, 0.1f, 0.99f] });

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task a_document_found_by_both_legs_outranks_one_found_by_either()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var fused = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            token: TestContext.Current.CancellationToken);

        // RRF sums a contribution per leg that found it, so the document both legs rank highly wins
        // even when neither leg puts it first on its own.
        fused[0].Id.ShouldBe(BothLegs);
        fused.Select(x => x.Id).ShouldContain(TextOnly);
        fused.Select(x => x.Id).ShouldContain(VectorOnly);
    }

    [Fact]
    public async Task scores_are_positive_and_descending()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var scored = await session.HybridSearchWithScoresAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            token: TestContext.Current.CancellationToken);

        scored.ShouldAllBe(x => x.Score > 0);
        scored.Select(x => x.Score).ShouldBe(scored.Select(x => x.Score).OrderByDescending(x => x));
    }

    /// <summary>
    ///     ⚠️ #633: <c>HybridTextStyle.Phrase</c> is GONE from the hybrid surface, and this is the
    ///     replacement it points callers at. The shared enum is deliberately two members, both safe to
    ///     hand a search box's raw contents, because a malformed query in one leg fails the WHOLE fused
    ///     call. Phrase search itself is untouched — it stays on <c>Query&lt;T&gt;()</c>, where a bad
    ///     query fails only the thing the caller asked for.
    /// </summary>
    [Fact]
    public async Task phrase_search_stays_reachable_through_query()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var matches = await session.Query<Passage>()
            .Where(x => x.Body.PhraseSearch("fox in the snow"))
            .ToListAsync(TestContext.Current.CancellationToken);

        matches.Select(x => x.Id).ShouldBe([BothLegs]);
    }

    /// <summary>
    ///     The shared enum has exactly the two members that are safe to hand raw input, on every store.
    /// </summary>
    [Fact]
    public void the_hybrid_text_styles_are_the_two_portable_ones()
    {
        Enum.GetNames<HybridTextStyle>().ShouldBe(["PlainText", "WebStyle"]);
    }

    [Fact]
    public async Task the_portable_default_is_plain_text()
    {
        // gh-627: the member is PlainText, spelled as Marten spells it, so the same call compiles
        // against either store. It is also the default, so a caller who names no style gets the
        // portable one.
        new HybridSearchOptions().TextStyle.ShouldBe(HybridTextStyle.PlainText);

        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var explicitly = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            options: new HybridSearchOptions(TextStyle: HybridTextStyle.PlainText),
            token: TestContext.Current.CancellationToken);

        explicitly[0].Id.ShouldBe(BothLegs);
    }

    [Fact]
    public async Task a_web_style_leg_fuses_the_same_way()
    {
        // gh-627: the enum now matches Marten's — PlainText and WebStyle mean the same thing on both
        // stores, so a hybrid call written against one compiles and behaves against the other.
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var fused = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox -turtle", new float[] { 0, 0, 1 },
            options: new HybridSearchOptions(TextStyle: HybridTextStyle.WebStyle),
            token: TestContext.Current.CancellationToken);

        // BothLegs carries "turtle" so the text leg excludes it, but the vector leg still finds it —
        // which is the fusion working, not a filter leaking.
        fused.ShouldNotBeEmpty();
        fused.Select(x => x.Id).ShouldContain(TextOnly);
    }

    /// <summary>
    ///     ⚠️ #633 / jasperfx#843: the filter is applied to BOTH legs, before each leg's candidate
    ///     depth. A filter on one leg only would fuse a ranking of a set the caller is about to
    ///     discard half of.
    /// </summary>
    [Fact]
    public async Task the_filter_applies_to_both_legs()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // Unfiltered, all three documents come back and BothLegs wins.
        var unfiltered = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 }, token: token);
        unfiltered.Count.ShouldBe(3);

        // The text leg found BothLegs and TextOnly; the vector leg found BothLegs and VectorOnly. If
        // the filter reached only one leg, the excluded document would still arrive through the other.
        var filtered = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            filter: x => x.Body != "a fox in the snow", token: token);

        filtered.Select(x => x.Id).ShouldNotContain(BothLegs);
        filtered.Select(x => x.Id).ShouldBe([TextOnly, VectorOnly], true);
    }

    /// <summary>The same, on the WebStyle leg, which reads through LINQ rather than through BM25.</summary>
    [Fact]
    public async Task the_filter_applies_to_the_web_style_leg_too()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var filtered = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            options: new HybridSearchOptions(TextStyle: HybridTextStyle.WebStyle),
            filter: x => x.Body != "a fox in the snow",
            token: TestContext.Current.CancellationToken);

        filtered.Select(x => x.Id).ShouldNotContain(BothLegs);
    }

    [Fact]
    public async Task candidate_depth_below_limit_is_refused_with_the_reason()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var ex = await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.HybridSearchAsync<Passage>(x => x.Embedding, "fox", new float[] { 0, 0, 1 },
                limit: 10,
                options: new HybridSearchOptions(CandidateDepth: 3),
                token: TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("read DEEPER than limit");
    }

    [Fact]
    public async Task a_smoothing_constant_below_one_is_refused()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var ex = await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.HybridSearchAsync<Passage>(x => x.Embedding, "fox", new float[] { 0, 0, 1 },
                options: new HybridSearchOptions(K: 0),
                token: TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("smoothing constant");
    }

    [Fact]
    public async Task a_type_with_no_full_text_index_is_refused_with_the_remedy()
    {
        requiresVectorSupport();
        ConfigureStore(opts => opts.Schema.For<Passage>().VectorIndex(x => x.Embedding, 3));

        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            session.HybridSearchAsync<Passage>(x => x.Embedding, "fox", new float[] { 0, 0, 1 },
                token: TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("no full-text index");
        ex.Message.ShouldContain("FullTextIndex(");
    }

    [Fact]
    public async Task more_than_one_text_member_points_at_the_explicit_overload()
    {
        requiresVectorSupport();
        ConfigureStore(opts =>
        {
            opts.Schema.For<TwoTexts>()
                .FullTextIndex(x => x.Title, x => x.Body)
                .VectorIndex(x => x.Embedding, 3);
        });

        await using var session = theStore.QuerySession();

        // The portable overload cannot infer which member the text leg means, and guessing would be
        // the wrong kind of convenience.
        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            session.HybridSearchAsync<TwoTexts>(x => x.Embedding, "fox", new float[] { 0, 0, 1 },
                token: TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("names both members explicitly");

        // And the explicit overload works where the portable one refuses.
        var explicitly = await session.HybridSearchWithScoresAsync<TwoTexts>(
            x => x.Body, x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            token: TestContext.Current.CancellationToken);

        explicitly.ShouldNotBeNull();
    }

    /// <summary>
    ///     ⚠️ #640 / jasperfx#854: <c>ColumnWeights</c> is REFUSED here rather than ignored.
    /// </summary>
    /// <remarks>
    ///     Polecat's full-text ranking addresses a single member, so there is no second column to
    ///     weigh and nothing could honour a weight. Ignoring it is the dangerous option: a caller who
    ///     weighted their title column and silently got an unweighted ranking has no way to find out,
    ///     because the search still returns plausible documents in a plausible order. That is the same
    ///     failure shape as the <c>Distance</c> default the shared options record was created to fix.
    /// </remarks>
    [Fact]
    public async Task column_weights_are_refused_by_name_rather_than_ignored()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var ex = await Should.ThrowAsync<NotSupportedException>(async () =>
            await session.HybridSearchAsync<Passage>(
                x => x.Embedding, "fox", new float[] { 0, 0, 1 },
                options: new HybridSearchOptions(ColumnWeights: [3.0, 1.0]), token: token));

        ex.Message.ShouldContain("Polecat");
        ex.Message.ShouldContain("ColumnWeights");
        ex.Message.ShouldContain("Fisher");
    }

    /// <summary>The default is untouched — only a NON-NULL value is refused.</summary>
    [Fact]
    public async Task the_default_of_no_column_weights_is_unaffected()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var fused = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            options: new HybridSearchOptions(K: 30),
            token: TestContext.Current.CancellationToken);

        fused.Count.ShouldBe(3);
    }
}
