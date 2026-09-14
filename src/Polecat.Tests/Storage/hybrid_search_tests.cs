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

    [Fact]
    public async Task a_phrase_leg_fuses_the_same_way()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        var fused = await session.HybridSearchAsync<Passage>(
            x => x.Embedding, "fox in the snow", new float[] { 0, 0, 1 },
            options: new HybridSearchOptions(TextStyle: HybridTextStyle.Phrase),
            token: TestContext.Current.CancellationToken);

        fused[0].Id.ShouldBe(BothLegs);
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
}
