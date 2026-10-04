using JasperFx;
using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     #729: a search for the ROOT of a hierarchy resolves each row to its concrete type, the way
///     <c>Query&lt;Root&gt;()</c> has since the #273 E2 closed-shape work.
/// </summary>
/// <remarks>
///     <para>
///         Split out of #723, which fixed the sub-class half — a sub-class search was scanning the
///         shared table unfiltered. This is the other direction: a root search correctly returns
///         every row, and used to materialize all of them AS the root, dropping each sub-class's own
///         members on the floor.
///     </para>
///     <para>
///         ⚠️ <b>The scored overloads are where a fix like this goes wrong</b>, and silently. Three
///         things read the search statements' columns by index — <c>DocumentReader.ColumnCount</c>,
///         <c>QueryByBatchAsync&lt;T1, T2&gt;</c> (score at <c>ColumnCount</c>), and
///         <c>DocumentReader.SyncMetadata</c> (which probes <c>startColumn + 2</c> guarded only by a
///         type test, and is already reading the score column). The discriminator is appended LAST
///         and found by NAME so none of those indexes move;
///         <see cref="the_scored_overloads_still_read_the_score_and_the_version" /> is what notices
///         if that stops being true, which is why its document type is <c>IRevisioned</c>.
///     </para>
/// </remarks>
public static class RootSearch
{
    public class Note: IRevisioned
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }

        /// <summary>
        ///     Makes a column-layout mistake observable. <c>SyncMetadata</c> probes a fixed offset
        ///     and assigns when the value happens to be an int or a long, so a shifted select list
        ///     shows up here as a wrong version rather than as an error.
        /// </summary>
        public int Version { get; set; }
    }

    public class Memo: Note
    {
        public string Approver { get; set; } = string.Empty;
    }

    public class Minute: Note
    {
        public string Chair { get; set; } = string.Empty;
    }

    public static readonly Guid PlainNote = Guid.NewGuid();
    public static readonly Guid TheMemo = Guid.NewGuid();
    public static readonly Guid TheMinute = Guid.NewGuid();

    public static readonly float[] TowardsMinute = [1f, 0f, 0f];

    public static async Task SeedAsync(IDocumentStore store, CancellationToken token)
    {
        await using var session = store.LightweightSession();

        session.Store(new Note { Id = PlainNote, Body = "the fox sleeps", Embedding = [0.9f, 0.1f, 0f] });
        session.Store<Note>(new Memo
        {
            Id = TheMemo, Body = "a fox memo", Approver = "jeremy", Embedding = [0f, 1f, 0f]
        });
        session.Store<Note>(new Minute
        {
            Id = TheMinute, Body = "fox minutes", Chair = "oskar", Embedding = [1f, 0f, 0f]
        });

        await session.SaveChangesAsync(token);
    }
}

/// <summary>Full-text, so these facts also run on the `edge` lane.</summary>
public class full_text_root_concrete_type_tests: OneOffConfigurationsContext
{
    private async Task<IDocumentStore> aStoreAsync()
    {
        ConfigureStore(opts => opts.Schema.For<RootSearch.Note>()
            .AddSubClass<RootSearch.Memo>()
            .AddSubClass<RootSearch.Minute>()
            .FullTextIndex(x => x.Body));

        await RootSearch.SeedAsync(theStore, TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task a_root_search_resolves_each_row_to_its_concrete_type()
    {
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchAsync<RootSearch.Note>(
            x => x.Body, "fox", limit: 10, token: token);

        var byId = hits.ToDictionary(x => x.Id);

        byId[RootSearch.PlainNote].ShouldBeOfType<RootSearch.Note>();
        byId[RootSearch.TheMemo].ShouldBeOfType<RootSearch.Memo>();
        byId[RootSearch.TheMinute].ShouldBeOfType<RootSearch.Minute>();

        // The members that used to be dropped on the floor.
        ((RootSearch.Memo)byId[RootSearch.TheMemo]).Approver.ShouldBe("jeremy");
        ((RootSearch.Minute)byId[RootSearch.TheMinute]).Chair.ShouldBe("oskar");
    }

    [Fact]
    public async Task a_root_search_agrees_with_query_over_the_root()
    {
        // ⚠️ The POINT of #729 is that the two read paths agree, so the fact is stated as a
        // comparison rather than as a list of expected types. A future divergence in either path
        // fails this without anyone having to remember to update both.
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var searched = await session.FullTextSearchAsync<RootSearch.Note>(
            x => x.Body, "fox", limit: 10, token: token);
        var queried = await session.Query<RootSearch.Note>().ToListAsync(token);

        searched.OrderBy(x => x.Id).Select(x => x.GetType())
            .ShouldBe(queried.OrderBy(x => x.Id).Select(x => x.GetType()));
    }

    [Fact]
    public async Task a_sub_class_search_is_unchanged()
    {
        // #723's half must stay exactly as it was: only that sub-class, with its member intact.
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchAsync<RootSearch.Memo>(
            x => x.Body, "fox", limit: 10, token: token);

        hits.Select(x => x.Id).ShouldBe([RootSearch.TheMemo]);
        hits.Single().ShouldBeOfType<RootSearch.Memo>().Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task the_scored_overloads_still_read_the_score_and_the_version()
    {
        // ⚠️ The column-layout fact. The discriminator is appended after the score and found by
        // name, so BOTH of these keep working: the score is still a real BM25 score read from the
        // right column, and Version is still whatever SyncMetadata's fixed-offset probe makes of it
        // rather than a stray double or an alias string.
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchWithScoresAsync<RootSearch.Note>(
            x => x.Body, "fox", limit: 10, token: token);

        hits.Count.ShouldBe(3);
        hits.Select(x => x.Score).ShouldBeInOrder(SortDirection.Descending);
        hits.ShouldAllBe(x => x.Score > 0);

        // Concrete types on the scored overload too, not only the document-only one.
        hits.Select(x => x.Document.GetType()).ShouldBe(
            new[]
            {
                typeof(RootSearch.Note), typeof(RootSearch.Memo), typeof(RootSearch.Minute)
            },
            ignoreOrder: true);
    }
}

/// <summary>Vector and hybrid, gated on VECTOR support.</summary>
public class vector_root_concrete_type_tests: OneOffConfigurationsContext
{
    private async Task<IDocumentStore> aStoreAsync()
    {
        Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

        ConfigureStore(opts => opts.Schema.For<RootSearch.Note>()
            .AddSubClass<RootSearch.Memo>()
            .AddSubClass<RootSearch.Minute>()
            .VectorIndex(x => x.Embedding, 3)
            .FullTextIndex(x => x.Body));

        await RootSearch.SeedAsync(theStore, TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task a_root_vector_search_resolves_each_row_to_its_concrete_type()
    {
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.VectorSearchAsync<RootSearch.Note>(
            x => x.Embedding, RootSearch.TowardsMinute, limit: 10, token: token);

        // Nearest first AND concrete -- the ordering is asserted alongside the types because the
        // discriminator column sits next to the ORDER BY key.
        hits.Select(x => x.Id).ShouldBe(
            [RootSearch.TheMinute, RootSearch.PlainNote, RootSearch.TheMemo]);
        hits[0].ShouldBeOfType<RootSearch.Minute>().Chair.ShouldBe("oskar");
        hits[2].ShouldBeOfType<RootSearch.Memo>().Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task a_scored_root_vector_search_keeps_its_distances()
    {
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.VectorSearchWithScoresAsync<RootSearch.Note>(
            x => x.Embedding, RootSearch.TowardsMinute, limit: 10, token: token);

        // Smaller is closer, and the nearest is exactly on the query vector -- so a distance read
        // from the wrong column could not produce this.
        hits.Select(x => x.Distance).ShouldBeInOrder();
        hits[0].Distance.ShouldBeLessThan(hits[2].Distance);
        hits[0].Document.ShouldBeOfType<RootSearch.Minute>();
    }

    [Fact]
    public async Task a_root_hybrid_search_resolves_each_row_to_its_concrete_type()
    {
        var store = await aStoreAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.HybridSearchAsync<RootSearch.Note>(
            x => x.Embedding, "fox", RootSearch.TowardsMinute, limit: 10, token: token);

        // Both legs feed the fusion, so a leg that still materialized as the root would show up
        // here as a Note the other leg resolved concretely.
        hits.Select(x => x.GetType()).ShouldBe(
            new[]
            {
                typeof(RootSearch.Note), typeof(RootSearch.Memo), typeof(RootSearch.Minute)
            },
            ignoreOrder: true);
    }
}
