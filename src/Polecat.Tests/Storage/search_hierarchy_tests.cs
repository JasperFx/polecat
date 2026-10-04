using JasperFx;
using JasperFx.Events.Vectors;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     The hierarchy discriminator on every search surface (#723).
/// </summary>
/// <remarks>
///     <para>
///         <b>The bug these were written against was not "a sibling row is included".</b> Rows come
///         back through <c>QueryByBatchAsync&lt;T&gt;</c>, which deserializes every returned row as
///         the <c>T</c> that was asked for — so a search for a sub-class over an unfiltered scan of
///         the shared hierarchy table materialized base-type and sibling rows <i>as that sub-class</i>.
///         That is why these facts check the sub-class's own member and not only the id: a wrong-type
///         row with its sub-class members at their defaults is the failure mode, and a count
///         assertion alone would pass on a store that returned the right number of wrong objects.
///     </para>
///     <para>
///         ⚠️ The corpus is built so that the <b>nearest</b> row to the query vector is a
///         <i>sibling</i> of the sub-class being searched for, and every document carries the search
///         term. An unfiltered search therefore does not merely include extra rows — it puts the
///         wrong one first.
///     </para>
/// </remarks>
public static class SearchHierarchy
{
    public class Note
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }

    /// <summary>
    ///     A sub-class with a member of its own, so a row materialized as the wrong type is
    ///     observable rather than merely miscounted.
    /// </summary>
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

    /// <summary>The query vector: the "x" axis, which <see cref="TheMinute" /> sits exactly on.</summary>
    public static readonly float[] TowardsMinute = [1f, 0f, 0f];

    /// <summary>
    ///     Every body carries "fox", and the embeddings are ordered so the nearest row to
    ///     <see cref="TowardsMinute" /> is the Minute, then the base Note, and the Memo is furthest.
    /// </summary>
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

/// <summary>
///     <see cref="FullTextSearchExtensions" /> over a hierarchy. No VECTOR here deliberately, so
///     these facts also run on the `edge` lane.
/// </summary>
public class full_text_search_hierarchy_tests: OneOffConfigurationsContext
{
    private async Task<IDocumentStore> aStoreWithNotesAsync()
    {
        ConfigureStore(opts => opts.Schema.For<SearchHierarchy.Note>()
            .AddSubClass<SearchHierarchy.Memo>()
            .AddSubClass<SearchHierarchy.Minute>()
            .FullTextIndex(x => x.Body));

        await SearchHierarchy.SeedAsync(theStore, TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task a_sub_class_search_returns_only_that_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchAsync<SearchHierarchy.Memo>(
            x => x.Body, "fox", limit: 10, token: token);

        hits.Select(x => x.Id).ShouldBe([SearchHierarchy.TheMemo]);

        // The sub-class member survives, which is the half a count assertion cannot see.
        hits.Single().Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task a_sub_class_scored_search_returns_only_that_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchWithScoresAsync<SearchHierarchy.Minute>(
            x => x.Body, "fox", limit: 10, token: token);

        hits.Select(x => x.Document.Id).ShouldBe([SearchHierarchy.TheMinute]);
        hits.Single().Document.Chair.ShouldBe("oskar");
    }

    [Fact]
    public async Task a_search_for_the_root_still_returns_the_whole_hierarchy()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.FullTextSearchAsync<SearchHierarchy.Note>(
            x => x.Body, "fox", limit: 10, token: token);

        hits.Select(x => x.Id).OrderBy(x => x)
            .ShouldBe(new[] { SearchHierarchy.PlainNote, SearchHierarchy.TheMemo, SearchHierarchy.TheMinute }
                .OrderBy(x => x));
    }

    [Fact]
    public async Task a_filter_still_narrows_within_the_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // The discriminator is ANDed with the caller's filter rather than replacing it.
        var none = await session.FullTextSearchAsync<SearchHierarchy.Memo>(
            x => x.Body, "fox", limit: 10, filter: x => x.Approver == "nobody", token: token);
        none.ShouldBeEmpty();

        var one = await session.FullTextSearchAsync<SearchHierarchy.Memo>(
            x => x.Body, "fox", limit: 10, filter: x => x.Approver == "jeremy", token: token);
        one.Single().Id.ShouldBe(SearchHierarchy.TheMemo);
    }
}

/// <summary>
///     The same facts for the soft-deleted declaration, because the discriminator joined the
///     soft-delete and tenant predicates in one shared helper and a shared helper must not regress
///     the two that were already right (#723).
/// </summary>
public class soft_deleted_search_hierarchy_tests: OneOffConfigurationsContext
{
    private async Task<IDocumentStore> aStoreWithSoftDeletedNotesAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Policies.ForDocument<SearchHierarchy.Note>(p => p.SoftDeleted = true);
            opts.Schema.For<SearchHierarchy.Note>()
                .AddSubClass<SearchHierarchy.Memo>()
                .AddSubClass<SearchHierarchy.Minute>()
                .FullTextIndex(x => x.Body);
        });

        await SearchHierarchy.SeedAsync(theStore, TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task soft_deletion_still_applies_alongside_the_discriminator()
    {
        var store = await aStoreWithSoftDeletedNotesAsync();
        var token = TestContext.Current.CancellationToken;

        await using (var session = store.LightweightSession())
        {
            var found = await session.FullTextSearchAsync<SearchHierarchy.Memo>(
                x => x.Body, "fox", limit: 10, token: token);
            found.Single().Id.ShouldBe(SearchHierarchy.TheMemo);

            session.Delete<SearchHierarchy.Memo>(SearchHierarchy.TheMemo);
            await session.SaveChangesAsync(token);
        }

        await using var query = store.QuerySession();

        // Soft-deleted rows keep their tokens -- the trigger sees an UPDATE, not a DELETE -- so this
        // is the predicate on the document table doing the work, not the token table.
        var afterDelete = await query.FullTextSearchAsync<SearchHierarchy.Memo>(
            x => x.Body, "fox", limit: 10, token: token);
        afterDelete.ShouldBeEmpty();

        // ...and the sibling it shares a table with is untouched.
        var minutes = await query.FullTextSearchAsync<SearchHierarchy.Minute>(
            x => x.Body, "fox", limit: 10, token: token);
        minutes.Single().Id.ShouldBe(SearchHierarchy.TheMinute);
    }
}

/// <summary>
///     <see cref="VectorSearchExtensions" /> and <see cref="HybridSearchExtensions" /> over a
///     hierarchy. Gated on VECTOR support, which the `edge` lane has none of.
/// </summary>
public class vector_search_hierarchy_tests: OneOffConfigurationsContext
{
    private async Task<IDocumentStore> aStoreWithNotesAsync()
    {
        Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

        ConfigureStore(opts => opts.Schema.For<SearchHierarchy.Note>()
            .AddSubClass<SearchHierarchy.Memo>()
            .AddSubClass<SearchHierarchy.Minute>()
            .VectorIndex(x => x.Embedding, 3)
            .FullTextIndex(x => x.Body));

        await SearchHierarchy.SeedAsync(theStore, TestContext.Current.CancellationToken);
        return theStore;
    }

    [Fact]
    public async Task a_sub_class_search_returns_only_that_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // ⚠️ The Memo is the FURTHEST of the three from the query vector, so an unfiltered search
        // returns the Minute first -- as a Memo, with Approver at its default.
        var hits = await session.VectorSearchAsync<SearchHierarchy.Memo>(
            x => x.Embedding, SearchHierarchy.TowardsMinute, limit: 10, token: token);

        hits.Select(x => x.Id).ShouldBe([SearchHierarchy.TheMemo]);
        hits.Single().Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task a_sub_class_scored_search_returns_only_that_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.VectorSearchWithScoresAsync<SearchHierarchy.Memo>(
            x => x.Embedding, SearchHierarchy.TowardsMinute, limit: 10, token: token);

        hits.Select(x => x.Document.Id).ShouldBe([SearchHierarchy.TheMemo]);
        hits.Single().Document.Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task a_search_for_the_root_still_returns_the_whole_hierarchy()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.VectorSearchAsync<SearchHierarchy.Note>(
            x => x.Embedding, SearchHierarchy.TowardsMinute, limit: 10, token: token);

        // Nearest first, and all three -- the root's search is unchanged.
        hits.Select(x => x.Id).ShouldBe(
            [SearchHierarchy.TheMinute, SearchHierarchy.PlainNote, SearchHierarchy.TheMemo]);
    }

    [Fact]
    public async Task a_hybrid_sub_class_search_returns_only_that_sub_class()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // Both legs reach the same table, so a discriminator on only one of them lets the siblings
        // back in through the other.
        var hits = await session.HybridSearchAsync<SearchHierarchy.Memo>(
            x => x.Embedding, "fox", SearchHierarchy.TowardsMinute, limit: 10, token: token);

        hits.Select(x => x.Id).ShouldBe([SearchHierarchy.TheMemo]);
        hits.Single().Approver.ShouldBe("jeremy");
    }

    [Fact]
    public async Task a_hybrid_sub_class_search_filters_the_web_style_leg_too()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        // The WebStyle text leg reads through Query<T>(), which filtered correctly all along -- this
        // pins that the two legs now agree rather than one carrying the other.
        var hits = await session.HybridSearchAsync<SearchHierarchy.Memo>(
            x => x.Embedding, "fox", SearchHierarchy.TowardsMinute, limit: 10,
            options: new HybridSearchOptions { TextStyle = HybridTextStyle.WebStyle }, token: token);

        hits.Select(x => x.Id).ShouldBe([SearchHierarchy.TheMemo]);
    }

    [Fact]
    public async Task a_hybrid_search_for_the_root_still_returns_the_whole_hierarchy()
    {
        var store = await aStoreWithNotesAsync();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var hits = await session.HybridSearchAsync<SearchHierarchy.Note>(
            x => x.Embedding, "fox", SearchHierarchy.TowardsMinute, limit: 10, token: token);

        hits.Select(x => x.Id).OrderBy(x => x)
            .ShouldBe(new[] { SearchHierarchy.PlainNote, SearchHierarchy.TheMemo, SearchHierarchy.TheMinute }
                .OrderBy(x => x));
    }
}

/// <summary>
///     <see cref="DocumentSearchFilters" /> itself — the predicates and the ORDER they come back in,
///     which the '?'-placeholder route depends on to bind its parameters positionally.
/// </summary>
/// <remarks>
///     No database, so this holds on every lane. The order is the contract: the full-text statement
///     renders <see cref="DocumentSearchFilters.SearchFilter.ToPlaceholderSql" /> into its text and
///     then appends the non-null parameters by iterating the same list, so a reordering that the
///     renderer and the binder disagreed about would bind the tenant id to the discriminator.
/// </remarks>
public class document_search_filters_tests
{
    private static DocumentMapping aHierarchyMapping(Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions();
        configure?.Invoke(options);

        var mapping = new DocumentMapping(typeof(SearchHierarchy.Note), options);
        mapping.AddSubClass(typeof(SearchHierarchy.Memo));
        return mapping;
    }

    [Fact]
    public void a_plain_type_owes_the_document_table_nothing()
    {
        var options = new StoreOptions();
        var mapping = new DocumentMapping(typeof(SearchHierarchy.Note), options);

        DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Note), "*DEFAULT*").ShouldBeEmpty();
    }

    [Fact]
    public void the_root_of_a_hierarchy_gets_no_discriminator()
    {
        var mapping = aHierarchyMapping();

        DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Note), "*DEFAULT*").ShouldBeEmpty();
    }

    [Fact]
    public void a_sub_class_gets_the_discriminator_with_its_alias()
    {
        var mapping = aHierarchyMapping();

        var filters = DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Memo), "*DEFAULT*");

        filters.Count.ShouldBe(1);
        filters[0].ToPlaceholderSql().ShouldBe("doc_type = ?");

        // The same alias the LINQ path and the write path use, not a second spelling of the type.
        filters[0].Parameter.ShouldBe(mapping.AliasFor(typeof(SearchHierarchy.Memo)));
    }

    [Fact]
    public void the_prefix_qualifies_every_column()
    {
        var mapping = aHierarchyMapping(opts =>
        {
            opts.Policies.AllDocumentsSoftDeleted();
            opts.Policies.AllDocumentsAreMultiTenanted();
        });

        var filters = DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Memo), "acme", "d.");

        filters.Select(x => x.ToPlaceholderSql())
            .ShouldBe(["d.is_deleted = 0", "d.tenant_id = ?", "d.doc_type = ?"]);
    }

    [Fact]
    public void the_parameters_come_back_in_the_order_their_placeholders_are_rendered()
    {
        var mapping = aHierarchyMapping(opts =>
        {
            opts.Policies.AllDocumentsSoftDeleted();
            opts.Policies.AllDocumentsAreMultiTenanted();
        });

        var filters = DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Memo), "acme", "d.");

        // The soft-delete predicate binds nothing, so it contributes no parameter and must not
        // shift the two that follow it.
        filters.Where(x => x.Parameter is not null).Select(x => x.Parameter)
            .ShouldBe(["acme", mapping.AliasFor(typeof(SearchHierarchy.Memo))]);
    }

    [Fact]
    public void a_type_that_is_not_a_registered_sub_class_is_refused_by_name()
    {
        var mapping = aHierarchyMapping();

        // The same refusal the LINQ path makes, from the same DocumentMapping.AliasFor -- rather
        // than quietly searching the whole hierarchy table.
        var ex = Should.Throw<ArgumentOutOfRangeException>(() =>
            DocumentSearchFilters.For(mapping, typeof(SearchHierarchy.Minute), "*DEFAULT*"));

        ex.Message.ShouldContain(nameof(SearchHierarchy.Minute));
    }
}
