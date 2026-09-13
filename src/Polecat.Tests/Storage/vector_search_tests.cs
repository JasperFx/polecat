using JasperFx.Events.Vectors;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     Vector search over a member declared with <c>Schema.For&lt;T&gt;().VectorIndex(...)</c>.
/// </summary>
/// <remarks>
///     <para>
///         The embedding is a persisted computed <c>VECTOR(n)</c> column over the JSON, so the write
///         path is untouched and the column cannot drift from the document. The tests that matter are
///         the ones proving that: a row written before the declaration existed is still searchable,
///         and a row updated by raw SQL past Polecat ranks by its new vector.
///     </para>
/// </remarks>
public class vector_search_tests: OneOffConfigurationsContext
{
    public class Passage
    {
        public Guid Id { get; set; }
        public string Text { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }

    public class Unindexed
    {
        public Guid Id { get; set; }
        public float[]? Embedding { get; set; }
    }

    private static readonly Guid East = Guid.NewGuid();
    private static readonly Guid North = Guid.NewGuid();
    private static readonly Guid Between = Guid.NewGuid();
    private static readonly Guid FarEast = Guid.NewGuid();
    private static readonly Guid Blank = Guid.NewGuid();

    private async Task<IDocumentStore> aStoreWithPassages()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<Passage>().VectorIndex(x => x.Embedding, 3);
            opts.Schema.For<Unindexed>();
        });

        await using var session = theStore.LightweightSession();
        session.Store(
            new Passage { Id = East, Text = "points east", Embedding = [1, 0, 0] },
            new Passage { Id = North, Text = "points north", Embedding = [0, 1, 0] },
            new Passage { Id = Between, Text = "between", Embedding = [0.7f, 0.7f, 0] },
            new Passage { Id = FarEast, Text = "further, same direction", Embedding = [5, 0, 0] },
            new Passage { Id = Blank, Text = "no embedding", Embedding = null });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return theStore;
    }

    // ---- the declaration -------------------------------------------------------------------------

    [Fact]
    public void the_column_expression_uses_json_query_not_json_value()
    {
        // JSON_VALUE truncates at 4000 characters, which a 768-float array is several times over, so
        // the scalar form every other computed column here uses cannot carry an embedding at all.
        var index = new VectorIndex("$.embedding", null, 768, DistanceFunction.Cosine);

        index.ColumnExpression().ShouldBe("CAST(JSON_QUERY(data, '$.embedding') AS VECTOR(768))");
        index.ColumnName.ShouldBe("vec_embedding");
    }

    [Fact]
    public void every_metric_is_a_distance_on_sql_server()
    {
        // Including dot, which the engine returns negated — so one ORDER BY serves all three and the
        // promise DistanceFunction makes on every store holds here with nothing to correct.
        VectorIndex.MetricName(DistanceFunction.Cosine).ShouldBe("cosine");
        VectorIndex.MetricName(DistanceFunction.L2).ShouldBe("euclidean");
        VectorIndex.MetricName(DistanceFunction.InnerProduct).ShouldBe("dot");
    }

    [Fact]
    public void declaring_is_refused_by_name_for_the_wrong_member_type_dimensions_or_twice()
    {
        Should.Throw<InvalidOperationException>(() =>
                new DocumentMappingExpression<Passage>().VectorIndex(x => x.Text, 3))
            .Message.ShouldContain("which cannot hold an embedding");

        Should.Throw<ArgumentOutOfRangeException>(() =>
            new DocumentMappingExpression<Passage>().VectorIndex(x => x.Embedding, 0));

        Should.Throw<InvalidOperationException>(() =>
                new DocumentMappingExpression<Passage>()
                    .VectorIndex(x => x.Embedding, 3)
                    .VectorIndex(x => x.Embedding, 4))
            .Message.ShouldContain("already carries a vector index");
    }

    // ---- searching -------------------------------------------------------------------------------

    [Fact]
    public async Task nearest_first_by_cosine_and_the_limit_holds()
    {
        var store = await aStoreWithPassages();
        await using var session = store.QuerySession();

        var nearest = await session.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 1, 0, 0 },
            3, token: TestContext.Current.CancellationToken);

        // Cosine ignores magnitude, so east and far-east tie at 0, then between, then north.
        nearest.Select(x => x.Id).Take(2).ShouldBe([East, FarEast], true);
        nearest[2].Id.ShouldBe(Between);
        nearest.Count.ShouldBe(3);
    }

    [Fact]
    public async Task scores_are_distances_smaller_is_closer_under_every_metric()
    {
        var store = await aStoreWithPassages();
        await using var session = store.QuerySession();
        var query = new float[] { 1, 0, 0 };
        var token = TestContext.Current.CancellationToken;

        var cosine = await session.VectorSearchWithScoresAsync<Passage>(x => x.Embedding, query, 4, token: token);
        cosine.Select(m => m.Distance).ShouldBeInOrder();
        cosine[0].Distance.ShouldBe(0, 1e-5);
        cosine.Single(m => m.Document.Id == North).Distance.ShouldBe(1, 1e-5);

        // Euclidean does not ignore magnitude: far-east is now far.
        var l2 = await session.VectorSearchWithScoresAsync<Passage>(x => x.Embedding, query, 4,
            DistanceFunction.L2, token);
        l2[0].Document.Id.ShouldBe(East);
        l2.Single(m => m.Document.Id == FarEast).Distance.ShouldBe(4, 1e-5);

        // Dot comes back negated, so it is still a distance: far-east is the best match.
        var dot = await session.VectorSearchWithScoresAsync<Passage>(x => x.Embedding, query, 4,
            DistanceFunction.InnerProduct, token);
        dot[0].Document.Id.ShouldBe(FarEast);
        dot[0].Distance.ShouldBe(-5, 1e-5);
    }

    [Fact]
    public async Task a_document_with_no_embedding_is_skipped_not_scored()
    {
        var store = await aStoreWithPassages();
        await using var session = store.QuerySession();

        var all = await session.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 0, 0, 1 },
            10, token: TestContext.Current.CancellationToken);

        all.Select(x => x.Id).ShouldNotContain(Blank);
        all.Count.ShouldBe(4);
    }

    [Fact]
    public async Task a_write_that_bypassed_polecat_still_ranks_by_the_new_vector()
    {
        // The reason the column is computed rather than written: it cannot drift from the document.
        var store = await aStoreWithPassages();

        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"UPDATE {GetType().Name.ToLowerInvariant()}.pc_doc_passage SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), "
                + "'$.embedding', JSON_QUERY('[0,0,1]')) WHERE id = @id";
            cmd.Parameters.AddWithValue("@id", North);
            (await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken)).ShouldBe(1);
        }

        await using var session = store.QuerySession();
        var nearest = await session.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 0, 0, 1 },
            1, token: TestContext.Current.CancellationToken);

        nearest.Single().Id.ShouldBe(North);
    }

    [Fact]
    public async Task declaring_a_vector_on_a_type_that_already_has_rows_needs_no_backfill()
    {
        // The payoff of a computed column over a written one. The rows below are stored by a store
        // that has never heard of a vector index; adding the declaration runs one ALTER TABLE and
        // every existing row is searchable at once, because SQL Server computes a PERSISTED column
        // for the rows already there.
        var token = TestContext.Current.CancellationToken;
        var before = Guid.NewGuid();

        ConfigureStore(_ => { });
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Passage { Id = before, Text = "written first", Embedding = [0, 0, 1] });
            await session.SaveChangesAsync(token);
        }

        ConfigureStore(opts => opts.Schema.For<Passage>().VectorIndex(x => x.Embedding, 3));

        await using var query = theStore.QuerySession();
        var nearest = await query.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 0, 0, 1 },
            1, token: token);

        nearest.Single().Id.ShouldBe(before);
    }

    [Fact]
    public async Task refusals_name_the_problem()
    {
        var store = await aStoreWithPassages();
        await using var session = store.QuerySession();
        var token = TestContext.Current.CancellationToken;

        var noIndex = await Should.ThrowAsync<InvalidOperationException>(() =>
            session.VectorSearchAsync<Unindexed>(x => x.Embedding, new float[] { 1, 0, 0 }, token: token));
        noIndex.Message.ShouldContain("'Unindexed' declares no vector index");

        var wrongMember = await Should.ThrowAsync<InvalidOperationException>(() =>
            session.VectorSearchAsync<Passage>(x => x.Text, new float[] { 1, 0, 0 }, token: token));
        wrongMember.Message.ShouldContain("is not a declared vector member");
        wrongMember.Message.ShouldContain("Declared: Embedding");

        var wrongLength = await Should.ThrowAsync<ArgumentException>(() =>
            session.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 1, 0 }, token: token));
        wrongLength.Message.ShouldContain("has 2 dimensions but 'Passage.Embedding' was declared with 3");

        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.VectorSearchAsync<Passage>(x => x.Embedding, new float[] { 1, 0, 0 }, 0, token: token));
    }
}
