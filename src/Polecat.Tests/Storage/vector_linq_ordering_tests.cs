using JasperFx.Events.Vectors;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     Ordering a LINQ query by vector distance — what widening <c>Statement.OrderBys</c> to
///     <c>ISqlFragment</c> made expressible.
/// </summary>
/// <remarks>
///     <para>
///         An ordering key used to be text appended verbatim, so a vector distance could not be one:
///         the query vector is a VALUE and has to bind. That is why <c>VectorSearchAsync</c> is written
///         against raw SQL, and it stays — the two are not redundant. It is the whole search in one
///         call and returns scores; this COMPOSES with the rest of LINQ, which raw SQL cannot.
///     </para>
/// </remarks>
public class vector_linq_ordering_tests: OneOffConfigurationsContext
{
    public class Passage
    {
        public Guid Id { get; set; }
        public string Team { get; set; } = "";
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
    private static readonly Guid RedNorth = Guid.NewGuid();

    private static void requiresVectorSupport()
        => Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

    private async Task seed()
    {
        requiresVectorSupport();

        ConfigureStore(opts =>
        {
            opts.Schema.For<Passage>().VectorIndex(x => x.Embedding, 3);
            opts.Schema.For<Unindexed>();
        });

        await using var session = theStore.LightweightSession();
        session.Store(
            new Passage { Id = East, Team = "blue", Embedding = [1, 0, 0] },
            new Passage { Id = Between, Team = "blue", Embedding = [0.7f, 0.7f, 0] },
            new Passage { Id = North, Team = "blue", Embedding = [0, 1, 0] },
            new Passage { Id = RedNorth, Team = "red", Embedding = [0, 1, 0] });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The ordering binds its parameter and sorts nearest first.</summary>
    [Fact]
    public async Task orders_by_distance_nearest_first()
    {
        await seed();

        await using var session = theStore.QuerySession();

        var ordered = await session.Query<Passage>()
            .OrderByVectorDistance(x => x.Embedding, new ReadOnlyMemory<float>([1f, 0f, 0f]))
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered[0].Id.ShouldBe(East);
        ordered[1].Id.ShouldBe(Between);
    }

    /// <summary>
    ///     ⚠️ The one that says why this exists. A filter and a vector ordering in one query is what
    ///     <c>VectorSearchAsync</c>'s raw SQL cannot express.
    /// </summary>
    [Fact]
    public async Task composes_with_an_ordinary_where()
    {
        await seed();

        await using var session = theStore.QuerySession();

        var ordered = await session.Query<Passage>()
            .Where(x => x.Team == "red")
            .OrderByVectorDistance(x => x.Embedding, new ReadOnlyMemory<float>([0f, 1f, 0f]))
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.Count.ShouldBe(1);
        ordered[0].Id.ShouldBe(RedNorth);
    }

    /// <summary>And with paging, which is the other half of composing.</summary>
    [Fact]
    public async Task composes_with_take()
    {
        await seed();

        await using var session = theStore.QuerySession();

        var ordered = await session.Query<Passage>()
            .OrderByVectorDistance(x => x.Embedding, new ReadOnlyMemory<float>([1f, 0f, 0f]))
            .Take(2)
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.Count.ShouldBe(2);
        ordered[0].Id.ShouldBe(East);
    }

    /// <summary>
    ///     The refusals are <c>VectorSearchAsync</c>'s own, because both call one resolver — two copies
    ///     would be two chances for one path to stop making them.
    /// </summary>
    [Fact]
    public async Task a_type_with_no_declared_index_is_refused_by_name()
    {
        await seed();

        await using var session = theStore.QuerySession();

        var ex = Should.Throw<InvalidOperationException>(() => session.Query<Unindexed>()
            .OrderByVectorDistance(x => x.Embedding, new ReadOnlyMemory<float>([1f, 0f, 0f])));

        ex.Message.ShouldContain("declares no vector index");
    }

    [Fact]
    public async Task a_query_vector_of_the_wrong_length_is_refused_by_name()
    {
        await seed();

        await using var session = theStore.QuerySession();

        var ex = Should.Throw<ArgumentException>(() => session.Query<Passage>()
            .OrderByVectorDistance(x => x.Embedding, new ReadOnlyMemory<float>([1f, 0f])));

        ex.Message.ShouldContain("was declared with 3");
    }
}
