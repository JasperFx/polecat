using JasperFx.Events.Documents;
using JasperFx.Events.Vectors;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     #633 / jasperfx#842: <c>IDocumentReadOperations.Search</c>, the store-neutral similarity-search
///     contract.
/// </summary>
/// <remarks>
///     <para>
///         The point is reachability, not new behaviour: every store's search entry point is an
///         extension method that casts to store internals in its first statement, so code written
///         against <see cref="IDocumentReadOperations" /> — a library that compiles once against
///         Marten, Polecat and Fisher — had no way to ask for "the ten nearest documents by
///         embedding" at all.
///     </para>
///     <para>
///         ⚠️ The contract is reached through an ACCESSOR rather than being members on the session.
///         Polecat's extension methods are already named <c>VectorSearchWithScoresAsync</c> and
///         <c>HybridSearchWithScoresAsync</c>; instance members of those names would beat the
///         extensions at every existing call site, silently and against a different implementation.
///     </para>
/// </remarks>
public class document_search_operations_tests: OneOffConfigurationsContext
{
    public class Passage
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public float[]? Embedding { get; set; }
    }

    private static readonly Guid BothLegs = Guid.NewGuid();
    private static readonly Guid TextOnly = Guid.NewGuid();
    private static readonly Guid VectorOnly = Guid.NewGuid();

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
        session.Store(new Passage { Id = BothLegs, Body = "a fox in the snow", Embedding = [0, 0, 1] });
        session.Store(new Passage { Id = TextOnly, Body = "a fox and nothing else", Embedding = [1, 0, 0] });
        session.Store(new Passage { Id = VectorOnly, Body = "a turtle naps", Embedding = [0, 0.1f, 0.99f] });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return theStore;
    }

    [Fact]
    public async Task vector_search_is_reachable_without_naming_a_polecat_type()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();

        // Deliberately held as the shared contract, which is the whole capability.
        IDocumentReadOperations reads = session;

        var scored = await reads.Search.VectorSearchWithScoresAsync<Passage>(
            x => x.Embedding, new float[] { 0, 0, 1 }, 2,
            token: TestContext.Current.CancellationToken);

        scored[0].Document.Id.ShouldBe(BothLegs);
        scored.Select(x => x.Distance).ShouldBeInOrder();

        // The document-only form is an extension over the scored one, so an implementer writes two
        // methods and every store projects to documents identically.
        var documents = await reads.Search.VectorSearchAsync<Passage>(
            x => x.Embedding, new float[] { 0, 0, 1 }, 2,
            token: TestContext.Current.CancellationToken);

        documents[0].Id.ShouldBe(BothLegs);
    }

    [Fact]
    public async Task hybrid_search_is_reachable_the_same_way()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();
        IDocumentReadOperations reads = session;

        var fused = await reads.Search.HybridSearchWithScoresAsync<Passage>(
            x => x.Embedding, "fox", new float[] { 0, 0, 1 },
            token: TestContext.Current.CancellationToken);

        fused[0].Document.Id.ShouldBe(BothLegs);
        fused.ShouldAllBe(x => x.Score > 0);
    }

    [Fact]
    public async Task the_filter_reaches_through_the_contract()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.QuerySession();
        IDocumentReadOperations reads = session;

        var filtered = await reads.Search.VectorSearchAsync<Passage>(
            x => x.Embedding, new float[] { 0, 0, 1 }, 1,
            filter: x => x.Body != "a fox in the snow",
            token: TestContext.Current.CancellationToken);

        filtered.Single().Id.ShouldBe(VectorOnly);
    }

    /// <summary>
    ///     ⚠️ A write session is an <see cref="IDocumentReadOperations" /> too, so the accessor has to
    ///     be there as well — it is one explicit implementation on <c>IQuerySession</c> covering every
    ///     session type rather than one per type.
    /// </summary>
    [Fact]
    public async Task the_accessor_is_on_every_session_type()
    {
        var store = await aStoreWithPassagesAsync();
        await using var session = store.LightweightSession();
        IDocumentReadOperations reads = session;

        var documents = await reads.Search.VectorSearchAsync<Passage>(
            x => x.Embedding, new float[] { 0, 0, 1 }, 1,
            token: TestContext.Current.CancellationToken);

        documents.Single().Id.ShouldBe(BothLegs);
    }
}
