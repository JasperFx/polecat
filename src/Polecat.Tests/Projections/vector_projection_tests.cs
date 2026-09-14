using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Vectors;
using Polecat.Projections.Vectors;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public record PageWritten(string PageId, string Text);

public record PageRemoved(string PageId);

public class PageVector: IVectorized<string>
{
    public string Id { get; set; } = null!;
    public string? Content { get; set; }
    public string? ContentHash { get; set; }
    public float[]? Embedding { get; set; }
}

/// <summary>A provider that counts calls, so "did this cost an embedding?" is observable.</summary>
public class CountingProvider: IEmbeddingProvider
{
    public int Calls { get; private set; }
    public int Texts { get; private set; }
    public int Dimensions => 3;

    public Task<ReadOnlyMemory<float>[]> GenerateEmbeddingsAsync(string[] texts, CancellationToken ct = default)
    {
        Calls++;
        Texts += texts.Length;
        return Task.FromResult(texts.Select(t => new ReadOnlyMemory<float>(
            [t.Length % 7 / 7f, t.Length % 5 / 5f, t.Length % 3 / 3f])).ToArray());
    }
}

public class PageVectorProjection(IEmbeddingProvider provider): VectorProjection<PageVector, string>(provider)
{
    protected override void Configure(VectorProjectionMap<PageVector, string> map)
    {
        map.Map<PageWritten>(e => e.Data.Text, e => e.Data.PageId);
        map.Delete<PageRemoved>(e => e.Data.PageId);
    }
}

/// <summary>
///     gh-628 — the projection that produces embeddings, so Polecat's vector search has something to
///     search.
/// </summary>
public class vector_projection_tests: OneOffConfigurationsContext
{
    private readonly CountingProvider _provider = new();

    private static void requiresVectorSupport()
        => Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

    private PageVectorProjection aProjection()
    {
        requiresVectorSupport();

        var projection = new PageVectorProjection(_provider);

        ConfigureStore(opts =>
        {
            opts.Schema.For<PageVector>().VectorIndex(x => x.Embedding, 3);
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        return projection;
    }

    private static IEvent Event<T>(T data) where T : notnull
        => new Event<T>(data) { Id = Guid.NewGuid(), Sequence = 1, Version = 1 };

    [Fact]
    public async Task an_event_produces_an_embedded_document()
    {
        var projection = aProjection();

        await using var session = theStore.LightweightSession();
        await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points east"))],
            TestContext.Current.CancellationToken);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<PageVector>("p1", TestContext.Current.CancellationToken);

        stored.ShouldNotBeNull();
        stored.Content.ShouldBe("points east");
        stored.Embedding.ShouldNotBeNull();
        stored.ContentHash.ShouldNotBeNullOrEmpty();
    }

    /// <summary>
    ///     ⚠️ The one the issue calls out as the part worth copying exactly. A rebuild runs the whole
    ///     stream through the daemon, so "rebuild the projection" and "re-embed the corpus, at cost"
    ///     must not be the same operation.
    /// </summary>
    [Fact]
    public async Task unchanged_content_costs_no_provider_call()
    {
        var projection = aProjection();

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points east"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var afterFirst = _provider.Calls;

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points east"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        _provider.Calls.ShouldBe(afterFirst);
    }

    /// <summary>Changed content does pay, or the hash would be hiding real work.</summary>
    [Fact]
    public async Task changed_content_is_embedded_again()
    {
        var projection = aProjection();

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points east"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var afterFirst = _provider.Calls;

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points north instead"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        _provider.Calls.ShouldBe(afterFirst + 1);
    }

    /// <summary>One call for a page, not one per row — the metered part is the round trip.</summary>
    [Fact]
    public async Task a_page_costs_one_provider_call()
    {
        var projection = aProjection();

        await using var session = theStore.LightweightSession();
        await projection.ApplyAsync(session,
        [
            Event(new PageWritten("p1", "one")),
            Event(new PageWritten("p2", "two")),
            Event(new PageWritten("p3", "three"))
        ], TestContext.Current.CancellationToken);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        _provider.Calls.ShouldBe(1);
        _provider.Texts.ShouldBe(3);
    }

    /// <summary>A retraction removes the row, addressed by the id the MAP produced.</summary>
    [Fact]
    public async Task a_delete_removes_the_document()
    {
        var projection = aProjection();

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", "points east"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageRemoved("p1"))],
                TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<PageVector>("p1", TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    /// <summary>
    ///     ⚠️ A write and a retraction of the same id in ONE page ends deleted. marten#5422 is this
    ///     defect: running all deletes before all upserts leaves the row in the index.
    /// </summary>
    [Fact]
    public async Task a_write_and_a_delete_in_one_page_ends_deleted()
    {
        var projection = aProjection();

        await using var session = theStore.LightweightSession();
        await projection.ApplyAsync(session,
            [Event(new PageWritten("p1", "points east")), Event(new PageRemoved("p1"))],
            TestContext.Current.CancellationToken);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<PageVector>("p1", TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    /// <summary>
    ///     ⚠️ Inline is refused by name when the store is BUILT, not on the first event. Embedding is a
    ///     metered network call and Inline would put it inside every caller's SaveChangesAsync.
    /// </summary>
    [Fact]
    public void an_inline_registration_is_refused_by_name()
    {
        requiresVectorSupport();

        var ex = Should.Throw<Exception>(() => DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionSource.ConnectionString);
            opts.DatabaseSchemaName = "vector_projection_inline";
            opts.Schema.For<PageVector>().VectorIndex(x => x.Embedding, 3);
            opts.Projections.Add(new PageVectorProjection(_provider), ProjectionLifecycle.Inline);
        }));

        ex.Message.ShouldContain("asynchronous only");
    }

    /// <summary>A projection that maps nothing would read every page and write nothing.</summary>
    [Fact]
    public void a_projection_that_maps_nothing_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => new EmptyProjection(_provider))
            .Message.ShouldContain("configured no event mappings");
    }
}

public class EmptyProjection(IEmbeddingProvider provider): VectorProjection<PageVector, string>(provider)
{
    protected override void Configure(VectorProjectionMap<PageVector, string> map)
    {
    }
}
