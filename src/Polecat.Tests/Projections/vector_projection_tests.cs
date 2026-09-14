using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Vectors;
using Polecat.Projections;
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
    protected override void Configure(VectorProjectionMap<string> map)
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

    /// <summary>
    ///     ⚠️ #633: the content hash is PERSISTED, so adopting the shared
    ///     <c>VectorEmbeddingPlan.HashOf</c> had to leave the spelling exactly where gh-628 left it —
    ///     lowercase hex SHA-256 of the UTF-8 text. A different spelling re-embeds every stored
    ///     document once, which is a real bill against a metered provider rather than a cosmetic
    ///     difference.
    /// </summary>
    [Fact]
    public void the_content_hash_is_lowercase_hex_sha256_of_the_utf8_text()
    {
        // Written out rather than recomputed, so the fact FAILS if the spelling ever moves instead of
        // moving with it.
        VectorEmbeddingPlan<string>.HashOf("points east")
            .ShouldBe("29e1028bfc8df26dd37cb73c1d219c0b7841fbe29892489eb988e8c8fe8c1c47");
    }

    /// <summary>
    ///     A row whose stored hash was written by gh-628's private hashing is still recognised as
    ///     unchanged after the swap to the shared one — the proof that the bump re-embeds nothing.
    /// </summary>
    [Fact]
    public async Task a_row_hashed_before_the_swap_is_still_unchanged()
    {
        var projection = aProjection();
        var token = TestContext.Current.CancellationToken;

        await using (var session = theStore.LightweightSession())
        {
            // Exactly what gh-628 wrote: Convert.ToHexStringLower(SHA256.HashData(UTF8(content))).
            session.Store(new PageVector
            {
                Id = "p9",
                Content = "points east",
                ContentHash = Convert.ToHexStringLower(
                    System.Security.Cryptography.SHA256.HashData(
                        System.Text.Encoding.UTF8.GetBytes("points east"))),
                Embedding = [1, 0, 0]
            });
            await session.SaveChangesAsync(token);
        }

        var before = _provider.Calls;

        await using (var session = theStore.LightweightSession())
        {
            await projection.ApplyAsync(session, [Event(new PageWritten("p9", "points east"))], token);
            await session.SaveChangesAsync(token);
        }

        _provider.Calls.ShouldBe(before);
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
    protected override void Configure(VectorProjectionMap<string> map)
    {
    }
}

/// <summary>A bare projection that carries configuration checks of its own.</summary>
public class ComplainingProjection: IProjection, IValidatedProjection<StoreOptions>
{
    public const string Complaint = "this projection is not configured the way it needs to be";

    public IEnumerable<string> ValidateConfiguration(StoreOptions options) => [Complaint];

    public Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation) => Task.CompletedTask;
}

/// <summary>
///     ⚠️ #633 / jasperfx#845: a BARE <see cref="IProjection" /> is registered through a
///     <c>ProjectionWrapper</c>, and <c>ProjectionGraph.AssertValidity</c> used to run
///     <c>OfType&lt;IValidatedProjection&lt;T&gt;&gt;()</c> over the wrappers — so a projection
///     carrying its own configuration checks was never asked, in silence.
/// </summary>
/// <remarks>
///     This is why Polecat carried a private <c>IVectorProjection</c> marker and a validation pass of
///     its own until this issue. It is also the one behaviour change a Polecat APPLICATION can notice
///     on the 2.70.0 bump: a hand-written projection that implemented the interface starts being
///     asked, which can surface a configuration error that was quietly passing before.
/// </remarks>
public class bare_projection_validation_tests
{
    [Fact]
    public void a_bare_projection_is_asked_to_validate_itself()
    {
        var ex = Should.Throw<Exception>(() => DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionSource.ConnectionString);
            opts.DatabaseSchemaName = "bare_projection_validation";
            opts.Projections.Add(new ComplainingProjection(), ProjectionLifecycle.Async);
        }));

        ex.Message.ShouldContain(ComplainingProjection.Complaint);
    }
}
