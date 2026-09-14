using JasperFx.Events.Projections;
using JasperFx.Events.Vectors;
using Polecat.Projections.Vectors;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public record MemoryStarted(string Title, string Body);

/// <summary>A PARTIAL update: null means "unchanged", which is what breaks a per-event selector.</summary>
public record MemoryRevised(string? Title, string? Body);

public partial class Memory
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Body { get; set; } = string.Empty;

    public static Memory Create(MemoryStarted e) => new() { Title = e.Title, Body = e.Body };

    public void Apply(MemoryRevised e)
    {
        if (e.Title is not null) Title = e.Title;
        if (e.Body is not null) Body = e.Body;
    }
}

public class MemoryVector: IVectorized<Guid>
{
    public Guid Id { get; set; }
    public string? Content { get; set; }
    public string? ContentHash { get; set; }
    public float[]? Embedding { get; set; }
}

public class MemoryVectorProjection(IEmbeddingProvider provider): VectorProjection<MemoryVector, Guid>(provider)
{
    protected override void Configure(VectorProjectionMap<Guid> map)
    {
        map.MapFromAggregate<Memory>(
            memory => $"{memory.Title}\n{memory.Body}",
            (typeof(MemoryStarted), e => e.StreamId),
            (typeof(MemoryRevised), e => e.StreamId));
    }
}

/// <summary>A projection keyed on something that is not the stream, which live aggregation cannot serve.</summary>
public class StringKeyedAggregateProjection(IEmbeddingProvider provider)
    : VectorProjection<PageVector, string>(provider)
{
    protected override void Configure(VectorProjectionMap<string> map)
    {
        map.MapFromAggregate<Memory>(
            memory => memory.Body,
            (typeof(MemoryStarted), e => e.StreamId.ToString()));
    }
}

/// <summary>
///     #633 / jasperfx#841: <c>MapFromAggregate</c>, the capability the shared
///     <see cref="VectorProjectionMap{TId}" /> exists for.
/// </summary>
/// <remarks>
///     A content selector that sees one event cannot keep an embedding correct across a partial-update
///     event: given <c>MemoryRevised { Title = null, Body = "new" }</c> where null means "unchanged",
///     returning the new body re-embeds the document without its title and returning null leaves the
///     embedding stale. Both answers are wrong, and there was no third one.
/// </remarks>
public class vector_projection_from_aggregate_tests: OneOffConfigurationsContext
{
    private readonly CountingProvider _provider = new();

    private static void requiresVectorSupport()
        => Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

    private MemoryVectorProjection aProjection()
    {
        requiresVectorSupport();

        var projection = new MemoryVectorProjection(_provider);

        ConfigureStore(opts =>
        {
            opts.Schema.For<MemoryVector>().VectorIndex(x => x.Embedding, 3);
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        return projection;
    }

    [Fact]
    public async Task a_partial_update_embeds_the_whole_aggregate_not_the_event()
    {
        var projection = aProjection();
        var token = TestContext.Current.CancellationToken;
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Memory>(streamId,
                new MemoryStarted("Quarterly review", "Revenue rose on renewals."),
                new MemoryRevised(null, "Revenue rose on renewals and new logos."));
            await session.SaveChangesAsync(token);
        }

        await using (var session = theStore.LightweightSession())
        {
            var events = await session.Events.FetchStreamAsync(streamId, token: token);
            await projection.ApplyAsync(session, events, token);
            await session.SaveChangesAsync(token);
        }

        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<MemoryVector>(streamId, token);

        stored.ShouldNotBeNull();

        // The title survives the revision that did not mention it, which is the whole point. A
        // per-event selector would have embedded only the new body.
        stored.Content.ShouldBe("Quarterly review\nRevenue rose on renewals and new logos.");
        stored.Embedding.ShouldNotBeNull();
    }

    [Fact]
    public async Task an_event_that_does_not_move_the_built_text_costs_no_model_call()
    {
        var projection = aProjection();
        var token = TestContext.Current.CancellationToken;
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Memory>(streamId, new MemoryStarted("Review", "Body"));
            await session.SaveChangesAsync(token);

            var events = await session.Events.FetchStreamAsync(streamId, token: token);
            await projection.ApplyAsync(session, events, token);
            await session.SaveChangesAsync(token);
        }

        var afterFirst = _provider.Calls;

        // A revision that changes nothing the content selector reads. The aggregate is rebuilt, the
        // text is hashed, the hash matches, and the model is never called.
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId, new MemoryRevised(null, null));
            await session.SaveChangesAsync(token);

            var events = await session.Events.FetchStreamAsync(streamId, fromVersion: 2, token: token);
            await projection.ApplyAsync(session, events, token);
            await session.SaveChangesAsync(token);
        }

        _provider.Calls.ShouldBe(afterFirst);
    }

    /// <summary>
    ///     ⚠️ Polecat serves <c>MapFromAggregate</c> by LIVE-AGGREGATING the stream the trigger names,
    ///     so the document id has to be the stream id. A projection keyed on anything else would
    ///     aggregate nothing and silently never write an embedding — refused when the store is built
    ///     instead.
    /// </summary>
    [Fact]
    public void an_id_that_is_not_the_stream_identity_is_refused_by_name()
    {
        requiresVectorSupport();

        var ex = Should.Throw<Exception>(() => DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionSource.ConnectionString);
            opts.DatabaseSchemaName = "vector_projection_aggregate_id";
            opts.Schema.For<PageVector>().VectorIndex(x => x.Embedding, 3);
            opts.Projections.Add(new StringKeyedAggregateProjection(_provider), ProjectionLifecycle.Async);
        }));

        ex.Message.ShouldContain("stream identity type");
    }

    /// <summary>
    ///     Two aggregate mappings would leave which one builds the text depending on registration
    ///     order, which is the shared map's refusal rather than Polecat's.
    /// </summary>
    [Fact]
    public void a_second_aggregate_mapping_is_refused()
    {
        var map = new VectorProjectionMap<Guid>();
        map.MapFromAggregate<Memory>(x => x.Body, (typeof(MemoryStarted), e => e.StreamId));

        Should.Throw<InvalidOperationException>(() =>
                map.MapFromAggregate<Memory>(x => x.Title, (typeof(MemoryRevised), e => e.StreamId)))
            .Message.ShouldContain("already built from");
    }
}
