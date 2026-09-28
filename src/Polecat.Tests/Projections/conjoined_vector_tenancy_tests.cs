using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Vectors;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Projections.Vectors;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

/// <summary>
///     #681 — the conjoined-tenancy dimension of the vector features: the projection that writes
///     embeddings, and the search that reads them.
/// </summary>
/// <remarks>
///     <para>
///         <c>VectorProjection</c> is async-only, so the marten#5439 inline shape cannot occur here —
///         but nothing asserted that it writes each tenant's document under that tenant, and nothing
///         asserted that <c>VectorSearchAsync</c> scopes its results. The hybrid search was the only
///         tested one of the pair.
///     </para>
///     <para>
///         Both gate on <see cref="ConnectionSource.SupportsVector" />: the <c>edge</c> CI lane is
///         Azure SQL Edge and has neither the <c>VECTOR</c> type nor native <c>json</c>, so a fact that
///         reaches the database has to say so rather than fail at schema creation.
///     </para>
/// </remarks>
public class conjoined_vector_tenancy_tests : OneOffConfigurationsContext
{
    private readonly CountingProvider _provider = new();

    private static void requiresVectorSupport()
        => Assert.SkipUnless(ConnectionSource.SupportsVector,
            "This SQL Server build has no VECTOR type (Azure SQL Edge, or pre-2025).");

    private PageVectorProjection aConjoinedProjection()
    {
        requiresVectorSupport();

        var projection = new PageVectorProjection(_provider);

        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<PageVector>().VectorIndex(x => x.Embedding, 3);
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        return projection;
    }

    private static IEvent Event<T>(T data) where T : notnull
        => new Event<T>(data) { Id = Guid.NewGuid(), Sequence = 1, Version = 1 };

    [Fact]
    public async Task a_vector_projection_writes_each_tenants_document_under_that_tenant()
    {
        var projection = aConjoinedProjection();
        var token = TestContext.Current.CancellationToken;

        // The SAME page id in two tenants with different text, so a leak shows as the wrong content
        // rather than a missing row.
        await using (var red = theStore.LightweightSession("Red"))
        {
            await projection.ApplyAsync(red, [Event(new PageWritten("p1", "red page"))], token);
            await red.SaveChangesAsync(token);
        }

        await using (var blue = theStore.LightweightSession("Blue"))
        {
            await projection.ApplyAsync(blue, [Event(new PageWritten("p1", "blue page longer"))], token);
            await blue.SaveChangesAsync(token);
        }

        await using (var redQuery = theStore.QuerySession("Red"))
        {
            (await redQuery.LoadAsync<PageVector>("p1", token)).ShouldNotBeNull()!.Content.ShouldBe("red page");
            (await redQuery.Query<PageVector>().CountAsync(token)).ShouldBe(1);
        }

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PageVector>("p1", token)).ShouldNotBeNull()!.Content
            .ShouldBe("blue page longer");
        (await blueQuery.Query<PageVector>().CountAsync(token)).ShouldBe(1);
    }

    [Fact]
    public async Task a_vector_projection_delete_only_removes_the_tenants_own_document()
    {
        var projection = aConjoinedProjection();
        var token = TestContext.Current.CancellationToken;

        foreach (var tenant in new[] { "Red", "Blue" })
        {
            await using var session = theStore.LightweightSession(tenant);
            await projection.ApplyAsync(session, [Event(new PageWritten("p1", $"{tenant} page"))], token);
            await session.SaveChangesAsync(token);
        }

        await using (var red = theStore.LightweightSession("Red"))
        {
            await projection.ApplyAsync(red, [Event(new PageRemoved("p1"))], token);
            await red.SaveChangesAsync(token);
        }

        await using (var redQuery = theStore.QuerySession("Red"))
        {
            (await redQuery.LoadAsync<PageVector>("p1", token)).ShouldBeNull();
        }

        await using var blueQuery = theStore.QuerySession("Blue");
        (await blueQuery.LoadAsync<PageVector>("p1", token)).ShouldNotBeNull()!.Content
            .ShouldBe("Blue page");
    }

    [Fact]
    public async Task vector_search_is_scoped_to_the_session_tenant_in_both_directions()
    {
        requiresVectorSupport();

        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<TenantedPassage>().VectorIndex(x => x.Embedding, 3);
        });

        var token = TestContext.Current.CancellationToken;

        // Both tenants hold a row pointing due east, and the shared id makes the scoping observable.
        var sharedId = Guid.NewGuid();
        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Store(new TenantedPassage { Id = sharedId, Owner = "Red", Embedding = [1, 0, 0] });
            red.Store(new TenantedPassage { Id = Guid.NewGuid(), Owner = "Red", Embedding = [0.9f, 0.1f, 0] });
            await red.SaveChangesAsync(token);
        }

        await using (var blue = theStore.LightweightSession("Blue"))
        {
            blue.Store(new TenantedPassage { Id = sharedId, Owner = "Blue", Embedding = [1, 0, 0] });
            await blue.SaveChangesAsync(token);
        }

        // A limit above the store-wide row count, so the only thing keeping the other tenant out is
        // the tenant filter rather than the top-N cut.
        await using (var redSearch = theStore.QuerySession("Red"))
        {
            var nearest = await redSearch.VectorSearchAsync<TenantedPassage>(
                x => x.Embedding, new float[] { 1, 0, 0 }, 10, token: token);

            nearest.Count.ShouldBe(2);
            nearest.ShouldAllBe(x => x.Owner == "Red");
        }

        await using var blueSearch = theStore.QuerySession("Blue");
        var blueNearest = await blueSearch.VectorSearchAsync<TenantedPassage>(
            x => x.Embedding, new float[] { 1, 0, 0 }, 10, token: token);

        blueNearest.Count.ShouldBe(1);
        blueNearest.Single().Owner.ShouldBe("Blue");
    }

    public class TenantedPassage
    {
        public Guid Id { get; set; }
        public string Owner { get; set; } = "";
        public float[]? Embedding { get; set; }
    }
}
