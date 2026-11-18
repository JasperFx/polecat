using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     Session-semantics audit (Stoat plan critter-hardening-audit, node
///     polecat-session-semantics-audit) — the Polecat analogue of marten#4801 / #4947 / #4956.
///
///     Marten's bugs were identity-map <em>cache</em> collisions: a nested ForTenant() session
///     shared the parent's tenant-blind ItemMap, so a cache hit could hand back another tenant's
///     instance while the miss path stayed correctly tenant-scoped. Polecat's NestedTenantSession
///     is a delegating wrapper rather than a real session, so the same question has to be asked one
///     level lower: is the <em>read</em> scoped to the override tenant at all?
///
///     Every read member on NestedTenantSession forwards to the parent session
///     (<c>_parent.LoadAsync&lt;T&gt;(id, token)</c>, <c>_parent.Query&lt;T&gt;()</c>, …) with no
///     tenant argument, so under conjoined tenancy — where the same id names a different document
///     per tenant — a read through ForTenant() answers from the parent's tenant. The writes are
///     tenant-correct (they go through TenantScopedStorageSession), which is what makes this quiet:
///     the row lands under the right tenant and is then read back as the wrong one.
/// </summary>
[Collection("integration")]
public class for_tenant_read_scoping_tests : IntegrationContext
{
    public for_tenant_read_scoping_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task ConfigureAsync(string schema)
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
    }

    /// <summary>
    ///     The core shape: one id, two tenants, two different documents. A read through
    ///     ForTenant(other) must answer with the other tenant's document.
    /// </summary>
    [Fact]
    public async Task load_through_for_tenant_reads_the_override_tenant()
    {
        await ConfigureAsync("ft_read_load");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id, Name = "A" });
            writes.ForTenant("tenant-b").Store(new TenantDoc { Id = id, Name = "B" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // A session whose own tenant is "tenant-a", reading tenant-b's copy through ForTenant.
        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        var mine = await session.LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);
        mine.ShouldNotBeNull();
        mine.Name.ShouldBe("A");

        var theirs = await session.ForTenant("tenant-b")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);

        theirs.ShouldNotBeNull();
        theirs.Name.ShouldBe("B");
    }

    /// <summary>
    ///     Order-independence: reading the override tenant FIRST must still answer that tenant, and
    ///     must not poison the parent's own subsequent read. This is the shape marten#4801 fixed —
    ///     whichever read populates the shared map first wins for both tenants.
    /// </summary>
    [Fact]
    public async Task for_tenant_read_first_does_not_poison_the_parent_read()
    {
        await ConfigureAsync("ft_read_order");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id, Name = "A" });
            writes.ForTenant("tenant-b").Store(new TenantDoc { Id = id, Name = "B" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.IdentitySession(new SessionOptions { TenantId = "tenant-a" });

        var theirs = await session.ForTenant("tenant-b")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);
        theirs.ShouldNotBeNull();
        theirs.Name.ShouldBe("B");

        var mine = await session.LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);
        mine.ShouldNotBeNull();
        mine.Name.ShouldBe("A");
    }

    /// <summary>
    ///     Two different override tenants through one parent session. Under a tenant-blind identity
    ///     map the second read is answered from the first tenant's cached instance.
    /// </summary>
    [Fact]
    public async Task two_for_tenant_views_do_not_share_an_identity_map_entry()
    {
        await ConfigureAsync("ft_read_two");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-b").Store(new TenantDoc { Id = id, Name = "B" });
            writes.ForTenant("tenant-c").Store(new TenantDoc { Id = id, Name = "C" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.IdentitySession(new SessionOptions { TenantId = "tenant-a" });

        var b = await session.ForTenant("tenant-b")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);
        var c = await session.ForTenant("tenant-c")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);

        b.ShouldNotBeNull();
        c.ShouldNotBeNull();
        b.Name.ShouldBe("B");
        c.Name.ShouldBe("C");
    }

    /// <summary>
    ///     A tenant with no row of its own must read null rather than falling through to the
    ///     parent tenant's document. This is the read-side leak in its most dangerous form: the
    ///     caller believes the document belongs to the tenant it asked for.
    /// </summary>
    [Fact]
    public async Task for_tenant_read_of_a_missing_document_does_not_fall_through_to_the_parent()
    {
        await ConfigureAsync("ft_read_missing");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id, Name = "A" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        var theirs = await session.ForTenant("tenant-empty")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);

        theirs.ShouldBeNull();
    }

    /// <summary>
    ///     LINQ through ForTenant must be scoped the same way LoadAsync is.
    /// </summary>
    [Fact]
    public async Task query_through_for_tenant_reads_the_override_tenant()
    {
        await ConfigureAsync("ft_read_query");

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = Guid.NewGuid(), Name = "A" });
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = Guid.NewGuid(), Name = "A" });
            writes.ForTenant("tenant-b").Store(new TenantDoc { Id = Guid.NewGuid(), Name = "B" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        var names = await session.ForTenant("tenant-b").Query<TenantDoc>()
            .OrderBy(x => x.Name)
            .ToListAsync(TestContext.Current.CancellationToken);

        names.Count.ShouldBe(1);
        names.Single().Name.ShouldBe("B");
    }

    /// <summary>
    ///     CheckExistsAsync is the cheapest read there is, and the one most likely to be used as an
    ///     authorization gate ("does this tenant own that id?"). It must not answer for the parent.
    /// </summary>
    [Fact]
    public async Task check_exists_through_for_tenant_reads_the_override_tenant()
    {
        await ConfigureAsync("ft_read_exists");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id, Name = "A" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        (await session.CheckExistsAsync<TenantDoc>(id, TestContext.Current.CancellationToken))
            .ShouldBeTrue();

        (await session.ForTenant("tenant-empty")
                .CheckExistsAsync<TenantDoc>(id, TestContext.Current.CancellationToken))
            .ShouldBeFalse();
    }

    /// <summary>
    ///     LoadManyAsync — the batched read path — must be scoped like the singular one.
    /// </summary>
    [Fact]
    public async Task load_many_through_for_tenant_reads_the_override_tenant()
    {
        await ConfigureAsync("ft_read_many");

        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id1, Name = "A1" });
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id2, Name = "A2" });
            writes.ForTenant("tenant-b").Store(new TenantDoc { Id = id1, Name = "B1" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        var docs = await session.ForTenant("tenant-b")
            .LoadManyAsync<TenantDoc>(new[] { id1, id2 }, TestContext.Current.CancellationToken);

        docs.Count.ShouldBe(1);
        docs.Single().Name.ShouldBe("B1");
    }

    /// <summary>
    ///     ForTenant() for the session's OWN tenant must stay consistent with reading the session
    ///     directly — the guard rail marten#4801 added after its first cut over-isolated.
    /// </summary>
    [Fact]
    public async Task for_tenant_of_the_sessions_own_tenant_matches_the_session()
    {
        await ConfigureAsync("ft_read_same");

        var id = Guid.NewGuid();

        await using (var writes = theStore.LightweightSession())
        {
            writes.ForTenant("tenant-a").Store(new TenantDoc { Id = id, Name = "A" });
            await writes.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant-a" });

        var direct = await session.LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);
        var viaForTenant = await session.ForTenant("tenant-a")
            .LoadAsync<TenantDoc>(id, TestContext.Current.CancellationToken);

        direct.ShouldNotBeNull();
        viaForTenant.ShouldNotBeNull();
        viaForTenant.Name.ShouldBe(direct.Name);
    }
}
