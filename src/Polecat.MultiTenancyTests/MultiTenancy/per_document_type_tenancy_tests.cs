using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

#region document types

/// <summary>A document type declared conjoined on its own, in a store whose events are not.</summary>
public class PerTypeTenantedInvoice
{
    public Guid Id { get; set; }
    public string Customer { get; set; } = "";
    public decimal Amount { get; set; }
}

/// <summary>Its single-tenanted neighbour, which is the whole point: the two coexist.</summary>
public class PerTypeSharedCurrency
{
    public Guid Id { get; set; }
    public string Code { get; set; } = "";
    public string Customer { get; set; } = "";
}

#endregion

/// <summary>
///     #682 / jasperfx#898 — <c>Schema.For&lt;T&gt;().MultiTenanted()</c> and its policy spellings.
///     Document tenancy used to come off <c>Events.TenancyStyle</c> store-wide, so none of the
///     arrangements below could be expressed at all.
/// </summary>
/// <remarks>
///     Every test here builds a store whose <b>event</b> tenancy is single, which is the arrangement
///     that was impossible before and the one that found the real defect: the LINQ provider decided
///     tenancy from <c>Events.TenancyStyle</c> while the keyed load path decided it from the document's
///     mapping. A conjoined document in a single-tenanted store therefore had its <c>tenant_id</c>
///     column and its composite key, loaded correctly by id, and leaked every tenant's rows through
///     every LINQ shape. Tests that put conjoined documents in a conjoined store — which is all the
///     older ones — cannot see that, because there the two sources agree.
/// </remarks>
public class per_document_type_tenancy_tests : OneOffConfigurationsContext
{
    private static readonly Guid SharedId = Guid.NewGuid();

    private async Task SeedBothTenantsAsync(CancellationToken token)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Store(new PerTypeTenantedInvoice { Id = SharedId, Customer = "Red", Amount = 1 });
            red.Store(new PerTypeTenantedInvoice { Id = Guid.NewGuid(), Customer = "Red", Amount = 2 });
            await red.SaveChangesAsync(token);
        }

        await using var blue = theStore.LightweightSession("Blue");
        blue.Store(new PerTypeTenantedInvoice { Id = SharedId, Customer = "Blue", Amount = 10 });
        await blue.SaveChangesAsync(token);
    }

    [Fact]
    public async Task schema_for_multi_tenanted_conjoins_that_type_in_a_single_tenanted_store()
    {
        ConfigureStore(opts => opts.Schema.For<PerTypeTenantedInvoice>().MultiTenanted());

        var token = TestContext.Current.CancellationToken;
        await SeedBothTenantsAsync(token);

        // The keyed load path, which was always mapping-driven.
        await using (var red = theStore.QuerySession("Red"))
        {
            (await red.LoadAsync<PerTypeTenantedInvoice>(SharedId, token))
                .ShouldNotBeNull().Customer.ShouldBe("Red");

            // ...and the LINQ shapes, which were not. Red holds two rows, Blue one, so a leak shows up
            // as a count in one direction and a wrong value in the other.
            (await red.Query<PerTypeTenantedInvoice>().CountAsync(token)).ShouldBe(2);
            (await red.Query<PerTypeTenantedInvoice>().ToListAsync(token))
                .ShouldAllBe(x => x.Customer == "Red");
        }

        await using var blue = theStore.QuerySession("Blue");

        (await blue.LoadAsync<PerTypeTenantedInvoice>(SharedId, token))
            .ShouldNotBeNull().Customer.ShouldBe("Blue");
        (await blue.Query<PerTypeTenantedInvoice>().CountAsync(token)).ShouldBe(1);
        (await blue.Query<PerTypeTenantedInvoice>().AnyAsync(x => x.Amount == 1, token)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_type_that_did_not_opt_in_stays_single_tenanted_beside_one_that_did()
    {
        ConfigureStore(opts => opts.Schema.For<PerTypeTenantedInvoice>().MultiTenanted());

        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        theStore.Options.TenancyStyleFor(typeof(PerTypeTenantedInvoice)).ShouldBe(TenancyStyle.Conjoined);
        theStore.Options.TenancyStyleFor(typeof(PerTypeSharedCurrency)).ShouldBe(TenancyStyle.Single);

        // One row, written under one tenant, is visible from the other — a single-tenanted table has no
        // tenant_id column, so there is nothing to scope and nothing is scoped.
        var id = Guid.NewGuid();
        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Store(new PerTypeSharedCurrency { Id = id, Code = "USD", Customer = "Red" });
            await red.SaveChangesAsync(token);
        }

        await using var blue = theStore.QuerySession("Blue");
        (await blue.LoadAsync<PerTypeSharedCurrency>(id, token)).ShouldNotBeNull().Code.ShouldBe("USD");
        (await blue.Query<PerTypeSharedCurrency>().CountAsync(token)).ShouldBe(1);
    }

    /// <summary>
    ///     A join whose two sides disagree about tenancy — the arrangement the per-type opt-in creates
    ///     and the one that decides how the join's tenant filters have to be built.
    /// </summary>
    /// <remarks>
    ///     Only the conjoined side may be filtered. Emitting <c>inner_t.tenant_id = @t</c> against a
    ///     single-tenanted table names a column that is not there, so the query fails outright rather
    ///     than over- or under-scoping; the opposite mistake — gating both sides on the outer's style —
    ///     leaves the conjoined side unfiltered. Both are only reachable once the two can differ.
    /// </remarks>
    [Fact]
    public async Task a_join_scopes_the_conjoined_side_and_leaves_the_single_tenanted_side_alone()
    {
        ConfigureStore(opts => opts.Schema.For<PerTypeTenantedInvoice>().MultiTenanted());

        var token = TestContext.Current.CancellationToken;
        await SeedBothTenantsAsync(token);

        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(new PerTypeSharedCurrency { Id = Guid.NewGuid(), Code = "USD", Customer = "Red" });
            seed.Store(new PerTypeSharedCurrency { Id = Guid.NewGuid(), Code = "EUR", Customer = "Blue" });
            await seed.SaveChangesAsync(token);
        }

        await using var red = theStore.QuerySession("Red");

        var joined = await red.Query<PerTypeTenantedInvoice>()
            .GroupJoin(red.Query<PerTypeSharedCurrency>(),
                i => i.Customer, c => c.Customer,
                (i, currencies) => new { i, currencies })
            .SelectMany(x => x.currencies, (x, c) => new { x.i.Amount, c.Code })
            .ToListAsync(token);

        // Red's two invoices join to the one USD row. Blue's invoice must not appear, and the EUR row
        // must remain reachable to whoever joins to it — it is not tenanted at all.
        joined.Count.ShouldBe(2);
        joined.ShouldAllBe(x => x.Code == "USD");
        joined.Select(x => x.Amount).OrderBy(x => x).ShouldBe([1m, 2m]);
    }

    [Fact]
    public async Task the_policy_spellings_reach_the_same_decision()
    {
        ConfigureStore(opts =>
        {
            opts.Policies.ForDocument<PerTypeTenantedInvoice>(p => p.MultiTenanted = true);
        });

        theStore.Options.TenancyStyleFor(typeof(PerTypeTenantedInvoice)).ShouldBe(TenancyStyle.Conjoined);
        theStore.Options.TenancyStyleFor(typeof(PerTypeSharedCurrency)).ShouldBe(TenancyStyle.Single);

        ConfigureStore(opts => opts.Policies.AllDocumentsAreMultiTenanted());

        theStore.Options.TenancyStyleFor(typeof(PerTypeTenantedInvoice)).ShouldBe(TenancyStyle.Conjoined);
        theStore.Options.TenancyStyleFor(typeof(PerTypeSharedCurrency)).ShouldBe(TenancyStyle.Conjoined);

        // And the fallback is unchanged: a conjoined EVENT store still conjoins every document, which
        // is how this worked before there was anything to opt into.
        ConfigureStore(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        theStore.Options.TenancyStyleFor(typeof(PerTypeSharedCurrency)).ShouldBe(TenancyStyle.Conjoined);

        await Task.CompletedTask;
    }
}
