using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

/// <summary>A conjoined document with no other purpose than being read every way there is.</summary>
public class SweptLedgerEntry
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = "";
    public string Category { get; set; } = "";
    public int Amount { get; set; }
}

/// <summary>Its single-tenanted neighbour, for the join arm.</summary>
public class SweptCategory
{
    public Guid Id { get; set; }
    public string Category { get; set; } = "";
    public string Label { get; set; } = "";
}

/// <summary>
///     #681 — the per-shape tenancy sweep for documents. Every statement shape a conjoined document can
///     be read through, asserted <b>in both directions</b> and over <b>one id shared between two
///     tenants</b>.
/// </summary>
/// <remarks>
///     <para>
///         Both of those are the wave-19 lesson. A one-directional assertion ("Red sees its own row")
///         passes on a completely unscoped query, and distinct ids per tenant hide a composite-key
///         mistake entirely — the shared id is what makes the scoping observable.
///     </para>
///     <para>
///         The shape dimension is what is new here. <c>DocumentConjoinedTenancyCompliance</c> covers a
///         closed minimum set across stores; a leak, when it happens, lives in ONE statement builder —
///         #686's independently-gated join sides are the live example — so the sweep has to name the
///         shapes rather than trust that scoping the list shape scoped the rest. Every arm below runs
///         with <b>no <c>Where</c></b>, because a <c>Where</c> can mask a missing tenant filter by
///         happening to exclude the other tenant's rows.
///     </para>
///     <para>
///         There is no <c>Include()</c> operator in Polecat, so the join arm is the whole of this
///         issue's "Include/join" bullet.
///     </para>
/// </remarks>
public class conjoined_document_read_sweep_tests : OneOffConfigurationsContext
{
    // One id, two tenants. Red holds this plus two more; Blue holds only this.
    private static readonly Guid SharedId = Guid.NewGuid();
    private static readonly Guid RedOnlyId = Guid.NewGuid();
    private static readonly Guid RedOtherId = Guid.NewGuid();

    private async Task SeedAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<SweptLedgerEntry>().MultiTenanted();
            opts.Schema.For<SweptCategory>();
        });

        var token = TestContext.Current.CancellationToken;
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await using (var red = theStore.LightweightSession("Red"))
        {
            red.Store(new SweptLedgerEntry { Id = SharedId, Owner = "Red", Category = "food", Amount = 1 });
            red.Store(new SweptLedgerEntry { Id = RedOnlyId, Owner = "Red", Category = "food", Amount = 2 });
            red.Store(new SweptLedgerEntry { Id = RedOtherId, Owner = "Red", Category = "rent", Amount = 4 });
            await red.SaveChangesAsync(token);
        }

        await using (var blue = theStore.LightweightSession("Blue"))
        {
            // Same id, different tenant, and an Amount chosen so every aggregate below differs between
            // the two tenants AND differs from the store-wide answer.
            blue.Store(new SweptLedgerEntry { Id = SharedId, Owner = "Blue", Category = "food", Amount = 100 });
            await blue.SaveChangesAsync(token);
        }

        await using var shared = theStore.LightweightSession();
        shared.Store(new SweptCategory { Id = Guid.NewGuid(), Category = "food", Label = "Groceries" });
        shared.Store(new SweptCategory { Id = Guid.NewGuid(), Category = "rent", Label = "Housing" });
        await shared.SaveChangesAsync(token);
    }

    [Fact]
    public async Task the_list_shape_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        (await red.Query<SweptLedgerEntry>().ToListAsync(token)).ShouldAllBe(x => x.Owner == "Red");
        (await blue.Query<SweptLedgerEntry>().ToListAsync(token)).ShouldAllBe(x => x.Owner == "Blue");
    }

    [Fact]
    public async Task the_counting_shapes_are_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        (await red.Query<SweptLedgerEntry>().CountAsync(token)).ShouldBe(3);
        (await blue.Query<SweptLedgerEntry>().CountAsync(token)).ShouldBe(1);

        (await red.Query<SweptLedgerEntry>().LongCountAsync(token)).ShouldBe(3L);
        (await blue.Query<SweptLedgerEntry>().LongCountAsync(token)).ShouldBe(1L);
    }

    [Fact]
    public async Task the_any_shape_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        // Predicate-less Any is true for both — the interesting asymmetry is a value only the OTHER
        // tenant holds, which must not be visible.
        (await red.Query<SweptLedgerEntry>().AnyAsync(token)).ShouldBeTrue();
        (await blue.Query<SweptLedgerEntry>().AnyAsync(token)).ShouldBeTrue();

        (await red.Query<SweptLedgerEntry>().AnyAsync(x => x.Amount == 100, token)).ShouldBeFalse();
        (await blue.Query<SweptLedgerEntry>().AnyAsync(x => x.Amount == 4, token)).ShouldBeFalse();
    }

    [Fact]
    public async Task the_aggregate_shapes_are_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        // Store-wide answers would be Sum 107, Min 1, Max 100 — none of which either tenant may see.
        (await red.Query<SweptLedgerEntry>().SumAsync(x => x.Amount, token)).ShouldBe(7);
        (await blue.Query<SweptLedgerEntry>().SumAsync(x => x.Amount, token)).ShouldBe(100);

        (await red.Query<SweptLedgerEntry>().MaxAsync(x => x.Amount, token)).ShouldBe(4);
        (await blue.Query<SweptLedgerEntry>().MaxAsync(x => x.Amount, token)).ShouldBe(100);

        (await red.Query<SweptLedgerEntry>().MinAsync(x => x.Amount, token)).ShouldBe(1);
        (await blue.Query<SweptLedgerEntry>().MinAsync(x => x.Amount, token)).ShouldBe(100);

        (await red.Query<SweptLedgerEntry>().AverageAsync(x => x.Amount, token)).ShouldBe(7d / 3, 0.0001);
        (await blue.Query<SweptLedgerEntry>().AverageAsync(x => x.Amount, token)).ShouldBe(100d, 0.0001);
    }

    [Fact]
    public async Task the_group_by_shape_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        var redGroups = await red.Query<SweptLedgerEntry>()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Amount) })
            .ToListAsync(token);

        redGroups.OrderBy(x => x.Category).Select(x => (x.Category, x.Total))
            .ShouldBe([("food", 3), ("rent", 4)]);

        // Blue's only row is in "food", and its total must not include Red's food rows.
        var blueGroups = await blue.Query<SweptLedgerEntry>()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Amount) })
            .ToListAsync(token);

        blueGroups.Select(x => (x.Category, x.Total)).ShouldBe([("food", 100)]);
    }

    [Fact]
    public async Task the_distinct_shape_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        (await red.Query<SweptLedgerEntry>().Select(x => x.Category).Distinct().ToListAsync(token))
            .OrderBy(x => x).ShouldBe(["food", "rent"]);

        // "rent" is Red's alone. A leak here shows as an extra distinct value rather than a wrong count.
        (await blue.Query<SweptLedgerEntry>().Select(x => x.Category).Distinct().ToListAsync(token))
            .ShouldBe(["food"]);
    }

    [Fact]
    public async Task the_select_projection_shape_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        (await red.Query<SweptLedgerEntry>().Select(x => x.Amount).ToListAsync(token))
            .OrderBy(x => x).ShouldBe([1, 2, 4]);
        (await blue.Query<SweptLedgerEntry>().Select(x => x.Amount).ToListAsync(token))
            .ShouldBe([100]);
    }

    [Fact]
    public async Task load_many_is_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        // Both tenants are asked for the SAME three ids. Red holds all three; Blue holds one of them
        // under the shared id and must not be handed Red's other two.
        var ids = new[] { SharedId, RedOnlyId, RedOtherId };

        var redLoaded = await red.LoadManyAsync<SweptLedgerEntry>(ids, token);
        redLoaded.Count.ShouldBe(3);
        redLoaded.ShouldAllBe(x => x.Owner == "Red");

        var blueLoaded = await blue.LoadManyAsync<SweptLedgerEntry>(ids, token);
        blueLoaded.Count.ShouldBe(1);
        blueLoaded.Single().Owner.ShouldBe("Blue");
        blueLoaded.Single().Id.ShouldBe(SharedId);
    }

    [Fact]
    public async Task the_batched_shapes_are_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        var redBatch = red.CreateBatchQuery();
        var redLoad = redBatch.Load<SweptLedgerEntry>(SharedId);
        var redList = redBatch.Query<SweptLedgerEntry>().ToList();
        var redCount = redBatch.Query<SweptLedgerEntry>().Count();
        var redExists = redBatch.CheckExists<SweptLedgerEntry>(RedOtherId);
        await redBatch.Execute(token);

        (await redLoad).ShouldNotBeNull()!.Owner.ShouldBe("Red");
        (await redList).Count.ShouldBe(3);
        (await redCount).ShouldBe(3);
        (await redExists).ShouldBeTrue();

        await using var blue = theStore.QuerySession("Blue");
        var blueBatch = blue.CreateBatchQuery();
        var blueLoad = blueBatch.Load<SweptLedgerEntry>(SharedId);
        var blueList = blueBatch.Query<SweptLedgerEntry>().ToList();
        var blueCount = blueBatch.Query<SweptLedgerEntry>().Count();
        // RedOtherId exists — in the other tenant. The existence check must say no.
        var blueExists = blueBatch.CheckExists<SweptLedgerEntry>(RedOtherId);
        await blueBatch.Execute(token);

        (await blueLoad).ShouldNotBeNull()!.Owner.ShouldBe("Blue");
        (await blueList).Count.ShouldBe(1);
        (await blueCount).ShouldBe(1);
        (await blueExists).ShouldBeFalse();
    }

    [Fact]
    public async Task the_first_and_single_shapes_are_scoped_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");
        await using var blue = theStore.QuerySession("Blue");

        (await blue.Query<SweptLedgerEntry>().SingleAsync(token)).Owner.ShouldBe("Blue");

        // Ordering makes this deterministic without a Where, which is the point of the sweep: Red's
        // lowest Amount is 1 and Blue's is 100, so a leak swaps the answer.
        (await red.Query<SweptLedgerEntry>().OrderBy(x => x.Amount).FirstAsync(token)).Amount.ShouldBe(1);
        (await blue.Query<SweptLedgerEntry>().OrderBy(x => x.Amount).FirstAsync(token)).Amount.ShouldBe(100);
    }

    [Fact]
    public async Task a_join_scopes_only_the_conjoined_side_in_both_directions()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        // #686 gated the two sides of a join on tenancy independently, which is the newest code in this
        // area and had one test behind it. Both directions here, and with no Where on either side.
        await using var red = theStore.QuerySession("Red");
        var redJoined = await red.Query<SweptLedgerEntry>()
            .GroupJoin(red.Query<SweptCategory>(), e => e.Category, c => c.Category,
                (e, cats) => new { e, cats })
            .SelectMany(x => x.cats, (x, c) => new { x.e.Amount, c.Label })
            .ToListAsync(token);

        redJoined.Select(x => x.Amount).OrderBy(x => x).ShouldBe([1, 2, 4]);
        redJoined.Select(x => x.Label).Distinct().OrderBy(x => x).ShouldBe(["Groceries", "Housing"]);

        await using var blue = theStore.QuerySession("Blue");
        var blueJoined = await blue.Query<SweptLedgerEntry>()
            .GroupJoin(blue.Query<SweptCategory>(), e => e.Category, c => c.Category,
                (e, cats) => new { e, cats })
            .SelectMany(x => x.cats, (x, c) => new { x.e.Amount, c.Label })
            .ToListAsync(token);

        // One conjoined row, and the single-tenanted side stays fully reachable — including the
        // "Housing" row that only Red's data joins to.
        blueJoined.Select(x => x.Amount).ShouldBe([100]);
        blueJoined.Single().Label.ShouldBe("Groceries");
    }

    [Fact]
    public async Task raw_sql_is_deliberately_not_tenant_scoped()
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;

        await using var red = theStore.QuerySession("Red");

        // The one read that is NOT scoped, stated rather than left to be discovered: the caller wrote
        // the SQL, so the caller owns the tenant_id filter. Pinned in both shapes — unfiltered sees
        // everything, and the caller's own filter works.
        var table = $"[{theStore.Options.DatabaseSchemaName}].[pc_doc_sweptledgerentry]";

        (await red.AdvancedSql.QueryAsync<int>($"SELECT COUNT(*) FROM {table}", token))
            .Single().ShouldBe(4);

        (await red.AdvancedSql.QueryAsync<int>(
                $"SELECT COUNT(*) FROM {table} WHERE tenant_id = ?", token, "Red"))
            .Single().ShouldBe(3);
    }
}
