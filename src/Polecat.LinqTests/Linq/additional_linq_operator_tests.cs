using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     ported: Marten — LINQ operators duplicated from Marten's suite: negation, modulo,
///     Last/LastOrDefault, <c>.Equals()</c>, nullable types, and HasValue.
/// </summary>
/// <remarks>
///     The last remaining duplication of a Marten test after compliance wave 3 (#399), and the one
///     the shared library cannot absorb: LINQ is document-database behavior, and
///     <c>JasperFx.Events.ComplianceTests</c> spans only the event sourcing surface. Polecat still
///     re-declares <c>IDocumentStore</c>/<c>IDocumentSession</c>/<c>IQuerySession</c> in its own
///     namespace, so there is nothing for a document-db compliance suite to be generic over yet;
///     Weasel.Storage is the intended convergence layer. Tracked in marten#5118.
///
///     <para>
///         Marker convention: <c>ported:</c> means this file duplicates a test that also exists in
///         another store and should eventually move into a shared suite. A bare <c>marten#NNNN</c>
///         citation elsewhere in this suite means the opposite — Polecat implemented the same
///         feature and the issue number is provenance worth keeping.
///     </para>
/// </remarks>
public class additional_linq_operator_tests : OneOffConfigurationsContext
{
    private async Task StoreSeedDataAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();

        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Alice", Age = 25, IsActive = true,
            Color = TargetColor.Red, Price = 100m, Score = 9.0, BigNumber = 1,
            NullableNumber = 10, NullableBoolean = true, NullableDateTime = new DateTime(2024, 1, 1)
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Bob", Age = 30, IsActive = false,
            Color = TargetColor.Green, Price = 200m, Score = 8.0, BigNumber = 2,
            NullableNumber = 20, NullableBoolean = false, NullableDateTime = new DateTime(2020, 6, 15)
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Charlie", Age = 35, IsActive = true,
            Color = TargetColor.Blue, Price = 300m, Score = 7.0, BigNumber = 3,
            NullableNumber = null, NullableBoolean = null, NullableDateTime = null
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Diana", Age = 40, IsActive = true,
            Color = TargetColor.Red, Price = 400m, Score = 6.0, BigNumber = 4,
            NullableNumber = 30, NullableBoolean = true, NullableDateTime = new DateTime(2030, 12, 31)
        });
        session.Store(new LinqTarget
        {
            Id = Guid.NewGuid(), Name = "Eve", Age = 45, IsActive = false,
            Color = TargetColor.Green, Price = 500m, Score = 5.0, BigNumber = 5,
            NullableNumber = null, NullableBoolean = null, NullableDateTime = null
        });

        await session.SaveChangesAsync();
    }

    // ===== Negation operator tests (ported from Marten's negation_operator.cs) =====

    [Fact]
    public async Task negating_predicate_with_and_operator()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !(x.Name == "Alice" && x.Age == 25))
            .ToListAsync(TestContext.Current.CancellationToken);

        // All except Alice
        results.Count.ShouldBe(4);
        results.ShouldNotContain(r => r.Name == "Alice");
    }

    [Fact]
    public async Task negating_predicate_with_or_operator()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !(x.Name == "Alice" || x.Name == "Bob"))
            .ToListAsync(TestContext.Current.CancellationToken);

        // Charlie, Diana, Eve
        results.Count.ShouldBe(3);
        results.ShouldNotContain(r => r.Name == "Alice");
        results.ShouldNotContain(r => r.Name == "Bob");
    }

    // ===== Modulo operator tests (ported from Marten's modulo_operator.cs) =====

    [Fact]
    public async Task use_modulo()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        // Even BigNumbers (2, 4) that are also < 5
        var results = await query.Query<LinqTarget>()
            .Where(x => x.BigNumber % 2 == 0 && x.BigNumber < 5)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
        results.Select(r => r.BigNumber).OrderBy(x => x).ShouldBe([2, 4]);
    }

    [Fact]
    public async Task use_modulo_operands_reversed()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        // Reversed form: 0 == x.BigNumber % 2
        var results = await query.Query<LinqTarget>()
            .Where(x => 0 == x.BigNumber % 2)
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(2);
        results.Select(r => r.BigNumber).OrderBy(x => x).ShouldBe([2, 4]);
    }

    // ===== Last/LastOrDefault tests (Polecat supports these unlike Marten) =====

    [Fact]
    public async Task last_async_with_order_by()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .LastAsync(TestContext.Current.CancellationToken);

        // Last by ascending Age = Eve (45)
        result.Name.ShouldBe("Eve");
    }

    [Fact]
    public async Task last_or_default_async_with_no_match()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .Where(x => x.Name == "Nobody")
            .LastOrDefaultAsync(TestContext.Current.CancellationToken);

        result.ShouldBeNull();
    }

    [Fact]
    public async Task last_async_with_predicate()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .LastAsync(x => x.IsActive, TestContext.Current.CancellationToken);

        // Active people ordered by Age: Alice(25), Charlie(35), Diana(40)
        // Last active = Diana
        result.Name.ShouldBe("Diana");
    }

    [Fact]
    public async Task last_or_default_async_with_descending_order()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Query<LinqTarget>()
            .OrderByDescending(x => x.Price)
            .LastOrDefaultAsync(TestContext.Current.CancellationToken);

        // Descending by Price: Eve(500), Diana(400), Charlie(300), Bob(200), Alice(100)
        // Last = Alice (lowest price)
        result.ShouldNotBeNull();
        result.Name.ShouldBe("Alice");
    }

    // ===== .Equals() method tests (ported from Marten's equals_method_usage_validation.cs) =====

    [Fact]
    public async Task equals_method_on_int()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age.Equals(30))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task equals_method_on_guid()
    {
        ConfigureStore(_ => { });

        var targetId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = targetId, Name = "Target", Age = 1 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Other", Age = 2 });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Id.Equals(targetId))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Target");
    }

    [Fact]
    public async Task equals_method_with_and_combination()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Age.Equals(25) && x.IsActive.Equals(true))
            .ToListAsync(TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    // ===== Nullable type tests (ported from Marten's nullable_types.cs) =====

    [Fact]
    public async Task query_against_non_null_nullable()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.NullableNumber > 15)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Bob (20) and Diana (30)
        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task query_nullable_equals_null()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.NullableNumber == null)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Charlie and Eve
        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task query_nullable_not_has_value()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !x.NullableNumber.HasValue)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Charlie and Eve
        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task query_nullable_has_value()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.NullableNumber.HasValue)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Alice (10), Bob (20), Diana (30)
        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task query_nullable_boolean_not_has_value()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !x.NullableBoolean.HasValue)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Charlie and Eve
        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task query_nullable_boolean_not_true()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.NullableBoolean != true)
            .ToListAsync(TestContext.Current.CancellationToken);

        // Bob (false), Charlie (null), Eve (null) — depends on SQL NULL comparison behavior
        // In SQL, NULL != true evaluates to UNKNOWN (excluded from results)
        // So only Bob
        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task query_nullable_datetime_has_value_with_or()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => !x.NullableDateTime.HasValue || x.NullableDateTime > new DateTime(2025, 1, 1))
            .ToListAsync(TestContext.Current.CancellationToken);

        // No value: Charlie, Eve. After 2025: Diana (2030-12-31) = 3
        results.Count.ShouldBe(3);
    }

    [Fact]
    public async Task count_nullable_not_has_value()
    {
        await StoreSeedDataAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Query<LinqTarget>()
            .Where(x => !x.NullableBoolean.HasValue)
            .CountAsync(TestContext.Current.CancellationToken);

        count.ShouldBe(2);
    }
}
