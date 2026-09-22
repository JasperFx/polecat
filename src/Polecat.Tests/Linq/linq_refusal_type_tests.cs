using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #656. A query Polecat cannot translate is refused with
///     <see cref="BadLinqExpressionException" /> — which derives from the shared
///     <see cref="JasperFx.BadLinqExpressionException" /> — and not with a bare
///     <c>NotSupportedException</c> that a caller cannot tell apart from every other one in .NET.
///     The refusal is a correctness guarantee, so the type has to be catchable and the message has to
///     say what IS accepted.
/// </summary>
public class linq_refusal_type_tests : OneOffConfigurationsContext
{
    private async Task<IQuerySession> aSeededSessionAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alice", Age = 25 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        return theStore.QuerySession();
    }

    [Fact]
    public async Task an_untranslatable_method_call_in_where_names_the_calls_that_do_translate()
    {
        await using var query = await aSeededSessionAsync();

        // A bool-returning call with no parser, so it reaches the method-call refusal itself rather
        // than the comparison one.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<LinqTarget>()
                .Where(x => x.Numbers.SequenceEqual(new[] { 1, 2 }))
                .ToListAsync(TestContext.Current.CancellationToken));

        // Catchable by store-agnostic code, which is the point of the jasperfx#795 lift.
        ex.ShouldBeAssignableTo<JasperFx.BadLinqExpressionException>();

        ex.Message.ShouldContain("SequenceEqual");
        ex.Message.ShouldContain("StartsWith");
        ex.Message.ShouldContain("AdvancedSql");
    }

    [Fact]
    public async Task an_untranslatable_predicate_shape_names_the_shapes_that_do_translate()
    {
        await using var query = await aSeededSessionAsync();

        // A predicate whose body is neither a comparison, a boolean member, nor a supported call.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<LinqTarget>()
                .Where(x => (x.Age > 20 ? x.IsActive : !x.IsActive))
                .ToListAsync(TestContext.Current.CancellationToken));

        ex.ShouldBeAssignableTo<JasperFx.BadLinqExpressionException>();
        ex.Message.ShouldContain("Polecat cannot translate");
        ex.Message.ShouldContain("AdvancedSql");
    }

    [Fact]
    public async Task an_untranslatable_group_by_key_names_the_accepted_forms()
    {
        await using var query = await aSeededSessionAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<LinqTarget>()
                .GroupBy(x => x.Age * 2)
                .Select(g => new { g.Key, Count = g.Count() })
                .ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("GroupBy key");
        ex.Message.ShouldContain("composite key");
    }

    /// <summary>
    ///     #663. An operator with no case in the parser's switch used to fall through and be silently
    ///     ignored, which is worse than a refusal: every one of these returned rows that looked like
    ///     an answer. Measured against three rows before the fix — <c>Order()</c> came back
    ///     unordered, <c>OrderBy(...).Reverse()</c> came back ascending, <c>SkipWhile</c> returned
    ///     all three, and <c>Except(itself)</c> returned all three where it should have returned
    ///     none.
    /// </summary>
    [Theory]
    [InlineData("Order")]
    [InlineData("OrderDescending")]
    [InlineData("Reverse")]
    [InlineData("TakeWhile")]
    [InlineData("SkipWhile")]
    [InlineData("Concat")]
    [InlineData("Union")]
    [InlineData("Except")]
    [InlineData("DefaultIfEmpty")]
    [InlineData("Join")]
    public async Task an_untranslatable_operator_is_refused_rather_than_ignored(string op)
    {
        await using var query = await aSeededSessionAsync();
        var token = TestContext.Current.CancellationToken;
        var source = query.Query<LinqTarget>();

        var chained = op switch
        {
            "Order" => source.Order(),
            "OrderDescending" => source.OrderDescending(),
            "Reverse" => source.OrderBy(x => x.Age).Reverse(),
            "TakeWhile" => source.TakeWhile(x => x.Age < 3),
            "SkipWhile" => source.SkipWhile(x => x.Age < 3),
            "Concat" => source.Concat(query.Query<LinqTarget>()),
            "Union" => source.Union(query.Query<LinqTarget>()),
            "Except" => source.Except(query.Query<LinqTarget>()),
            "DefaultIfEmpty" => source.DefaultIfEmpty()!,
            "Join" => source.Join(query.Query<LinqTarget>(), x => x.Id, y => y.Id, (x, y) => x),
            _ => throw new ArgumentOutOfRangeException(nameof(op))
        };

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => chained.ToListAsync(token));

        ex.Message.ShouldContain($"'{op}'");
        ex.Message.ShouldContain("refused rather than ignored");

        // The message lists what DOES translate, so the caller is not sent to the source.
        ex.Message.ShouldContain("OrderByDescending");
        ex.Message.ShouldContain("GroupJoin(...).SelectMany(...)");
        ex.Message.ShouldContain("AdvancedSql");
    }

    /// <summary>
    ///     The other side of the same guard: an operator that really is a no-op on a Polecat query
    ///     must keep working, or the refusal breaks queries that were correct.
    /// </summary>
    [Fact]
    public async Task an_operator_that_genuinely_does_nothing_is_still_allowed()
    {
        await using var query = await aSeededSessionAsync();
        var token = TestContext.Current.CancellationToken;

        (await query.Query<LinqTarget>().AsQueryable().ToListAsync(token)).ShouldNotBeEmpty();
        (await query.Query<LinqTarget>().Cast<LinqTarget>().ToListAsync(token)).ShouldNotBeEmpty();
        (await query.Query<LinqTarget>().OfType<LinqTarget>().ToListAsync(token)).ShouldNotBeEmpty();
    }

    /// <summary>
    ///     A NARROWING OfType is a filter, not a no-op, and Polecat has no OfType translation at all
    ///     — document subclasses are queried through <c>Query&lt;TSubClass&gt;()</c>. Ignoring it
    ///     returns the parent's rows dressed as a subclass query.
    /// </summary>
    [Fact]
    public async Task a_narrowing_of_type_is_refused()
    {
        await using var query = await aSeededSessionAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<LinqTarget>().OfType<LinqTargetSpecial>()
                .ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("'OfType'");
    }

    /// <summary>
    ///     The deliberate exclusion. Synchronous execution is an unsupported API rather than an
    ///     untranslatable expression — the query may well be translatable, and there is no correctness
    ///     claim being made about it — so it stays a NotSupportedException.
    /// </summary>
    [Fact]
    public async Task synchronous_enumeration_is_still_a_plain_not_supported_exception()
    {
        await using var query = await aSeededSessionAsync();

        var ex = Should.Throw<NotSupportedException>(() =>
            query.Query<LinqTarget>().Where(x => x.Age > 1).ToList());

        ex.ShouldNotBeAssignableTo<JasperFx.BadLinqExpressionException>();
    }

    /// <summary>
    ///     So is a marker method invoked in memory: <c>PlainTextSearch</c> exists for the expression
    ///     tree to carry, and calling it directly is a different mistake from writing a query that
    ///     cannot be translated.
    /// </summary>
    [Fact]
    public void a_marker_method_called_in_memory_is_still_a_plain_not_supported_exception()
    {
        var ex = Should.Throw<NotSupportedException>(() => "text".PlainTextSearch("term"));

        ex.ShouldNotBeAssignableTo<JasperFx.BadLinqExpressionException>();
    }
}
