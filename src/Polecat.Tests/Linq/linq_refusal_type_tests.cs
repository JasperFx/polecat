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
        ex.Message.ShouldContain("MatchesSql");
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
        ex.Message.ShouldContain("MatchesSql");
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
