using Polecat.Linq;
using Polecat.Linq.SoftDeletes;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Linq;

/// <summary>
///     jasperfx#795: a LINQ refusal is catchable without naming Polecat.
/// </summary>
/// <remarks>
///     <para>
///         <c>Polecat.Linq.BadLinqExpressionException</c> derives from
///         <c>JasperFx.BadLinqExpressionException</c>, so store-agnostic code — a library written
///         against the shared document contracts, and expected to run on Polecat or Fisher — can catch
///         "I cannot answer this correctly" without a per-store catch block.
///     </para>
///     <para>
///         ⚠️ <b>The derivation is the whole feature, and nothing else proves it.</b> Polecat's own
///         tests all catch the Polecat type, which passes whether or not the base is there — so
///         dropping the base class would leave every existing LINQ refusal test green and silently
///         break the one thing the lift was for.
///     </para>
///     <para>
///         Marten deliberately stays out of this hierarchy until Marten 10 (marten#5346), so a consumer
///         spanning all three stores still needs to nominate the type. Fisher and Polecat are the
///         convergence.
///     </para>
/// </remarks>
public class store_agnostic_linq_refusal_tests: OneOffConfigurationsContext
{
    public class Note
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    [Fact]
    public async Task a_refusal_is_catchable_as_the_shared_jasperfx_type()
    {
        ConfigureStore(opts => opts.Schema.For<Note>());

        await using var session = theStore.QuerySession();

        // MaybeDeleted() on a type that is not soft-deleted: refused rather than ignored, because
        // ignoring it would return every row.
        var ex = await Should.ThrowAsync<JasperFx.BadLinqExpressionException>(async () =>
            await session.Query<Note>()
                .MaybeDeleted()
                .ToListAsync(TestContext.Current.CancellationToken));

        // Still Polecat's own type, so code already catching that keeps working.
        ex.ShouldBeOfType<BadLinqExpressionException>();
    }
}
