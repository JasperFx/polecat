using JasperFx;
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
///         #944 (Polecat 6): Polecat throws <c>JasperFx.BadLinqExpressionException</c> itself. The
///         <c>Polecat.Linq.BadLinqExpressionException</c> subclass it used to throw is gone, so a
///         library written against the shared document contracts and expected to run on Polecat or
///         Fisher catches "I cannot answer this correctly" with no per-store catch block.
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

        // Exactly the shared type, not a Polecat subclass of it.
        ex.ShouldBeOfType<JasperFx.BadLinqExpressionException>();
    }
}
