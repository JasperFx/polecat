using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Polecat.Linq;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Aot;

/// <summary>
///     #733: the public LINQ async surface declares that it needs dynamic code, instead of
///     suppressing the diagnostic that says so.
/// </summary>
/// <remarks>
///     <para>
///         <b>An annotation is a contract, and nothing else here can hold it.</b> A consumer only
///         sees IL2026 / IL3050 when they are trimming or AOT-publishing, so no ordinary build in
///         this repository exercises it, and the one lane that does
///         (<c>Polecat.AotSmoke</c>) proves the <i>absence</i> of warnings on the clean surface
///         rather than their presence here. If someone re-adds an
///         <see cref="UnconditionalSuppressMessageAttribute" /> to quiet a noisy consumer build,
///         every test in this suite still passes and the lie is back.
///     </para>
///     <para>
///         ⚠️ <b>The negative assertion is the important one.</b> What #733 removed was not a missing
///         annotation — it was a PRESENT suppression claiming the path was safe, justified with "the
///         trimmer preserves those intrinsics", directly contradicting the same class's remarks
///         telling AOT publishers to avoid it. A store that compiles clean under <c>PublishAot</c>
///         and throws on first query is worse than one that refuses to compile.
///     </para>
/// </remarks>
public class aot_annotation_contract_tests
{
    private static readonly Type Extensions = typeof(PolecatQueryableExtensions);

    [Fact]
    public void the_linq_async_surface_requires_dynamic_code()
    {
        Extensions.GetCustomAttribute<RequiresDynamicCodeAttribute>()
            .ShouldNotBeNull("PolecatQueryableExtensions must declare that it needs dynamic code (#733)")
            .Message.ShouldContain("polecat#733");
    }

    [Fact]
    public void the_linq_async_surface_requires_unreferenced_code()
    {
        Extensions.GetCustomAttribute<RequiresUnreferencedCodeAttribute>()
            .ShouldNotBeNull("PolecatQueryableExtensions must declare that it is not trim-safe (#733)")
            .Message.ShouldContain("polecat#733");
    }

    [Fact]
    public void the_linq_async_surface_does_not_suppress_the_diagnostics_it_now_declares()
    {
        // ⚠️ THE fact. A suppression and an annotation on the same member are contradictory, and the
        // suppression wins silently -- which is exactly how #733 stayed invisible. Asserted by code
        // rather than by comment, because the failure mode is someone adding one back in good faith
        // to quiet a downstream build.
        var suppressed = Extensions
            .GetCustomAttributes<UnconditionalSuppressMessageAttribute>()
            .Select(x => x.CheckId)
            .ToArray();

        suppressed.ShouldNotContain(x => x.StartsWith("IL2026"));
        suppressed.ShouldNotContain(x => x.StartsWith("IL3050"));
    }

    [Theory]
    [InlineData(nameof(PolecatQueryableExtensions.ToListAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.FirstAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.FirstOrDefaultAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.SingleAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.CountAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.LongCountAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.AnyAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.SumAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.MinAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.MaxAsync))]
    [InlineData(nameof(PolecatQueryableExtensions.AverageAsync))]
    public void every_terminator_inherits_the_declaration(string name)
    {
        // The attributes sit on the class, which the analyzer applies to every member. Pinned per
        // terminator anyway: a future refactor that moves one of these to another class would
        // silently drop its declaration, and "it was on the class" is not something a consumer's
        // build can check.
        Extensions.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(x => x.Name == name)
            .ShouldNotBeEmpty($"'{name}' is no longer on PolecatQueryableExtensions");

        Extensions.GetCustomAttribute<RequiresDynamicCodeAttribute>().ShouldNotBeNull();
    }

    [Fact]
    public void the_surfaces_that_execute_a_query_through_the_wrappers_declare_it_too()
    {
        // Propagation, asserted where it actually matters: these are public entry points a consumer
        // calls directly, so each has to carry the declaration itself rather than relying on the
        // wrapper it happens to call internally.
        typeof(Polecat.Pagination.PagedListExtensions)
            .GetMethod(nameof(Polecat.Pagination.PagedListExtensions.ToPagedListAsync))
            .ShouldNotBeNull()
            .GetCustomAttribute<RequiresDynamicCodeAttribute>()
            .ShouldNotBeNull("ToPagedListAsync executes a LINQ query (#733)");

        typeof(HybridSearchExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(x => x.Name == nameof(HybridSearchExtensions.HybridSearchAsync))
            .ShouldAllBe(x => x.GetCustomAttribute<RequiresDynamicCodeAttribute>() != null);
    }
}
