using System.Linq.Expressions;
using Polecat.Tests.Harness;
using Xunit;

namespace Polecat.Tests.Storage;

// Compile-only probe: every pre-existing call shape must still bind, and none may become ambiguous
// (CS0121). It asserts nothing at runtime -- the COMPILER is the assertion.
public class overload_binding_probe
{
    private sealed class Doc { public Guid Id { get; set; } public string Body { get; set; } = ""; }

    [Fact]
    public void every_pre_existing_call_shape_still_compiles()
    {
        Func<IQuerySession, Task> _ = async s =>
        {
            Expression<Func<Doc, object?>> member = x => x.Body;
            Expression<Func<Doc, bool>> filter = x => x.Body != "";
            var ct = CancellationToken.None;

            // --- shapes that existed in 5.29.0: must still bind to the ORIGINAL signature ---
            await s.FullTextSearchAsync(member, "fox");
            await s.FullTextSearchAsync(member, "fox", 10);
            await s.FullTextSearchAsync(member, "fox", 10, filter);
            await s.FullTextSearchAsync(member, "fox", 10, filter, ct);
            await s.FullTextSearchAsync(member, "fox", filter: filter);
            await s.FullTextSearchAsync(member, "fox", token: ct);

            await s.FullTextSearchWithScoresAsync(member, "fox");
            await s.FullTextSearchWithScoresAsync(member, "fox", 10, filter, ct);
            await s.FullTextSearchWithScoresAsync(member, "fox", filter: filter);

            // --- the new shapes ---
            await s.FullTextSearchAsync(member, "fox", new FullTextSearchOptions(K1: 1.5));
            await s.FullTextSearchAsync(member, "fox", new FullTextSearchOptions(B: 0.0), 5);
            await s.FullTextSearchAsync(member, "fox", new FullTextSearchOptions(), 5, filter, ct);
            await s.FullTextSearchWithScoresAsync(member, "fox", new FullTextSearchOptions(K1: 2.0));
        };
    }
}
