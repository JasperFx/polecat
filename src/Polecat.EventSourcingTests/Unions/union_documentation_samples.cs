using Polecat.Linq;

namespace Polecat.Tests.Unions;

// Compile-only samples for the union section of docs/documents/querying/linq/index.md. net11.0 only, like every
// file in this folder. Nothing here runs as a test; the behavior is covered by union_member_linq_tests.cs.
public class union_documentation_samples
{
    public async Task query_union_members(IQuerySession session)
    {
        #region sample_polecat_linq_union_members
        // Which case the union holds
        var paidByCard = await session.Query<UnionOrder>()
            .Where(x => x.Payment.Value is UnionCard)
            .ToListAsync();

        // A member of one case, through a cast or as
        var oneCard = await session.Query<UnionOrder>()
            .Where(x => ((UnionCard)x.Payment.Value!).Last4 == "5678")
            .ToListAsync();

        var bigCash = await session.Query<UnionOrder>()
            .Where(x => (x.Payment.Value as UnionCash)!.Amount > 50m)
            .OrderByDescending(x => ((UnionCash)x.Payment.Value!).Amount)
            .ToListAsync();

        // A default union holds nothing
        var unpaid = await session.Query<UnionOrder>()
            .Where(x => x.Payment.Value == null)
            .ToListAsync();
        #endregion
    }
}
