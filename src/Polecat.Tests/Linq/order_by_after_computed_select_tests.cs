using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ProjectedOrderingDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public ProjectedOrderingInner Inner { get; set; } = new();
}

public class ProjectedOrderingInner
{
    public int Level { get; set; }
}

// gh-998, mirroring marten#5955 / marten#5942. A computed Select() is projected in memory, after the rows are
// read, so an OrderBy() over the projected value has no column to order by. Polecat ordered by what the
// statement selected, the document's JSON body: SQL Server 2025's native json refuses to sort it ("The JSON
// data type cannot be compared or sorted"), and json text would have sorted the documents as text. Marten
// computes the projection in SQL; Polecat has no exact translation of a computed projection yet, so it refuses
// with BadLinqExpressionException. ORDER BY data is never emitted.
public class order_by_after_computed_select_tests : OneOffConfigurationsContext
{
    private static readonly string[] Pool = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"];

    private static readonly ProjectedOrderingDoc[] Docs = new[] { 3, 8, 1, 6, 4, 7, 2, 5, 10, 9 }
        .Select(i => new ProjectedOrderingDoc
        {
            Id = Guid.NewGuid(), Number = i, Name = Pool[i % Pool.Length] + i,
            Inner = new ProjectedOrderingInner { Level = 100 - i }
        }).ToArray();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task SeedAsync()
    {
        // One at a time, out of Number order, so an ordering that goes missing shows up
        foreach (var doc in Docs)
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }
    }

    public static TheoryData<string> ComputedShapes =>
    [
        "modulo", "modulo_descending", "concatenation", "id_to_string", "widening_then_multiply", "negation",
        "to_upper", "then_skip_take", "first"
    ];

    [Theory]
    [MemberData(nameof(ComputedShapes))]
    public async Task an_ordering_over_a_computed_projection_is_refused(string shape)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var q = session.Query<ProjectedOrderingDoc>();

        Func<Task> query = shape switch
        {
            "modulo" => () => q.Select(x => x.Number * 7 % 11).OrderBy(n => n).ToListAsync(Ct),
            "modulo_descending" => () => q.Select(x => x.Number % 3).OrderByDescending(n => n).ToListAsync(Ct),
            "concatenation" => () => q.Select(x => x.Name + "z" + x.Number).OrderByDescending(n => n).ToListAsync(Ct),
            "id_to_string" => () => q.Select(x => x.Id.ToString()).OrderBy(s => s).ToListAsync(Ct),
            "widening_then_multiply" => () =>
                q.Select(x => (long)x.Number * 1_000_000_000L).OrderBy(n => n).ToListAsync(Ct),
            "negation" => () => q.Select(x => -x.Number).OrderBy(n => n).ToListAsync(Ct),
            "to_upper" => () => q.Select(x => x.Name.ToUpper()).OrderBy(n => n).ToListAsync(Ct),
            "then_skip_take" => () => q.Select(x => x.Number % 4).OrderBy(n => n).Skip(2).Take(3).ToListAsync(Ct),
            "first" => () => q.Select(x => x.Number * 2).OrderByDescending(n => n).FirstAsync(Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(query);
        ex.Message.ShouldContain("OrderBy", Case.Sensitive, shape);
    }

    [Fact]
    public async Task ordering_the_documents_themselves_is_refused()
    {
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            session.Query<ProjectedOrderingDoc>().OrderBy(x => x).ToListAsync(Ct));
        ex.Message.ShouldContain("OrderBy");
    }

    public static TheoryData<string> MemberShapes =>
    [
        "member", "member_descending", "string_member", "nested_member", "widened_member", "member_paged",
        "member_first"
    ];

    // The projected member is a column, and still orders the rows
    [Theory]
    [MemberData(nameof(MemberShapes))]
    public async Task an_ordering_over_a_projected_member_still_orders(string shape)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var q = session.Query<ProjectedOrderingDoc>();
        var docs = Docs.AsQueryable();

        switch (shape)
        {
            case "member":
                (await q.Select(x => x.Number).OrderBy(n => n).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => x.Number).OrderBy(n => n).ToList());
                break;
            case "member_descending":
                (await q.Select(x => x.Number).OrderByDescending(n => n).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => x.Number).OrderByDescending(n => n).ToList());
                break;
            case "string_member":
                (await q.Select(x => x.Name).OrderByDescending(n => n).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => x.Name).OrderByDescending(n => n).ToList());
                break;
            case "nested_member":
                (await q.Select(x => x.Inner.Level).OrderBy(n => n).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => x.Inner.Level).OrderBy(n => n).ToList());
                break;
            case "widened_member":
                (await q.Select(x => (long)x.Number).OrderBy(n => n).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => (long)x.Number).OrderBy(n => n).ToList());
                break;
            case "member_paged":
                (await q.Select(x => x.Number).OrderBy(n => n).Skip(2).Take(3).ToListAsync(Ct))
                    .ShouldBe(docs.Select(x => x.Number).OrderBy(n => n).Skip(2).Take(3).ToList());
                break;
            case "member_first":
                (await q.Select(x => x.Number).OrderByDescending(n => n).FirstAsync(Ct)).ShouldBe(10);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(shape));
        }
    }
}
