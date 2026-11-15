using Bobcat;
using JasperFx;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Linq;

namespace Polecat.Specs;

/// <summary>
///     #808: LINQ over a union-typed document member. The compiler refuses a union pattern in an
///     expression tree, so every query shape goes through the union's <c>Value</c>.
/// </summary>
[BobcatFeature("Querying union-typed members")]
public class querying_union_members(SpecHost app) : PolecatSpec(app)
{
    private IReadOnlyList<string> _customers = [];

    private async Task GivenOrdersAsync()
    {
        await GivenReadModel(new Order { Id = Guid.NewGuid(), Customer = "ann", Payment = new Payment(new Card("4242")) });
        await GivenReadModel(new Order { Id = Guid.NewGuid(), Customer = "bea", Payment = new Payment(new Cash(20m)) });
        await GivenReadModel(new Order { Id = Guid.NewGuid(), Customer = "cal", Payment = new Payment(new Card("1881")) });
    }

    [Fact]
    public async Task a_type_test_finds_the_documents_holding_that_case()
    {
        await GivenOrdersAsync();

        await WhenActing("querying for orders paid by card", async services =>
        {
            var session = services.GetRequiredService<IQuerySession>();
            _customers = await session.Query<Order>()
                .Where(x => x.Payment.Value is Card)
                .OrderBy(x => x.Customer)
                .Select(x => x.Customer)
                .ToListAsync();
        });

        ThenTheActSucceeded();
        ThenMatches(_customers, new List<string> { "ann", "cal" });
    }

    [Fact]
    public async Task a_case_member_is_only_read_while_the_union_holds_that_case()
    {
        await GivenOrdersAsync();

        await WhenActing("querying for orders whose card ends 1881", async services =>
        {
            var session = services.GetRequiredService<IQuerySession>();
            _customers = await session.Query<Order>()
                .Where(x => ((Card)x.Payment.Value!).Last4 == "1881")
                .Select(x => x.Customer)
                .ToListAsync();
        });

        ThenMatches(_customers, new List<string> { "cal" });
    }

    [Fact]
    public async Task ordering_by_the_union_itself_is_refused()
    {
        await GivenOrdersAsync();

        await WhenActing("ordering orders by their payment union", async services =>
        {
            var session = services.GetRequiredService<IQuerySession>();
            await session.Query<Order>().OrderBy(x => x.Payment).ToListAsync();
        });

        ThenRefusedWith<BadLinqExpressionException>();
    }
}
