using Bobcat;
using Microsoft.Extensions.DependencyInjection;

namespace Polecat.Specs;

/// <summary>
///     #808 / jasperfx#1053: a union appended as an event is stored as its active case, and a
///     self-aggregating type applies it through one <c>Apply(TUnion)</c>.
/// </summary>
[BobcatFeature("Union events")]
public class union_events(SpecHost app) : PolecatSpec(app)
{
    [Fact]
    public async Task an_appended_union_is_stored_as_the_case_it_holds()
    {
        var account = Guid.NewGuid();
        await GivenEvents<Account>(account, new AccountOpened("bob"));

        await WhenActing("a debit is appended as an AccountActivity union", async services =>
        {
            await using var session = services.GetRequiredService<IDocumentStore>().LightweightSession();
            session.Events.Append(account, new AccountActivity(new AccountDebited(3m, "fee")));
            await session.SaveChangesAsync();
        });

        ThenEvents(new AccountDebited(3m, "fee"));
    }

    [Fact]
    public async Task an_aggregate_applies_each_case_of_the_union()
    {
        var account = Guid.NewGuid();
        await GivenEvents<Account>(account,
            new AccountOpened("bob"),
            new AccountActivity(new AccountCredited(10m)),
            new AccountActivity(new AccountDebited(3m, "fee")),
            new AccountActivity(new AccountCredited(5m)));

        await ThenAggregate<Account>(account, Specify<Account>()
            .With(x => x.Owner, "bob")
            .With(x => x.Balance, 12m)
            .With(x => x.Debits, 1));
    }

    [Fact]
    public async Task a_union_holding_no_case_is_refused()
    {
        await WhenActing("a stream is started with a default AccountActivity", async services =>
        {
            await using var session = services.GetRequiredService<IDocumentStore>().LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new AccountActivity());
            await session.SaveChangesAsync();
        });

        ThenRefusedWith<InvalidOperationException>();
        ThenNoEvents();
    }
}
