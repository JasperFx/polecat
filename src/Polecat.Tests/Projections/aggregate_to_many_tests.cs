using JasperFx.Events;
using JasperFx.Events.Grouping;
using JasperFx.Events.Projections;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

#region sample_aggregate_to_many_projection

public record MoneyDeposited(Guid AccountId, int Amount);
public record AccountFrozen(Guid AccountId);

public class Balance
{
    public Guid Id { get; set; }
    public int Amount { get; set; }
}

public partial class BalanceProjection : MultiStreamProjection<Balance, Guid>
{
    public BalanceProjection()
    {
        Identity<MoneyDeposited>(e => e.AccountId);
        Identity<AccountFrozen>(e => e.AccountId);
    }

    public void Apply(MoneyDeposited e, Balance b) => b.Amount += e.Amount;

    public bool ShouldDelete(AccountFrozen e) => true;
}

#endregion

public record LoyaltyEarned(Guid CardId, int Points);

public class CardOwner
{
    public Guid Id { get; set; } // card id
    public Guid MemberId { get; set; }
}

public class MemberLoyalty
{
    public Guid Id { get; set; }
    public int Points { get; set; }
}

public partial class MemberLoyaltyProjection : MultiStreamProjection<MemberLoyalty, Guid>
{
    public MemberLoyaltyProjection()
    {
        CustomGrouping(new Grouper());
    }

    public void Apply(LoyaltyEarned e, MemberLoyalty agg) => agg.Points += e.Points;

    public class Grouper : IJasperFxAggregateGrouper<Guid, IQuerySession>
    {
        public async Task Group(IQuerySession session, IReadOnlyList<IEvent> events, IEventGrouping<Guid> grouping)
        {
            var earned = events.OfType<IEvent<LoyaltyEarned>>().ToList();
            if (earned.Count == 0) return;

            var cardIds = earned.Select(e => e.Data.CardId).Distinct().ToArray();
            var owners = await session.Query<CardOwner>().Where(x => cardIds.Contains(x.Id)).ToListAsync();
            var map = owners.ToDictionary(x => x.Id, x => x.MemberId);

            foreach (var e in earned)
            {
                if (map.TryGetValue(e.Data.CardId, out var member))
                {
                    grouping.AddEvent(member, e);
                }
            }
        }
    }
}

// #364 (marten#4998 parity): AggregateToManyAsync runs an event query through the multi-stream
// projection registered for T — the projection's real slicer/grouper + enrichment + per-slice build
// against the live session — and returns one aggregate per resulting identity.
public class aggregate_to_many_tests : OneOffConfigurationsContext
{
    private async Task<DocumentStore> CreateStore()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add(new BalanceProjection(), ProjectionLifecycle.Async);
            opts.Projections.Add(new MemberLoyaltyProjection(), ProjectionLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        return theStore;
    }

    [Fact]
    public async Task fans_a_cross_stream_query_out_to_one_aggregate_per_identity()
    {
        var store = await CreateStore();

        var acctA = Guid.NewGuid();
        var acctB = Guid.NewGuid();
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session = store.LightweightSession();

        // acctA deposited across two streams; acctB in one — the fan-out keys on AccountId, not stream.
        session.Events.Append(stream1, new MoneyDeposited(acctA, 100), new MoneyDeposited(acctB, 50));
        session.Events.Append(stream2, new MoneyDeposited(acctA, 25));
        await session.SaveChangesAsync();

        #region sample_aggregate_to_many

        var aggregates = await session.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == stream1 || e.StreamId == stream2)
            .AggregateToManyAsync<Balance>();

        #endregion

        aggregates.Count.ShouldBe(2);

        // Identity is stamped on each aggregate...
        aggregates.Single(x => x.Id == acctA).Amount.ShouldBe(125);
        aggregates.Single(x => x.Id == acctB).Amount.ShouldBe(50);
    }

    [Fact]
    public async Task excludes_aggregates_that_should_delete()
    {
        var store = await CreateStore();

        var acctA = Guid.NewGuid();
        var acctB = Guid.NewGuid();
        var stream = Guid.NewGuid();

        await using var session = store.LightweightSession();

        session.Events.Append(stream,
            new MoneyDeposited(acctA, 100),
            new MoneyDeposited(acctB, 200),
            new AccountFrozen(acctB));
        await session.SaveChangesAsync();

        var aggregates = await session.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == stream)
            .AggregateToManyAsync<Balance>();

        // acctB is frozen (ShouldDelete) and so is absent; only acctA survives.
        aggregates.Count.ShouldBe(1);
        aggregates.Single().Id.ShouldBe(acctA);
        aggregates.Single().Amount.ShouldBe(100);
    }

    [Fact]
    public async Task enrichment_reads_reference_data_from_the_live_session()
    {
        var store = await CreateStore();

        var cardA = Guid.NewGuid();
        var cardB = Guid.NewGuid();
        var cardC = Guid.NewGuid();
        var memberX = Guid.NewGuid();
        var memberY = Guid.NewGuid();

        await using var session = store.LightweightSession();

        // Present-day reference data the grouper reads during enrichment.
        session.Store(new CardOwner { Id = cardA, MemberId = memberX });
        session.Store(new CardOwner { Id = cardB, MemberId = memberX });
        session.Store(new CardOwner { Id = cardC, MemberId = memberY });
        await session.SaveChangesAsync();

        var stream = Guid.NewGuid();
        session.Events.Append(stream,
            new LoyaltyEarned(cardA, 10),
            new LoyaltyEarned(cardB, 5),
            new LoyaltyEarned(cardC, 20));
        await session.SaveChangesAsync();

        var aggregates = await session.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == stream)
            .AggregateToManyAsync<MemberLoyalty>();

        // Member-keyed, not card-keyed — only possible because the grouper read CardOwner from the session.
        aggregates.Count.ShouldBe(2);
        aggregates.Single(x => x.Id == memberX).Points.ShouldBe(15);
        aggregates.Single(x => x.Id == memberY).Points.ShouldBe(20);
    }

    [Fact]
    public async Task empty_query_returns_empty_list()
    {
        var store = await CreateStore();

        await using var session = store.LightweightSession();

        var aggregates = await session.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == Guid.NewGuid()) // matches nothing
            .AggregateToManyAsync<Balance>();

        aggregates.ShouldBeEmpty();
    }

    [Fact]
    public async Task throws_when_no_projection_produces_the_aggregate_type()
    {
        var store = await CreateStore();

        await using var session = store.LightweightSession();

        await Should.ThrowAsync<ArgumentException>(async () =>
        {
            await session.Events.QueryAllRawEvents().AggregateToManyAsync<UnrelatedAggregate>();
        });
    }

    public class UnrelatedAggregate
    {
        public Guid Id { get; set; }
    }
}
