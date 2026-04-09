using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

#region sample_polecat_monthly_account_activity_events

public record AccountOpened(string AccountName);
public record AccountDebited(decimal Amount, string Description);
public record AccountCredited(decimal Amount, string Description);

#endregion

#region sample_polecat_monthly_account_activity_document

public class MonthlyAccountActivity
{
    public string Id { get; set; } = "";
    public Guid AccountId { get; set; }
    public string Month { get; set; } = "";
    public int TransactionCount { get; set; }
    public decimal TotalDebits { get; set; }
    public decimal TotalCredits { get; set; }
    public decimal NetChange => TotalCredits - TotalDebits;
}

#endregion

#region sample_polecat_monthly_account_activity_projection

public class MonthlyAccountActivityProjection : MultiStreamProjection<MonthlyAccountActivity, string>
{
    public MonthlyAccountActivityProjection()
    {
        // Route each event to a document keyed by "{streamId}:{yyyy-MM}"
        // This segments a single account stream into monthly summaries
        Identity<IEvent<AccountDebited>>(e =>
            $"{e.StreamId}:{e.Timestamp:yyyy-MM}");
        Identity<IEvent<AccountCredited>>(e =>
            $"{e.StreamId}:{e.Timestamp:yyyy-MM}");
    }

    public MonthlyAccountActivity Create(IEvent<AccountDebited> e) => new()
    {
        AccountId = e.StreamId,
        Month = e.Timestamp.ToString("yyyy-MM"),
        TransactionCount = 1,
        TotalDebits = e.Data.Amount
    };

    public MonthlyAccountActivity Create(IEvent<AccountCredited> e) => new()
    {
        AccountId = e.StreamId,
        Month = e.Timestamp.ToString("yyyy-MM"),
        TransactionCount = 1,
        TotalCredits = e.Data.Amount
    };

    public void Apply(IEvent<AccountDebited> e, MonthlyAccountActivity view)
    {
        view.TransactionCount++;
        view.TotalDebits += e.Data.Amount;
    }

    public void Apply(IEvent<AccountCredited> e, MonthlyAccountActivity view)
    {
        view.TransactionCount++;
        view.TotalCredits += e.Data.Amount;
    }
}

#endregion

[Collection("integration")]
public class time_based_multi_stream_projection_tests : IntegrationContext
{
    public time_based_multi_stream_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithMonthlyProjection()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<MonthlyAccountActivityProjection>(ProjectionLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task segments_single_stream_into_monthly_documents()
    {
        var store = await CreateStoreWithMonthlyProjection();
        var accountId = Guid.NewGuid();

        // Start the account stream with events spread across two months
        await using var session = store.LightweightSession();
        session.Events.StartStream(accountId, new AccountOpened("Checking"));
        await session.SaveChangesAsync();

        // Append debits and credits
        await using var session2 = store.LightweightSession();
        session2.Events.Append(accountId,
            new AccountDebited(100m, "Groceries"),
            new AccountCredited(2500m, "Salary"),
            new AccountDebited(50m, "Coffee"));
        await session2.SaveChangesAsync();

        // All events land in the same month (current month) since timestamps are set by the server
        var currentMonth = DateTime.UtcNow.ToString("yyyy-MM");
        var expectedId = $"{accountId}:{currentMonth}";

        await using var query = store.QuerySession();
        var activity = await query.LoadAsync<MonthlyAccountActivity>(expectedId);

        activity.ShouldNotBeNull();
        activity.AccountId.ShouldBe(accountId);
        activity.Month.ShouldBe(currentMonth);
        activity.TotalDebits.ShouldBe(150m);
        activity.TotalCredits.ShouldBe(2500m);
        activity.TransactionCount.ShouldBe(3);
    }

    [Fact]
    public async Task separate_accounts_get_separate_monthly_documents()
    {
        var store = await CreateStoreWithMonthlyProjection();
        var account1 = Guid.NewGuid();
        var account2 = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(account1, new AccountOpened("Checking"));
        session.Events.StartStream(account2, new AccountOpened("Savings"));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(account1, new AccountDebited(200m, "Rent"));
        session2.Events.Append(account2, new AccountCredited(5000m, "Deposit"));
        await session2.SaveChangesAsync();

        var currentMonth = DateTime.UtcNow.ToString("yyyy-MM");

        await using var query = store.QuerySession();
        var activity1 = await query.LoadAsync<MonthlyAccountActivity>($"{account1}:{currentMonth}");
        var activity2 = await query.LoadAsync<MonthlyAccountActivity>($"{account2}:{currentMonth}");

        activity1.ShouldNotBeNull();
        activity1.TotalDebits.ShouldBe(200m);

        activity2.ShouldNotBeNull();
        activity2.TotalCredits.ShouldBe(5000m);
    }
}
