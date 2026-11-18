using JasperFx.Events;

namespace Polecat.Tests.Testing;

#region sample_polecat_stub_stream_domain
public record AccountOpened(string Owner, decimal OpeningBalance);

public record Deposited(decimal Amount);

public record Withdrawn(decimal Amount);

public record Overdrawn(decimal Attempted, decimal Balance);

public class Account
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public bool IsClosed { get; set; }

    public static Account Create(AccountOpened opened) =>
        new() { Owner = opened.Owner, Balance = opened.OpeningBalance };

    public void Apply(Deposited e) => Balance += e.Amount;
    public void Apply(Withdrawn e) => Balance -= e.Amount;
}
#endregion

#region sample_polecat_stub_stream_handler
public record WithdrawFunds(Guid AccountId, decimal Amount);

public static class WithdrawFundsHandler
{
    // Wolverine's [WriteAggregate] hands the handler the stream directly; nothing here touches a
    // session, a database, or Polecat.
    public static void Handle(WithdrawFunds command, IEventStream<Account> stream)
    {
        var account = stream.Aggregate
                      ?? throw new InvalidOperationException("That account does not exist");

        if (account.Balance < command.Amount)
        {
            stream.AppendOne(new Overdrawn(command.Amount, account.Balance));
            return;
        }

        stream.AppendOne(new Withdrawn(command.Amount));
    }
}
#endregion

public class unit_testing_event_handlers_samples
{
    #region sample_polecat_stub_stream_happy_path
    [Fact]
    public void a_withdrawal_within_the_balance_appends_withdrawn()
    {
        var stream = new StubEventStream<Account>(new Account { Owner = "Hank", Balance = 250m });

        WithdrawFundsHandler.Handle(new WithdrawFunds(stream.Id, 100m), stream);

        // The event itself, not "a method was called with something".
        stream.EventsAppended.ShouldHaveSingleItem()
            .ShouldBeOfType<Withdrawn>()
            .Amount.ShouldBe(100m);
    }
    #endregion

    #region sample_polecat_stub_stream_refusal
    [Fact]
    public void a_withdrawal_over_the_balance_appends_overdrawn_instead()
    {
        var stream = new StubEventStream<Account>(new Account { Owner = "Hank", Balance = 20m });

        WithdrawFundsHandler.Handle(new WithdrawFunds(stream.Id, 100m), stream);

        var overdrawn = stream.EventsAppended.ShouldHaveSingleItem().ShouldBeOfType<Overdrawn>();
        overdrawn.Attempted.ShouldBe(100m);
        overdrawn.Balance.ShouldBe(20m);
    }
    #endregion

    #region sample_polecat_stub_stream_missing_aggregate
    [Fact]
    public void a_null_aggregate_is_a_stream_that_does_not_exist_yet()
    {
        // No events have ever been appended to this stream, so the handler sees no aggregate.
        var stream = new StubEventStream<Account>(null);

        Should.Throw<InvalidOperationException>(
            () => WithdrawFundsHandler.Handle(new WithdrawFunds(stream.Id, 100m), stream));

        stream.EventsAppended.ShouldBeEmpty();
    }
    #endregion

    #region sample_polecat_stub_stream_identity_and_version
    [Fact]
    public void identities_and_versions_are_settable_for_a_handler_that_reads_them()
    {
        var accountId = Guid.NewGuid();

        var stream = new StubEventStream<Account>(new Account { Owner = "Hank", Balance = 250m })
        {
            // Set whichever identity the handler under test reads. Both are populated by default,
            // because the stub cannot know which identity style the store is configured for.
            Id = accountId,
            Key = accountId.ToString(),
            StartingVersion = 4,
            CurrentVersion = 4
        };

        WithdrawFundsHandler.Handle(new WithdrawFunds(accountId, 100m), stream);

        stream.EventsAppended.ShouldHaveSingleItem();

        // Events wraps the same appends in IEvent envelopes, for a handler or an assertion that
        // reads the interface's own member rather than the raw bodies.
        stream.Events.ShouldHaveSingleItem().Data.ShouldBeOfType<Withdrawn>();
    }
    #endregion
}
