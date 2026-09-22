# Unit Testing Event-Sourced Handlers

A command handler that takes an `IEventStream<T>` — Wolverine's `[WriteAggregate]` shape, or the body
you pass to `WriteToAggregate` — is a pure decision: given this aggregate state and this command,
which events should be appended? That decision is worth testing on its own, without a database.

`JasperFx.Events.StubEventStream<T>` is the stand-in. Construct one with the aggregate state the
handler should see, call the handler, and assert on what it appended.

**No Polecat-specific type is involved.** Polecat's `IEventStream<T>` *is*
`JasperFx.Events.IEventStream<T>`, so the stub satisfies it as-is — and the same test reads
identically on Marten and Fisher. It is one of the places where the store-agnostic abstraction pays
off for an application rather than just for the stores.

## The handler under test

<!-- snippet: sample_polecat_stub_stream_handler -->
<a id='snippet-sample_polecat_stub_stream_handler'></a>
```cs
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
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L29-L50' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_handler' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Working against this aggregate:

<!-- snippet: sample_polecat_stub_stream_domain -->
<a id='snippet-sample_polecat_stub_stream_domain'></a>
```cs
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
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L5-L27' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_domain' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Asserting on what was appended

<!-- snippet: sample_polecat_stub_stream_happy_path -->
<a id='snippet-sample_polecat_stub_stream_happy_path'></a>
```cs
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
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L54-L67' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_happy_path' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`EventsAppended` is the raw event bodies, in order, which is what most assertions want.

The interesting case is usually the one where the handler decides *differently*:

<!-- snippet: sample_polecat_stub_stream_refusal -->
<a id='snippet-sample_polecat_stub_stream_refusal'></a>
```cs
[Fact]
public void a_withdrawal_over_the_balance_appends_overdrawn_instead()
{
    var stream = new StubEventStream<Account>(new Account { Owner = "Hank", Balance = 20m });

    WithdrawFundsHandler.Handle(new WithdrawFunds(stream.Id, 100m), stream);

    var overdrawn = stream.EventsAppended.ShouldHaveSingleItem().ShouldBeOfType<Overdrawn>();
    overdrawn.Attempted.ShouldBe(100m);
    overdrawn.Balance.ShouldBe(20m);
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L69-L81' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_refusal' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## A stream that does not exist yet

`Aggregate` is null for a stream with no events — the state a handler that starts a stream, or one
that must refuse, should see:

<!-- snippet: sample_polecat_stub_stream_missing_aggregate -->
<a id='snippet-sample_polecat_stub_stream_missing_aggregate'></a>
```cs
[Fact]
public void a_null_aggregate_is_a_stream_that_does_not_exist_yet()
{
    // No events have ever been appended to this stream, so the handler sees no aggregate.
    var stream = new StubEventStream<Account>(null);

    Should.Throw<InvalidOperationException>(
        () => WithdrawFundsHandler.Handle(new WithdrawFunds(stream.Id, 100m), stream));

    stream.EventsAppended.ShouldBeEmpty();
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L83-L95' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_missing_aggregate' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Identities and versions

`Id` and `Key` are both populated by default, because the stub cannot know which
`StreamIdentity` the store is configured for; set whichever the handler reads. `StartingVersion` and
`CurrentVersion` are settable for a version-sensitive handler, and `Events` exposes the same appends
as `IEvent` envelopes:

<!-- snippet: sample_polecat_stub_stream_identity_and_version -->
<a id='snippet-sample_polecat_stub_stream_identity_and_version'></a>
```cs
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
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Testing/unit_testing_event_handlers_samples.cs#L97-L121' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_stub_stream_identity_and_version' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Prefer this over a mocking library

It is tempting to reach for `Substitute.For<IEventStream<Account>>()` and then verify
`Received(1).AppendOne(Arg.Any<Withdrawn>())`. Don't.

That assertion proves a method was called. It does not check **which** event was appended or what it
carried — and the event, with its values, is the entire content of the handler's decision. A handler
that appends `new Withdrawn(0m)` passes it. So does one that appends the right event for the wrong
reason. Tightening the mock to `Received(1).AppendOne(new Withdrawn(100m))` gets you closer, at the
cost of a setup that breaks whenever the handler is refactored in a way that changes nothing about
its behaviour.

A recorded list of events is the thing the handler is supposed to produce. Assert on that.

## Where the stub stops

The stub **records**. It does not persist, project, validate, or raise anything:

- appends go into a list; nothing is written, and nothing reads back
- `TryFastForwardVersion` does nothing, and no optimistic-concurrency exception is ever raised
- inline projections do not run, so no read model is updated
- `sp_getapplock` semantics, QuickAppend's version handling, and projection correctness are all
  invisible here

Those belong in [integration tests](/testing/integration) against a real SQL Server. The division is
the useful one: the stub covers *what the handler decided*, and the integration test covers *what the
store did with it*.
