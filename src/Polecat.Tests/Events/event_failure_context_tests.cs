using JasperFx.Events.Daemon;
using Polecat.Exceptions;

namespace Polecat.Tests.Events;

/// <summary>
///     #368 / jasperfx#565: the daemon classifies a paused shard purely from what the store's exception
///     declares — <c>ShardFailure.For</c> looks for an <see cref="IEventFailureContext" /> and there is
///     deliberately NO fallback type-name sniffing. So these pin the contract Polecat's read-path
///     exceptions have to hold up: the right category, the failing sequence, and the event type alias
///     surviving as data rather than only as prose in the message.
/// </summary>
public class event_failure_context_tests
{
    [Fact]
    public void deserialization_failure_declares_the_serialization_category()
    {
        var ex = new EventDeserializationFailureException(4815, "quest_started", new DivideByZeroException("Boom!"));

        IEventFailureContext context = ex;
        context.Category.ShouldBe(ShardFailureCategory.EventSerialization);
        context.Sequence.ShouldBe(4815);
        context.EventTypeName.ShouldBe("quest_started");

        // Raised while reading a pc_events row, BEFORE there is an IEvent — nothing else is knowable.
        context.EventId.ShouldBeNull();
        context.StreamId.ShouldBeNull();
        context.StreamKey.ShouldBeNull();
        context.TenantId.ShouldBeNull();
        context.Version.ShouldBeNull();
    }

    [Fact]
    public void unknown_event_type_is_kept_distinct_from_a_serialization_failure()
    {
        // A missing registration or a rollback past the event type's introduction is a deployment fix,
        // not a data fix, so an operator responds to it differently.
        IEventFailureContext context = new UnknownEventTypeException("Some.Removed.EventType, SomeAsm", 1623);

        context.Category.ShouldBe(ShardFailureCategory.UnknownEventType);
        context.Sequence.ShouldBe(1623);
    }

    [Fact]
    public void unknown_event_type_reports_the_sentinel_when_the_throw_site_had_no_row()
    {
        IEventFailureContext context = new UnknownEventTypeException("Some.Removed.EventType, SomeAsm");
        context.Sequence.ShouldBe(UnknownEventTypeException.UnknownSequence);
    }

    [Fact]
    public void shard_failure_classifies_a_deserialization_failure()
    {
        var ex = new EventDeserializationFailureException(4815, "quest_started", new DivideByZeroException("Boom!"));

        var failure = ShardFailure.For(ex, DateTimeOffset.UtcNow);

        failure.Category.ShouldBe(ShardFailureCategory.EventSerialization);
        failure.RootExceptionType.ShouldBe("System.DivideByZeroException");
        failure.Event.ShouldNotBeNull();
        failure.Event.Sequence.ShouldBe(4815);
        failure.Event.EventTypeName.ShouldBe("quest_started");
    }

    [Fact]
    public void shard_failure_finds_the_context_through_a_wrapping_exception()
    {
        // The per-event exceptions routinely arrive wrapped — a ShardStopException around the real
        // failure is the daemon's normal shape — so classification has to walk the whole graph.
        var inner = new EventDeserializationFailureException(99, "quest_started", new DivideByZeroException("Boom!"));
        var wrapped = new InvalidOperationException("Shard stopping", inner);

        var failure = ShardFailure.For(wrapped, DateTimeOffset.UtcNow);

        failure.Category.ShouldBe(ShardFailureCategory.EventSerialization);
        failure.Event!.Sequence.ShouldBe(99);
    }

    [Fact]
    public void shard_failure_takes_the_lowest_sequence_out_of_an_aggregate()
    {
        // A batch can produce several failures at once; the shard stops at the earliest failing event.
        var aggregate = new AggregateException(
            new EventDeserializationFailureException(200, "b", new Exception()),
            new EventDeserializationFailureException(100, "a", new Exception()));

        var failure = ShardFailure.For(aggregate, DateTimeOffset.UtcNow);

        failure.Event!.Sequence.ShouldBe(100);
        failure.Event.EventTypeName.ShouldBe("a");
    }

    [Fact]
    public void anything_that_names_no_event_still_classifies_as_other()
    {
        var failure = ShardFailure.For(new TimeoutException("database went away"), DateTimeOffset.UtcNow);

        failure.Category.ShouldBe(ShardFailureCategory.Other);
        failure.Event.ShouldBeNull();
    }
}
