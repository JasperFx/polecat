using JasperFx.Events;

namespace Polecat.Tests.Exceptions;

/// <summary>
///     #944 (Polecat 6): an exception that Marten, Polecat and Fisher share is ONE type in
///     JasperFx, thrown directly by all three stores. Polecat used to keep empty subclasses
///     (<c>Polecat.Exceptions.StreamLockedException</c> and so on) for their names; those are gone,
///     and these facts are what stops one from coming back. A subclass would still be catchable as
///     the JasperFx type, so nothing else in the suite would notice it.
/// </summary>
public class lifted_event_exception_tests
{
    public static IEnumerable<TheoryDataRow<Type>> SharedExceptionTypes() =>
    [
        new(typeof(StreamLockedException)),
        new(typeof(ExistingStreamIdCollisionException)),
        new(typeof(NonExistentStreamException)),
        new(typeof(UnknownEventTypeException)),
        new(typeof(EventDeserializationFailureException)),
        new(typeof(DefaultTenantUsageDisabledException)),
        new(typeof(ArchivedStreamException)),
        new(typeof(JasperFx.BadLinqExpressionException))
    ];

    [Theory]
    [MemberData(nameof(SharedExceptionTypes))]
    public void polecat_declares_no_subclass_of_the_shared_exception(Type shared)
    {
        var subclasses = typeof(DocumentStore).Assembly.GetTypes()
            .Where(t => t.IsSubclassOf(shared))
            .Select(t => t.FullName)
            .ToArray();

        subclasses.ShouldBeEmpty();
    }

    [Fact]
    public void the_obsolete_invalid_stream_exception_is_gone()
    {
        typeof(DocumentStore).Assembly.GetType("Polecat.Exceptions.InvalidStreamException").ShouldBeNull();
    }

    [Fact]
    public void stream_locked_carries_the_stream_id_and_cause()
    {
        var streamId = Guid.NewGuid();
        var inner = new DivideByZeroException();
        var ex = new StreamLockedException(streamId, inner);

        ex.StreamId.ShouldBe(streamId);
        ex.InnerException.ShouldBeSameAs(inner);
        ex.Message.ShouldContain(streamId.ToString());
    }

    [Fact]
    public void non_existent_stream_carries_the_id()
    {
        var ex = new NonExistentStreamException("foo");

        ex.Id.ShouldBe("foo");
        ex.Message.ShouldContain("foo");
    }

    [Fact]
    public void existing_stream_id_collision_carries_the_id()
    {
        var id = Guid.NewGuid();
        var ex = new ExistingStreamIdCollisionException(id);

        ex.Id.ShouldBe(id);
    }
}
