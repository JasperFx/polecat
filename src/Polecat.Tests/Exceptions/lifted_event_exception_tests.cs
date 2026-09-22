using Polecat.Exceptions;

namespace Polecat.Tests.Exceptions;

/// <summary>
///     #651: Polecat's event store exceptions keep their own names, but a store-agnostic caller has
///     to be able to catch the JasperFx.Events type and have it work — that is the whole point of
///     the jasperfx#751/#756 lift, and it is what a Wolverine
///     <c>OnException&lt;JasperFx.Events.StreamLockedException&gt;()</c> policy matches on. These
///     facts are cheap and they are the only thing standing between "derives from the shared type"
///     and a silent regression back to <c>: Exception</c>.
/// </summary>
public class lifted_event_exception_tests
{
    [Fact]
    public void stream_locked_is_catchable_as_the_lifted_type()
    {
        var streamId = Guid.NewGuid();
        var inner = new DivideByZeroException();
        var ex = new StreamLockedException(streamId, inner);

        ex.ShouldBeAssignableTo<JasperFx.Events.StreamLockedException>();
        ex.StreamId.ShouldBe(streamId);
        ex.InnerException.ShouldBeSameAs(inner);
        ex.Message.ShouldContain(streamId.ToString());
    }

    [Fact]
    public void non_existent_stream_is_catchable_as_the_lifted_type()
    {
        var ex = new NonExistentStreamException("foo");

        ex.ShouldBeAssignableTo<JasperFx.Events.NonExistentStreamException>();
        ex.Id.ShouldBe("foo");
        ex.Message.ShouldContain("foo");
    }

    [Fact]
    public void existing_stream_id_collision_is_catchable_as_the_lifted_type()
    {
        var id = Guid.NewGuid();
        var ex = new ExistingStreamIdCollisionException(id);

        ex.ShouldBeAssignableTo<JasperFx.Events.ExistingStreamIdCollisionException>();
        ex.Id.ShouldBe(id);
    }
}
