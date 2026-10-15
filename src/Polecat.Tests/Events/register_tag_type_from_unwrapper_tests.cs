using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #772 / jasperfx#1012: <c>RegisterTagType&lt;TTag, TInner&gt;(Func&lt;TTag, TInner&gt;)</c> builds the
///     registration from <c>TagTypeRegistration.FromUnwrapper</c>, the trim and Native AOT safe path,
///     and must behave exactly like the reflective <c>RegisterTagType&lt;TTag&gt;()</c>.
/// </summary>
public class register_tag_type_from_unwrapper_tests : OneOffConfigurationsContext
{
    [Fact]
    public void registration_carries_the_tag_and_inner_types()
    {
        var options = new StoreOptions();
        var registration = options.Events.RegisterTagType<StudentId, Guid>(x => x.Value, "student");

        registration.TagType.ShouldBe(typeof(StudentId));
        registration.SimpleType.ShouldBe(typeof(Guid));
        registration.TableSuffix.ShouldBe("student");

        // A second registration of the same tag type returns the first one, as RegisterTagType<TTag>() does
        options.Events.RegisterTagType<StudentId>().ShouldBeSameAs(registration);
    }

    [Fact]
    public async Task tagged_events_round_trip_through_an_unwrapper_registration()
    {
        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<StudentId, Guid>(x => x.Value, "student");
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var studentId = new StudentId(Guid.NewGuid());

        await using (var session = theStore.LightweightSession())
        {
            var e = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
            e.WithTag(studentId);
            session.Events.Append(Guid.NewGuid(), e);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        var events = await query.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(studentId),
            TestContext.Current.CancellationToken);

        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<StudentEnrolled>();
    }
}
