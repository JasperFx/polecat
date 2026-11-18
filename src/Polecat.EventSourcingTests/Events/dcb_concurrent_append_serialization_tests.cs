#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     Concurrent appends guarded by the same DCB tag boundary must serialize: when several writers
///     race the same boundary, exactly one may commit. Ported from Marten's coverage of the same
///     invariant — marten#4591 (the barrier-synced shape) and marten#5300 (the staggered shape).
/// </summary>
/// <remarks>
///     The racers each carry their OWN StudentId as well as the shared CourseId. EventBoundary routes
///     an event to a stream by the first tag with an AggregateType, so distinct per-racer StudentIds
///     mean distinct streams, and the (stream_id, version) constraint on pc_events does NOT serialize
///     them. The race has to be caught by the DCB boundary check alone — which is the whole point.
/// </remarks>
public class dcb_concurrent_append_serialization_tests : OneOffConfigurationsContext
{
    private const int Racers = 16;
    private const int Rounds = 50;

    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     The staggered shape (marten#5300): racers are NOT synchronized, so each one's fetch and save
    ///     interleave freely with everyone else's. This is the shape that catches a boundary check whose
    ///     reads do not agree on a single point in time.
    /// </summary>
    [Fact]
    public async Task staggered_racers_on_one_boundary_serialize_to_one_winner()
    {
        await ConfigureAsync();

        long worst = 0;
        for (var round = 0; round < Rounds; round++)
        {
            var courseId = new CourseId(Guid.NewGuid());
            await Task.WhenAll(Enumerable.Range(0, Racers).Select(_ => TryEnrollAsync(courseId)));

            worst = Math.Max(worst, await EnrollmentCountAsync(courseId));
        }

        worst.ShouldBe(1);
    }

    /// <summary>
    ///     Same race, but the boundary already has an event under it (an unrelated AssignmentSubmitted),
    ///     so the racers are appending at an established position rather than creating the boundary from
    ///     nothing. A check that only guards the first-ever append would pass the test above and fail here.
    /// </summary>
    [Fact]
    public async Task staggered_racers_at_an_established_boundary_serialize_to_one_winner()
    {
        await ConfigureAsync();

        long worst = 0;
        for (var round = 0; round < Rounds; round++)
        {
            var courseId = new CourseId(Guid.NewGuid());
            await SeedUnrelatedActivityAsync(courseId);
            await Task.WhenAll(Enumerable.Range(0, Racers).Select(_ => TryEnrollAsync(courseId)));

            worst = Math.Max(worst, await EnrollmentCountAsync(courseId));
        }

        worst.ShouldBe(1);
    }

    /// <summary>
    ///     The barrier-synced shape (marten#4591): every racer completes its fetch, then all of them are
    ///     released into SaveChangesAsync at once. A different interleaving from the staggered test — this
    ///     one puts maximum pressure on the save-time check rather than on the fetch's reads.
    /// </summary>
    [Fact]
    public async Task barrier_synced_racers_on_one_boundary_serialize_to_one_winner()
    {
        await ConfigureAsync();

        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(courseId);

        var fetched = new TaskCompletionSource[Racers];
        for (var i = 0; i < Racers; i++) fetched[i] = new TaskCompletionSource();
        var release = new TaskCompletionSource();

        var racers = Enumerable.Range(0, Racers).Select(i => Task.Run(async () =>
        {
            await using var session = theStore.LightweightSession();
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query);

            fetched[i].SetResult();
            await release.Task;

            var enrolled = session.Events.BuildEvent(new StudentEnrolled($"Student-{i}", "Math"));
            enrolled.WithTag(new StudentId(Guid.NewGuid()), courseId);
            boundary.AppendOne(enrolled);

            try
            {
                await session.SaveChangesAsync();
                return (Committed: true, Conflicted: false);
            }
            catch (DcbConcurrencyException)
            {
                return (Committed: false, Conflicted: true);
            }
            catch (AggregateException ex) when (ex.InnerExceptions.All(x => x is DcbConcurrencyException))
            {
                return (Committed: false, Conflicted: true);
            }
        })).ToArray();

        await Task.WhenAll(fetched.Select(x => x.Task));
        release.SetResult();

        var results = await Task.WhenAll(racers);

        var committed = results.Count(x => x.Committed);
        committed.ShouldBe(1,
            $"Expected exactly one racer to commit; {committed} committed and {results.Count(x => x.Conflicted)} saw a DCB conflict.");
    }

    // Invariant under test: at most one enrollment per course. A racer whose fetch already shows an
    // enrollment backs off, so every commit past the first is one the boundary check should have refused.
    private async Task TryEnrollAsync(CourseId courseId)
    {
        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(courseId));

        if (boundary.Aggregate is { StudentName.Length: > 0 })
        {
            return;
        }

        var enrolled = session.Events.BuildEvent(new StudentEnrolled("Student", "Math"));
        enrolled.WithTag(new StudentId(Guid.NewGuid()), courseId);
        boundary.AppendOne(enrolled);

        try
        {
            await session.SaveChangesAsync();
        }
        catch (DcbConcurrencyException)
        {
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(x => x is DcbConcurrencyException))
        {
        }
    }

    private async Task SeedUnrelatedActivityAsync(CourseId courseId)
    {
        await using var session = theStore.LightweightSession();
        var submitted = session.Events.BuildEvent(new AssignmentSubmitted("kickoff", 100));
        submitted.WithTag(new StudentId(Guid.NewGuid()), courseId);
        session.Events.Append(Guid.NewGuid(), submitted);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> EnrollmentCountAsync(CourseId courseId)
    {
        await using var session = theStore.LightweightSession();
        var enrollments = await session.Events.QueryByTagsAsync(
            new EventTagQuery().Or<StudentEnrolled, CourseId>(courseId), TestContext.Current.CancellationToken);
        return enrollments.Count;
    }
}
