using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record TagReadCode(string Value);

public record TagReadSeat(int Value);

/// <summary>
///     What a DCB tag read returns (#841). Every other event query leaves archived events out, so a
///     decision folded from tags has to as well, whichever of the four paths into the SQL it took:
///     <c>QueryByTagsAsync</c>, <c>EventsExistAsync</c> and their two batched twins.
/// </summary>
public class dcb_tag_read_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.AddEventType<AssignmentSubmitted>();
            opts.Events.RegisterTagType<StudentId>("student").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<TagReadCode>("code").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<TagReadSeat>("seat").ForAggregate<StudentCourseEnrollment>();
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<Guid> AppendAsync(object data, params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        var tagged = session.Events.BuildEvent(data);
        tagged.WithTag(tags);
        session.Events.Append(streamId, tagged);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task ArchiveAsync(Guid streamId)
    {
        await using var session = theStore.LightweightSession();
        session.Events.ArchiveStream(streamId);
        await session.SaveChangesAsync(Ct);
    }

    private async Task UnArchiveAsync(Guid streamId)
    {
        await using var session = theStore.LightweightSession();
        session.Events.UnArchiveStream(streamId);
        await session.SaveChangesAsync(Ct);
    }

    private static StudentEnrolled Enrolled(string name = "Alice") => new(name, "Math");

    [Fact]
    public async Task query_by_tags_leaves_out_an_archived_event()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled("Archived"), course);
        await AppendAsync(Enrolled("Live"), course);

        await ArchiveAsync(doomed);

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(course), Ct);
        events.Single().Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Live");
    }

    [Fact]
    public async Task events_exist_is_false_for_an_archived_event()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(doomed);

        await using var session = theStore.LightweightSession();
        (await session.Events.EventsExistAsync(new EventTagQuery().Or<CourseId>(course), Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_event_type_condition_does_not_reach_past_the_archived_filter()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(doomed);

        var query = new EventTagQuery().Or<StudentEnrolled, CourseId>(course);

        await using var session = theStore.LightweightSession();
        (await session.Events.QueryByTagsAsync(query, Ct)).ShouldBeEmpty();
        (await session.Events.EventsExistAsync(query, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task batched_reads_leave_out_an_archived_event()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(doomed);

        var query = new EventTagQuery().Or<CourseId>(course);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var exists = batch.EventsExist(query);
        var boundary = batch.FetchForWritingByTags<StudentCourseEnrollment>(query);
        await batch.Execute(Ct);

        (await exists).ShouldBeFalse();
        (await boundary).Events.ShouldBeEmpty();
    }

    [Fact]
    public async Task aggregating_and_fetching_for_writing_do_not_fold_an_archived_event()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(doomed);

        var query = new EventTagQuery().Or<CourseId>(course);

        await using var session = theStore.LightweightSession();
        (await session.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query, Ct)).ShouldBeNull();

        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        boundary.Aggregate.ShouldBeNull();
        boundary.Events.ShouldBeEmpty();
        boundary.LastSeenSequence.ShouldBe(0);
    }

    [Fact]
    public async Task un_archiving_brings_the_events_back()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(streamId);
        await UnArchiveAsync(streamId);

        var query = new EventTagQuery().Or<CourseId>(course);

        await using var session = theStore.LightweightSession();
        (await session.Events.QueryByTagsAsync(query, Ct)).Count.ShouldBe(1);
        (await session.Events.EventsExistAsync(query, Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task events_exist_finds_an_event_carrying_only_one_of_two_queried_tag_types()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(Enrolled(), course);

        var query = new EventTagQuery().Or<CourseId>(course).Or<StudentId>(new StudentId(Guid.NewGuid()));

        await using var session = theStore.LightweightSession();
        (await session.Events.QueryByTagsAsync(query, Ct)).Count.ShouldBe(1);
        (await session.Events.EventsExistAsync(query, Ct)).ShouldBeTrue();

        var batch = session.CreateBatchQuery();
        var batched = batch.EventsExist(query);
        await batch.Execute(Ct);
        (await batched).ShouldBeTrue();
    }

    [Fact]
    public async Task events_exist_applies_an_event_type_to_its_own_condition_only()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var student = new StudentId(Guid.NewGuid());
        await AppendAsync(Enrolled(), course);

        // The event type filter belongs to the student condition, which matches nothing. It must not
        // turn the course condition, which has no filter, into a miss -- nor the other way around.
        await using var session = theStore.LightweightSession();

        (await session.Events.EventsExistAsync(
            new EventTagQuery().Or<CourseId>(course).Or<AssignmentSubmitted, StudentId>(student), Ct))
            .ShouldBeTrue();

        (await session.Events.EventsExistAsync(
            new EventTagQuery().Or<AssignmentSubmitted, CourseId>(course).Or<StudentId>(student), Ct))
            .ShouldBeFalse();
    }

    [Fact]
    public async Task an_event_tagged_with_two_queried_values_comes_back_once()
    {
        await ConfigureAsync();
        var first = new CourseId(Guid.NewGuid());
        var second = new CourseId(Guid.NewGuid());
        await AppendAsync(new AssignmentSubmitted("HW1", 95), first, second);

        var query = new EventTagQuery().Or<CourseId>(first).Or<CourseId>(second);

        await using var session = theStore.LightweightSession();
        (await session.Events.QueryByTagsAsync(query, Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task an_event_tagged_with_two_queried_types_comes_back_once()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var student = new StudentId(Guid.NewGuid());
        await AppendAsync(new AssignmentSubmitted("HW1", 95), course, student);

        var query = new EventTagQuery().Or<CourseId>(course).Or<StudentId>(student);

        await using var session = theStore.LightweightSession();
        (await session.Events.QueryByTagsAsync(query, Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_doubly_tagged_event_folds_once_on_every_path()
    {
        await ConfigureAsync();
        var first = new CourseId(Guid.NewGuid());
        var second = new CourseId(Guid.NewGuid());
        await AppendAsync(new AssignmentSubmitted("HW1", 95), first, second);

        var query = new EventTagQuery().Or<CourseId>(first).Or<CourseId>(second);

        await using var session = theStore.LightweightSession();

        var aggregated = await session.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query, Ct);
        aggregated!.Assignments.Count.ShouldBe(1);

        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        boundary.Aggregate!.Assignments.Count.ShouldBe(1);

        var batch = session.CreateBatchQuery();
        var batched = batch.FetchForWritingByTags<StudentCourseEnrollment>(query);
        await batch.Execute(Ct);
        (await batched).Aggregate!.Assignments.Count.ShouldBe(1);
    }

    [Fact]
    public async Task several_tagged_events_stay_in_sequence_order()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var student = new StudentId(Guid.NewGuid());

        await AppendAsync(Enrolled("one"), student);
        await AppendAsync(Enrolled("two"), course, student);
        await AppendAsync(Enrolled("three"), course);

        var query = new EventTagQuery().Or<CourseId>(course).Or<StudentId>(student);

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(query, Ct);

        events.Select(x => ((StudentEnrolled)x.Data).StudentName).ShouldBe(["one", "two", "three"]);
    }

    // A boundary and its check have to agree about archived events. The check never reads events: it
    // compares a counter. So the two can only disagree if the counter stays still while what the
    // boundary reads changes, or moves in a way a retry cannot clear.

    [Fact]
    public async Task a_boundary_over_nothing_but_archived_events_is_empty_and_saves()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), course);

        await ArchiveAsync(doomed);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.ShouldBeEmpty();

        var tagged = session.Events.BuildEvent(Enrolled("Bob"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);

        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task un_archiving_under_a_boundary_is_a_conflict()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = await AppendAsync(Enrolled(), course);
        await ArchiveAsync(streamId);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.ShouldBeEmpty();

        // The decision above was made without an event that is about to be visible again
        await UnArchiveAsync(streamId);

        var tagged = session.Events.BuildEvent(Enrolled("Bob"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);

        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task archiving_under_a_boundary_is_a_conflict_a_retry_clears()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = await AppendAsync(Enrolled(), course);

        var query = new EventTagQuery().Or<CourseId>(course);

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundary.Events.Count.ShouldBe(1);

            await ArchiveAsync(streamId);

            var tagged = session.Events.BuildEvent(Enrolled("Bob"));
            tagged.WithTag(course);
            session.Events.Append(Guid.NewGuid(), tagged);

            await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        await using (var retry = theStore.LightweightSession())
        {
            var boundary = await retry.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundary.Events.ShouldBeEmpty();

            var tagged = retry.Events.BuildEvent(Enrolled("Bob"));
            tagged.WithTag(course);
            retry.Events.Append(Guid.NewGuid(), tagged);

            await retry.SaveChangesAsync(Ct);
        }
    }

    /// <summary>
    ///     The bump is SQL stringifying a tag column and the capture is .NET stringifying a value, and
    ///     the check only works if they land on the same <c>pc_dcb_tag_version</c> row. So this goes
    ///     through every tag column type there is rather than the one a boundary usually uses.
    /// </summary>
    [Fact]
    public async Task an_archive_reaches_the_counter_of_every_tag_column_type()
    {
        await ConfigureAsync();

        var student = new StudentId(Guid.NewGuid());
        var code = new TagReadCode("Mixed-Case Code");
        var seat = new TagReadSeat(1234);

        foreach (var query in new[]
                 {
                     new EventTagQuery().Or<StudentId>(student),
                     new EventTagQuery().Or<TagReadCode>(code),
                     new EventTagQuery().Or<TagReadSeat>(seat)
                 })
        {
            var streamId = await AppendAsync(Enrolled(), student, code, seat);

            await using var session = theStore.LightweightSession();
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundary.Events.ShouldNotBeEmpty();

            await ArchiveAsync(streamId);

            session.Events.Append(Guid.NewGuid(), new AssignmentSubmitted("HW1", 95));

            await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }
    }

    /// <summary>
    ///     A stream whose events carry no tag this boundary names has nothing to do with it. Archiving
    ///     has to be a conflict for the tags on the stream and not for everything.
    /// </summary>
    [Fact]
    public async Task archiving_an_unrelated_stream_leaves_a_boundary_alone()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(Enrolled(), course);
        var unrelated = await AppendAsync(Enrolled(), new CourseId(Guid.NewGuid()));

        await using var session = theStore.LightweightSession();
        await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        await ArchiveAsync(unrelated);

        var tagged = session.Events.BuildEvent(Enrolled("Bob"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);

        await session.SaveChangesAsync(Ct);
    }

    /// <summary>
    ///     A deliberate exception: the explorer is the diagnostic surface. Its stream reads and listings
    ///     show archived data, and a tag lookup that hid it would leave an operator unable to find the
    ///     events the DCB reads have just stopped returning.
    /// </summary>
    [Fact]
    public async Task the_explorer_still_shows_an_archived_event()
    {
        await ConfigureAsync();
        var student = new StudentId(Guid.NewGuid());
        var doomed = await AppendAsync(Enrolled(), student);

        await ArchiveAsync(doomed);

        var explorer = (IEventStore)theStore;
        var tags = new Dictionary<string, string> { ["StudentId"] = student.Value.ToString() };

        var found = new List<EventRecord>();
        await foreach (var e in explorer.QueryByTagsAsync(tags, JasperFx.StorageConstants.DefaultTenantId, Ct))
        {
            found.Add(e);
        }

        found.Count.ShouldBe(1);
    }
}
