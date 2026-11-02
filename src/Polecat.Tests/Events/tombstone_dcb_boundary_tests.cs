using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #890's Related section. A tombstone changes what a boundary over the stream's tags reads, exactly as
///     an archive does (#841), and the boundary check only ever compares the counter, so the counter has to
///     move or the boundary saves a decision made from events that no longer exist.
/// </summary>
public class tombstone_dcb_boundary_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync(Action<StoreOptions>? configure = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<StudentId>("student").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course").ForAggregate<StudentCourseEnrollment>();
            configure?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<Guid> AppendAsync(params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        tagged.WithTag(tags);
        session.Events.Append(streamId, tagged);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task TombstoneAsync(Guid streamId)
    {
        await using var session = theStore.LightweightSession();
        session.Events.TombstoneStream(streamId);
        await session.SaveChangesAsync(Ct);
    }

    private async Task<long> VersionRowsAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{nameof(tombstone_dcb_boundary_tests)}].[pc_dcb_tag_version]";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    [Fact]
    public async Task a_boundary_cannot_save_through_a_tombstone_of_what_it_read()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.Count.ShouldBe(1);

        await TombstoneAsync(doomed);

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        boundary.AppendOne(tagged);

        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_retry_after_the_tombstone_sees_the_stream_gone_and_saves()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);
        await TombstoneAsync(doomed);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.ShouldBeEmpty();

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        boundary.AppendOne(tagged);

        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task every_tag_on_the_stream_moves()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var student = new StudentId(Guid.NewGuid());
        var doomed = await AppendAsync(course, student);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<StudentId>(student), Ct);

        await TombstoneAsync(doomed);

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(student);
        boundary.AppendOne(tagged);

        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task tombstoning_an_unrelated_stream_leaves_a_boundary_alone()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var other = await AppendAsync(new CourseId(Guid.NewGuid()));
        await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        await TombstoneAsync(other);

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        boundary.AppendOne(tagged);

        await session.SaveChangesAsync(Ct);
    }

    /// <summary>The conflict in #890: deleting the row restarts the counter and a stale capture can match.</summary>
    [Fact]
    public async Task the_counter_row_is_kept()
    {
        await ConfigureAsync();
        var doomed = await AppendAsync(new CourseId(Guid.NewGuid()));
        (await VersionRowsAsync()).ShouldBe(1);

        await TombstoneAsync(doomed);

        (await VersionRowsAsync()).ShouldBe(1);
    }

    /// <summary>Events older than the counter have no row, and a boundary reading them captures 0.</summary>
    [Fact]
    public async Task a_tag_with_no_counter_row_gets_one()
    {
        await ConfigureAsync();
        var doomed = await AppendAsync(new CourseId(Guid.NewGuid()));

        await using (var conn = await OpenConnectionAsync())
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DELETE FROM [{nameof(tombstone_dcb_boundary_tests)}].[pc_dcb_tag_version]";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        await TombstoneAsync(doomed);

        (await VersionRowsAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task archived_partitioning_has_no_cascade_and_is_still_a_conflict()
    {
        await ConfigureAsync(opts => opts.EventGraph.UseArchivedStreamPartitioning = true);
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        await TombstoneAsync(doomed);

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        boundary.AppendOne(tagged);

        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }
}
