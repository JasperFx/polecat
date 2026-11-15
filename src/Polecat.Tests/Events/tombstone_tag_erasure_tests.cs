using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record ErasureEmail(string Value);

public record ErasureNumber(int Value);

public record ErasureAccount(long Value);

/// <summary>
///     #890: a tag can be personal data, and <c>pc_dcb_tag_version</c> holds it as text, so a tombstone
///     takes the row once no event carries the tag. Doing that has to lose neither a conflict nor another
///     stream's counter. The value now comes from a sequence, which is what makes the delete safe.
/// </summary>
public class tombstone_tag_erasure_tests : OneOffConfigurationsContext
{
    private const string Schema = nameof(tombstone_tag_erasure_tests);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync(Action<StoreOptions>? configure = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("erase_course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<StudentId>("erase_student").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<ErasureEmail>("erase_email").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<ErasureNumber>("erase_number").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<ErasureAccount>("erase_account").ForAggregate<StudentCourseEnrollment>();
            configure?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<Guid> AppendAsync(params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.Append(streamId, Tagged(session, tags));
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private static IEvent Tagged(IDocumentSession session, params object[] tags)
    {
        var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        tagged.WithTag(tags);
        return tagged;
    }

    private async Task TombstoneAsync(Guid streamId)
    {
        await using var session = theStore.LightweightSession();
        session.Events.TombstoneStream(streamId);
        await session.SaveChangesAsync(Ct);
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private Task<long> VersionRowsAsync(object? tagValue = null)
        => ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0"
                       + (tagValue == null ? "" : $" AND tag_value = '{tagValue}'"));

    private Task DeleteVersionRowsAsync() => ExecuteAsync($"DELETE FROM [{Schema}].[pc_dcb_tag_version]");

    /// <summary>
    ///     The sequence in the issue, which lost the conflict when the row was simply deleted: the counter
    ///     came back at 1 and C's <c>WHERE version = 1</c> matched a row it had never seen.
    /// </summary>
    [Fact]
    public async Task the_stale_capture_in_the_issue_cannot_match_the_row_created_after_the_tombstone()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        var stream = await AppendAsync(course);

        // Boundary C reads the tag
        await using var c = theStore.LightweightSession();
        var boundaryC = await c.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        boundaryC.Events.Count.ShouldBe(1);

        await TombstoneAsync(stream);
        (await VersionRowsAsync(course.Value)).ShouldBe(0);

        // Boundary E reads nothing, and creates the row again
        await using (var e = theStore.LightweightSession())
        {
            var boundaryE = await e.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundaryE.Events.ShouldBeEmpty();
            boundaryE.AppendOne(Tagged(e, course));
            await e.SaveChangesAsync(Ct);
        }

        (await VersionRowsAsync(course.Value)).ShouldBe(1);

        boundaryC.AppendOne(Tagged(c, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => c.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task the_row_goes_with_the_last_stream_carrying_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var first = await AppendAsync(course);
        var second = await AppendAsync(course);

        await TombstoneAsync(first);
        (await VersionRowsAsync(course.Value)).ShouldBe(1);

        await TombstoneAsync(second);
        (await VersionRowsAsync(course.Value)).ShouldBe(0);
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_event_tag_erase_course]")).ShouldBe(0);
    }

    [Fact]
    public async Task only_the_tags_nothing_else_carries_go()
    {
        await ConfigureAsync();
        var shared = new CourseId(Guid.NewGuid());
        var own = new StudentId(Guid.NewGuid());
        var doomed = await AppendAsync(shared, own);
        await AppendAsync(shared);

        await TombstoneAsync(doomed);

        (await VersionRowsAsync(own.Value)).ShouldBe(0);
        (await VersionRowsAsync(shared.Value)).ShouldBe(1);
    }

    /// <summary>The row is shared by value, across every tag type that happens to stringify the same.</summary>
    [Fact]
    public async Task the_same_value_under_another_tag_type_is_another_row()
    {
        await ConfigureAsync();
        var value = Guid.NewGuid();
        var doomed = await AppendAsync(new CourseId(value));
        await AppendAsync(new StudentId(value));

        await TombstoneAsync(doomed);

        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_table = 'erase_course'"))
            .ShouldBe(0);
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_table = 'erase_student'"))
            .ShouldBe(1);
    }

    [Fact]
    public async Task a_boundary_over_a_tag_another_stream_still_carries_loses_and_the_retry_saves()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        var doomed = await AppendAsync(course);
        await AppendAsync(course);

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundary.Events.Count.ShouldBe(2);

            await TombstoneAsync(doomed);

            boundary.AppendOne(Tagged(session, course));
            await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        await using var retry = theStore.LightweightSession();
        var again = await retry.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        again.Events.Count.ShouldBe(1);
        again.AppendOne(Tagged(retry, course));
        await retry.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_tombstone_does_not_create_a_row()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);
        await DeleteVersionRowsAsync();

        await TombstoneAsync(doomed);

        (await VersionRowsAsync()).ShouldBe(0);
    }

    /// <summary>
    ///     With no row there is nothing for a tombstone to move or delete, and a capture of 0 would simply
    ///     create one at save, so nothing but the events themselves can say the read went stale.
    /// </summary>
    [Fact]
    public async Task a_boundary_that_captured_0_over_events_loses_when_they_are_tombstoned()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        var doomed = await AppendAsync(course);
        await DeleteVersionRowsAsync();

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            boundary.Events.Count.ShouldBe(1);

            await TombstoneAsync(doomed);

            boundary.AppendOne(Tagged(session, course));
            await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        // The failed save rolled back, so the value is not in the table by way of that save either
        (await VersionRowsAsync()).ShouldBe(0);

        await using var retry = theStore.LightweightSession();
        var again = await retry.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        again.Events.ShouldBeEmpty();
        again.AppendOne(Tagged(retry, course));
        await retry.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_boundary_that_captured_0_loses_when_another_stream_keeps_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);
        await AppendAsync(course);
        await DeleteVersionRowsAsync();

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.Count.ShouldBe(2);

        await TombstoneAsync(doomed);
        (await VersionRowsAsync()).ShouldBe(0);

        boundary.AppendOne(Tagged(session, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>The way a tag legitimately has events and no row, with no row deleted by hand.</summary>
    [Fact]
    public async Task a_tag_assigned_after_the_fact_has_no_row_and_still_conflicts()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(new CourseId(Guid.NewGuid()));

        await using (var assign = theStore.LightweightSession())
        {
            assign.Events.AssignTagWhere(x => x.StreamId == doomed, course);
            await assign.SaveChangesAsync(Ct);
        }

        (await VersionRowsAsync(course.Value)).ShouldBe(0);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);
        boundary.Events.Count.ShouldBe(1);

        await TombstoneAsync(doomed);

        boundary.AppendOne(Tagged(session, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task the_same_goes_for_a_boundary_fetched_in_a_batch()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);
        await DeleteVersionRowsAsync();

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var fetched = batch.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course));
        await batch.Execute(Ct);
        var boundary = await fetched;
        boundary.Events.Count.ShouldBe(1);

        await TombstoneAsync(doomed);

        boundary.AppendOne(Tagged(session, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_boundary_with_no_row_is_left_alone_while_its_events_are_all_there()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var other = await AppendAsync(new CourseId(Guid.NewGuid()));
        await AppendAsync(course);
        await DeleteVersionRowsAsync();

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        await TombstoneAsync(other);

        boundary.AppendOne(Tagged(session, course));
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>One tag with a row and history, one brand new: the mixture the recheck runs for.</summary>
    [Fact]
    public async Task a_boundary_over_a_new_tag_and_an_old_one_saves()
    {
        await ConfigureAsync();
        var known = new CourseId(Guid.NewGuid());
        var fresh = new CourseId(Guid.NewGuid());
        await AppendAsync(known);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(known).Or<CourseId>(fresh), Ct);
        boundary.Events.Count.ShouldBe(1);

        boundary.AppendOne(Tagged(session, fresh));
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>
    ///     A tombstone of the very stream it read, with a new tag beside it: the recheck is what notices,
    ///     because the new tag has no row and the old tag's row is gone.
    /// </summary>
    [Fact]
    public async Task a_boundary_over_a_new_tag_and_a_tombstoned_one_loses()
    {
        await ConfigureAsync();
        var known = new CourseId(Guid.NewGuid());
        var fresh = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(known);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(known).Or<CourseId>(fresh), Ct);

        await TombstoneAsync(doomed);

        boundary.AppendOne(Tagged(session, fresh));

        // Once rather than as an AggregateException: the row and the recheck agree about one boundary
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     The same row read twice, with a tombstone between. Asserting the lower of the two captures used
    ///     to cover every case, until a row could disappear: the lower one is now the "no row" of the
    ///     newer read, and that would have created the row and won.
    /// </summary>
    [Fact]
    public async Task two_boundaries_in_one_session_either_side_of_a_tombstone_lose()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        var doomed = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        var older = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        older.Events.Count.ShouldBe(1);

        await TombstoneAsync(doomed);

        var newer = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        newer.Events.ShouldBeEmpty();

        newer.AppendOne(Tagged(session, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task an_archived_stream_still_carries_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var archived = await AppendAsync(course);
        var doomed = await AppendAsync(course);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(archived);
            await session.SaveChangesAsync(Ct);
        }

        await TombstoneAsync(doomed);

        // Its tag row is still stored, and un-archiving would bring the value straight back
        (await VersionRowsAsync(course.Value)).ShouldBe(1);

        await TombstoneAsync(archived);
        (await VersionRowsAsync(course.Value)).ShouldBe(0);
    }

    /// <summary>Appends run ahead of queued operations, so the tombstone sees what the same save appended.</summary>
    [Fact]
    public async Task an_append_in_the_same_save_keeps_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        session.Events.TombstoneStream(doomed);
        session.Events.Append(Guid.NewGuid(), Tagged(session, course));
        await session.SaveChangesAsync(Ct);

        (await VersionRowsAsync(course.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task an_append_to_the_tombstoned_stream_in_the_same_save_does_not_keep_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        session.Events.Append(doomed, Tagged(session, course));
        session.Events.TombstoneStream(doomed);
        await session.SaveChangesAsync(Ct);

        (await VersionRowsAsync(course.Value)).ShouldBe(0);
    }

    [Fact]
    public async Task two_tombstones_in_one_save_erase_the_tag_they_share()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var first = await AppendAsync(course);
        var second = await AppendAsync(course);

        await using var session = theStore.LightweightSession();
        session.Events.TombstoneStream(first);
        session.Events.TombstoneStream(second);
        await session.SaveChangesAsync(Ct);

        (await VersionRowsAsync(course.Value)).ShouldBe(0);
    }

    [Fact]
    public async Task every_kind_of_tag_value_reaches_its_row()
    {
        await ConfigureAsync();
        var email = new ErasureEmail("ann@example.com");
        var number = new ErasureNumber(42);
        var account = new ErasureAccount(9_000_000_000);
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(email, number, account, course);
        (await VersionRowsAsync()).ShouldBe(4);

        await TombstoneAsync(doomed);

        (await VersionRowsAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task a_string_stream_is_erased_the_same_way()
    {
        await ConfigureAsync(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        var email = new ErasureEmail("ann@example.com");
        var shared = new ErasureEmail("shared@example.com");

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append("doomed", Tagged(session, email, shared));
            session.Events.Append("other", Tagged(session, shared));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream("doomed");
            await session.SaveChangesAsync(Ct);
        }

        (await VersionRowsAsync(email.Value)).ShouldBe(0);
        (await VersionRowsAsync(shared.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task archived_partitioning_has_no_cascade_and_is_erased_too()
    {
        await ConfigureAsync(opts => opts.EventGraph.UseArchivedStreamPartitioning = true);
        var course = new CourseId(Guid.NewGuid());
        var shared = new StudentId(Guid.NewGuid());
        var doomed = await AppendAsync(course, shared);
        await AppendAsync(shared);

        await TombstoneAsync(doomed);

        (await VersionRowsAsync(course.Value)).ShouldBe(0);
        (await VersionRowsAsync(shared.Value)).ShouldBe(1);
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_event_tag_erase_course]")).ShouldBe(0);
    }

    [Fact]
    public async Task a_tenant_only_loses_its_own_row()
    {
        await ConfigureAsync(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.Append(streamId, Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = "red" }))
        {
            red.Events.TombstoneStream(streamId);
            await red.SaveChangesAsync(Ct);
        }

        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tenant_id = 'red'"))
            .ShouldBe(0);
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tenant_id = 'blue'"))
            .ShouldBe(1);
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_event_tag_erase_course]")).ShouldBe(1);
    }

    /// <summary>An archive keeps its events, so it isn't erasure and carries on creating the row.</summary>
    [Fact]
    public async Task an_archive_still_moves_and_creates_rows()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var moved = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(course, moved);
        await ExecuteAsync(
            $"DELETE FROM [{Schema}].[pc_dcb_tag_version] WHERE tag_value = '{course.Value}'");
        var before = await ScalarAsync(
            $"SELECT version FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_value = '{moved.Value}'");

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(stream);
            await session.SaveChangesAsync(Ct);
        }

        (await VersionRowsAsync(course.Value)).ShouldBe(1);
        (await ScalarAsync($"SELECT version FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_value = '{moved.Value}'"))
            .ShouldBeGreaterThan(before);
    }

    /// <summary>
    ///     What a tombstone can't do, pinned because the docs say so: it finds tags through the stream's tag
    ///     rows, and compaction has already deleted those along with the events.
    /// </summary>
    [Fact]
    public async Task a_tag_only_compacted_events_carried_keeps_its_row()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(course);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(stream, new StudentEnrolled("Bob", "Math"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<StudentCourseEnrollment>(stream);
            await session.SaveChangesAsync(Ct);
        }

        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_event_tag_erase_course]")).ShouldBe(0);

        await TombstoneAsync(stream);

        (await VersionRowsAsync(course.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task a_stale_capture_cannot_match_a_row_recreated_after_a_clean()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        await AppendAsync(course);

        await using var stale = theStore.LightweightSession();
        var boundary = await stale.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);

        await theStore.Advanced.CleanAllEventDataAsync(Ct);
        await AppendAsync(course);

        boundary.AppendOne(Tagged(stale, course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => stale.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_value_is_never_given_out_twice()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var seen = new HashSet<long>();

        for (var i = 0; i < 3; i++)
        {
            var first = await AppendAsync(course);
            seen.Add(await ScalarAsync(
                $"SELECT version FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_value = '{course.Value}'")).ShouldBeTrue();

            var second = await AppendAsync(course);
            seen.Add(await ScalarAsync(
                $"SELECT version FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tag_value = '{course.Value}'")).ShouldBeTrue();

            await TombstoneAsync(first);
            await TombstoneAsync(second);
        }

        seen.ShouldAllBe(x => x >= DcbTagVersionTable.SequenceStart);
    }
}

/// <summary>A database can hold several tenants with no <c>tenant_id</c> on its tag tables.</summary>
public class tombstone_tag_erasure_across_tenants_in_a_database_tests : IAsyncLifetime
{
    private const string Schema = "tag_erasure_db";

    private static readonly string Database = ConnectionSource.Scoped("tag_erasure_db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropDatabaseAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE [{Database}];";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public ValueTask DisposeAsync() => new(DropDatabaseAsync());

    private static async Task DropDatabaseAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF DB_ID('{Database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{Database}];
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task another_tenants_stream_is_not_a_carrier_and_keeps_its_own_row()
    {
        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("erase_course").ForAggregate<StudentCourseEnrollment>();

            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("alpha", ConnectionSource.ConnectionStringFor(Database));
                tenancy.AddTenant("alpha-neighbour", ConnectionSource.ConnectionStringFor(Database));
            });
        });

        var course = new CourseId(Guid.NewGuid());
        var doomed = Guid.NewGuid();

        foreach (var (tenant, streamId) in new[] { ("alpha", doomed), ("alpha-neighbour", Guid.NewGuid()) })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
            tagged.WithTag(course);
            session.Events.Append(streamId, tagged);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.TombstoneStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT tenant_id FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0";
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe("alpha-neighbour");
    }
}

/// <summary>
///     Under <c>UseTenantPartitionedEvents</c> every tenant numbers its own events, so a carrier has to be
///     found by more than <c>seq_id</c>, and the same sequence numbers in two tenants prove it.
/// </summary>
[Collection("tenant-partitioning")]
public class tombstone_tag_erasure_under_tenant_partitioning_tests : IAsyncLifetime
{
    private const string Schema = "pt_tag_erasure";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task a_tag_is_erased_from_one_tenant_and_kept_by_another_and_by_another_stream()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("pt_erase_course").ForAggregate<StudentCourseEnrollment>();
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var course = new CourseId(Guid.NewGuid());

        async Task<Guid> AppendAsync(string tenant)
        {
            var stream = Guid.NewGuid();
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            var tagged = session.Events.BuildEvent(new StudentEnrolled(tenant, "Math"));
            tagged.WithTag(course);
            session.Events.StartStream(stream, tagged);
            await session.SaveChangesAsync(Ct);
            return stream;
        }

        async Task TombstoneAsync(string tenant, Guid stream)
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.TombstoneStream(stream);
            await session.SaveChangesAsync(Ct);
        }

        async Task<int> RowsAsync(string tenant)
        {
            var rows = await TestSchema.QueryAsync(
                $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE CHARINDEX(':', tag_table) = 0 AND tenant_id = '{tenant}'");
            return (int)rows[0][0];
        }

        var red = await AppendAsync("red");
        var redAgain = await AppendAsync("red");
        await AppendAsync("blue");

        await TombstoneAsync("red", red);
        (await RowsAsync("red")).ShouldBe(1);

        await TombstoneAsync("red", redAgain);
        (await RowsAsync("red")).ShouldBe(0);
        (await RowsAsync("blue")).ShouldBe(1);
    }
}
