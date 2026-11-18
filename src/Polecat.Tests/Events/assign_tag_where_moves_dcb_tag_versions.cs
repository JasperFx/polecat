using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1070 (port of marten#6078): <c>AssignTagWhere</c> changes which events a boundary over the tag
///     reads without appending anything, so it has to move the tag's <c>pc_dcb_tag_version</c> rows the way an
///     archive does (#841). Every conflict case is a boundary that reads, the assignment, and a save that has to
///     fail.
/// </summary>
public class assign_tag_where_moves_dcb_tag_versions : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string Schema => theStore.Options.DatabaseSchemaName;

    private async Task ConfigureAsync(DcbConflictScope scope = DcbConflictScope.TagValueAndEventType,
        TenancyStyle tenancy = TenancyStyle.Single)
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.AddEventType<StudentDropped>();
            opts.Events.RegisterTagType<CourseId>("assign_course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.DcbConflictScope = scope;
            opts.Events.TenancyStyle = tenancy;
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static IEvent Tagged(IDocumentSession session, object data, params object[] tags)
    {
        var e = session.Events.BuildEvent(data);
        e.WithTag(tags);
        return e;
    }

    private async Task<Guid> AppendAsync(object data, string? tenantId = null, params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = tenantId == null ? theStore.LightweightSession() : theStore.LightweightSession(tenantId);
        session.Events.Append(streamId, tags.Length == 0 ? data : Tagged(session, data, tags));
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task AssignAsync(Guid streamId, CourseId course, string? tenantId = null)
    {
        await using var session = tenantId == null ? theStore.LightweightSession() : theStore.LightweightSession(tenantId);
        session.Events.AssignTagWhere(x => x.StreamId == streamId, course);
        await session.SaveChangesAsync(Ct);
    }

    private async Task<int> RowsAsync(Guid value, string? tenantId = null)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE tag_value = '{value}'"
            + (tenantId == null ? "" : $" AND tenant_id = '{tenantId}'");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(Ct));
    }

    private string TypeKey<T>()
        => DcbTagVersionKeys.ForEventType("assign_course", theStore.Options.EventGraph.EventMappingFor(typeof(T)).EventTypeName);

    private async Task<string[]> KeysAsync(Guid value)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT tag_table FROM [{Schema}].[pc_dcb_tag_version] WHERE tag_value = '{value}' ORDER BY tag_table";
        var keys = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) keys.Add(reader.GetString(0));
        return keys.ToArray();
    }

    [Theory]
    [InlineData(DcbConflictScope.TagValue, false)]
    [InlineData(DcbConflictScope.TagValueAndEventType, false)]
    [InlineData(DcbConflictScope.TagValueAndEventType, true)]
    public async Task a_boundary_conflicts_with_a_tag_assigned_to_events_it_did_not_read(DcbConflictScope scope,
        bool byEventType)
    {
        await ConfigureAsync(scope);
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(new StudentEnrolled("Ann", "Math"), null, course);
        var untagged = await AppendAsync(new StudentEnrolled("Bob", "Math"));

        var query = byEventType
            ? new EventTagQuery().Or(course).AndEventsOfType<StudentEnrolled>()
            : new EventTagQuery().Or(course);

        await using var decision = theStore.LightweightSession();
        var boundary = await decision.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        boundary.Events.Count.ShouldBe(1);

        await AssignAsync(untagged, course);

        boundary.AppendOne(Tagged(decision, new StudentEnrolled("Cid", "Math"), course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => decision.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_boundary_over_a_tag_nothing_carried_conflicts_with_its_first_assignment()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var untagged = await AppendAsync(new StudentEnrolled("Bob", "Math"));

        await using var decision = theStore.LightweightSession();
        var boundary = await decision.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or(course), Ct);
        boundary.Events.ShouldBeEmpty();

        await AssignAsync(untagged, course);

        boundary.AppendOne(Tagged(decision, new StudentEnrolled("Cid", "Math"), course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => decision.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_retry_after_the_conflict_reads_the_assigned_events_and_saves()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var untagged = await AppendAsync(new StudentEnrolled("Bob", "Math"));
        var query = new EventTagQuery().Or(course);

        await using (var decision = theStore.LightweightSession())
        {
            var boundary = await decision.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            await AssignAsync(untagged, course);
            boundary.AppendOne(Tagged(decision, new StudentEnrolled("Cid", "Math"), course));
            await Should.ThrowAsync<DcbConcurrencyException>(() => decision.SaveChangesAsync(Ct));
        }

        await using var retry = theStore.LightweightSession();
        var again = await retry.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        again.Events.Count.ShouldBe(1);
        again.AppendOne(Tagged(retry, new StudentEnrolled("Cid", "Math"), course));
        await retry.SaveChangesAsync(Ct);
    }

    [Theory]
    [InlineData(DcbConflictScope.TagValue)]
    [InlineData(DcbConflictScope.TagValueRecordingEventTypes)]
    public async Task the_assignment_creates_the_rows_the_scope_keeps(DcbConflictScope scope)
    {
        await ConfigureAsync(scope);
        var course = new CourseId(Guid.NewGuid());
        var stream = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(stream, new StudentEnrolled("Bob", "Math"), new StudentDropped("moved"));
            await session.SaveChangesAsync(Ct);
        }

        await AssignAsync(stream, course);

        string[] expected = scope == DcbConflictScope.TagValue
            ? ["assign_course"]
            : ["assign_course", TypeKey<StudentEnrolled>(), TypeKey<StudentDropped>()];
        (await KeysAsync(course.Value)).ShouldBe(expected, ignoreOrder: true);
    }

    [Fact]
    public async Task an_assignment_that_matches_nothing_writes_no_row()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());

        await AssignAsync(Guid.NewGuid(), course);

        (await RowsAsync(course.Value)).ShouldBe(0);
    }

    [Fact]
    public async Task an_assignment_that_matches_only_another_tenants_events_writes_no_row()
    {
        await ConfigureAsync(tenancy: TenancyStyle.Conjoined);
        var course = new CourseId(Guid.NewGuid());
        var theirs = await AppendAsync(new StudentEnrolled("Bob", "Math"), "two");

        await AssignAsync(theirs, course, "one");

        (await RowsAsync(course.Value)).ShouldBe(0);

        // and in the tenant whose events it does match, the row is that tenant's
        await AssignAsync(theirs, course, "two");
        (await RowsAsync(course.Value, "one")).ShouldBe(0);
        (await RowsAsync(course.Value, "two")).ShouldBeGreaterThan(0);
    }
}
