using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     jasperfx#1184: a <c>[BoundaryAggregate]</c> that also has an <c>Id</c> member. The generator gave it
///     only the Guid-keyed evolver, which <c>FetchForWritingByTags</c> did not look up.
/// </summary>
[BoundaryAggregate]
public partial class ScopeCourseRoster
{
    public Guid Id { get; set; }
    public int Enrolled { get; private set; }

    public void Apply(StudentEnrolled e) => Enrolled++;
    public void Apply(StudentDropped e) => Enrolled--;
}

/// <summary>
///     #972 and #973 (marten#5887), the parts the shared <c>DcbAppendConditionCompliance</c> suite leaves to
///     the store: the version rows a check does and does not move, the per-event-type rows, every
///     <see cref="DcbConflictScope" />, the rollout between them, and what archiving and tombstoning do to the
///     per-event-type rows.
/// </summary>
public class dcb_conflict_scope_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string Schema => theStore.Options.DatabaseSchemaName;

    private void Configure(StoreOptions opts, DcbConflictScope? scope)
    {
        opts.Events.AddEventType<StudentEnrolled>();
        opts.Events.AddEventType<StudentDropped>();
        opts.Events.AddEventType<AssignmentSubmitted>();
        opts.Events.RegisterTagType<CourseId>("scope_course").ForAggregate<StudentCourseEnrollment>();
        opts.Events.RegisterTagType<StudentId>("scope_student").ForAggregate<StudentCourseEnrollment>();
        if (scope.HasValue) opts.Events.DcbConflictScope = scope.Value;
    }

    private async Task ConfigureAsync(DcbConflictScope? scope = null)
    {
        ConfigureStore(opts => Configure(opts, scope));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    /// <summary>Another node on the same schema, running another scope.</summary>
    private DocumentStore OtherNode(DcbConflictScope scope)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            Configure(opts, scope);
        });

        Disposables.Add(store);
        return store;
    }

    private static IEvent Tagged(IDocumentSession session, object data, params object[] tags)
    {
        var e = session.Events.BuildEvent(data);
        e.WithTag(tags);
        return e;
    }

    private static Task<Guid> AppendAsync(DocumentStore store, object data, params object[] tags)
        => AppendToAsync(store, Guid.NewGuid(), data, tags);

    private static async Task<Guid> AppendToAsync(DocumentStore store, Guid stream, object data,
        params object[] tags)
    {
        await using var session = store.LightweightSession();
        session.Events.Append(stream, Tagged(session, data, tags));
        await session.SaveChangesAsync(Ct);
        return stream;
    }

    private string TypeKey<T>(string suffix)
        => DcbTagVersionKeys.ForEventType(suffix, theStore.Options.EventGraph.EventMappingFor(typeof(T)).EventTypeName);

    private async Task<long?> VersionAsync(string tagTable, Guid value)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT version FROM [{Schema}].[pc_dcb_tag_version] WHERE tag_table = '{tagTable}' AND tag_value = '{value}'";
        var result = await cmd.ExecuteScalarAsync(Ct);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    private async Task<int> RowsAsync(string? where = null)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version]" + (where == null ? "" : $" WHERE {where}");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(Ct));
    }

    private static EventTagQuery Enrollments(CourseId course)
        => new EventTagQuery().Or(course).AndEventsOfType<StudentEnrolled>();

    [Fact]
    public void the_default_is_the_dcb_specifications_append_condition()
    {
        new StoreOptions().Events.DcbConflictScope.ShouldBe(DcbConflictScope.TagValueAndEventType);
    }

    /// <summary>
    ///     #972: a boundary that reads a tag value and appends an event not carrying it leaves that value's
    ///     version where it was. A row nothing had created is created at 0, which is what "no row" captures
    ///     as, so the lock the check needs exists without making the row look written.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_check_leaves_the_version_of_a_row_it_only_read_alone(bool seeded)
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        if (seeded) await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);

        var before = await VersionAsync("scope_course", course.Value);
        if (seeded) before.ShouldNotBeNull(); else before.ShouldBeNull();

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
                new EventTagQuery().Or(course), Ct);
            boundary.AppendOne(Tagged(session, new AssignmentSubmitted("essay", 90), new StudentId(Guid.NewGuid())));
            await session.SaveChangesAsync(Ct);
        }

        (await VersionAsync("scope_course", course.Value)).ShouldBe(before ?? 0);
    }

    /// <summary>The producer side is unchanged: a boundary appending under its own tag still moves it.</summary>
    [Fact]
    public async Task a_boundary_appending_under_what_it_read_moves_the_version()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        var before = await VersionAsync("scope_course", course.Value);

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
                new EventTagQuery().Or(course), Ct);
            boundary.AppendOne(Tagged(session, new StudentEnrolled("Bob", "Math"), course));
            await session.SaveChangesAsync(Ct);
        }

        (await VersionAsync("scope_course", course.Value)).ShouldNotBe(before);
    }

    /// <summary>#973, per scope: only the DCB specification's condition lets the unselected type through.</summary>
    [Theory]
    [InlineData(DcbConflictScope.TagValue, true)]
    [InlineData(DcbConflictScope.TagValueRecordingEventTypes, true)]
    [InlineData(DcbConflictScope.TagValueAndEventType, false)]
    public async Task an_event_of_a_type_the_condition_does_not_name(DcbConflictScope scope, bool conflicts)
    {
        await ConfigureAsync(scope);
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(Enrollments(course), Ct);
        boundary.Events.Count.ShouldBe(1);

        await AppendAsync(theStore, new StudentDropped("moved"), course);

        boundary.AppendOne(Tagged(session, new StudentEnrolled("Bob", "Math"), course));
        if (conflicts)
        {
            await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }
        else
        {
            await Should.NotThrowAsync(() => session.SaveChangesAsync(Ct));
        }
    }

    [Theory]
    [InlineData(DcbConflictScope.TagValue)]
    [InlineData(DcbConflictScope.TagValueRecordingEventTypes)]
    [InlineData(DcbConflictScope.TagValueAndEventType)]
    public async Task an_event_of_a_type_the_condition_names_conflicts_under_every_scope(DcbConflictScope scope)
    {
        await ConfigureAsync(scope);
        var course = new CourseId(Guid.NewGuid());

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(Enrollments(course), Ct);

        await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);

        boundary.AppendOne(Tagged(session, new StudentEnrolled("Bob", "Math"), course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Theory]
    [InlineData(DcbConflictScope.TagValue, 0)]
    [InlineData(DcbConflictScope.TagValueRecordingEventTypes, 2)]
    [InlineData(DcbConflictScope.TagValueAndEventType, 2)]
    public async Task only_the_recording_scopes_keep_a_version_per_event_type(DcbConflictScope scope, int byType)
    {
        await ConfigureAsync(scope);
        var course = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        await AppendToAsync(theStore, stream, new StudentDropped("moved"), course);

        (await RowsAsync("CHARINDEX(':', tag_table) = 0")).ShouldBe(1);
        (await RowsAsync("CHARINDEX(':', tag_table) > 0")).ShouldBe(byType);

        if (byType > 0)
        {
            (await VersionAsync(TypeKey<StudentEnrolled>("scope_course"), course.Value)).ShouldNotBeNull();
            (await VersionAsync(TypeKey<StudentDropped>("scope_course"), course.Value)).ShouldNotBeNull();
        }
    }

    /// <summary>The rollout's second step: every node now records event types, and one starts checking them.</summary>
    [Fact]
    public async Task a_node_checking_event_types_sees_appends_from_a_node_recording_them()
    {
        await ConfigureAsync(DcbConflictScope.TagValueAndEventType);
        var recording = OtherNode(DcbConflictScope.TagValueRecordingEventTypes);
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(recording, new StudentEnrolled("Ann", "Math"), course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(Enrollments(course), Ct);

        await AppendAsync(recording, new StudentEnrolled("Bob", "Math"), course);

        boundary.AppendOne(Tagged(session, new StudentEnrolled("Cat", "Math"), course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     Pins the hazard the rollout step exists for, so it is not mistaken for a bug: a node still on
    ///     <see cref="DcbConflictScope.TagValue" /> (or on Polecat 5) moves no per-event-type version, so a node
    ///     checking them cannot see its appends.
    /// </summary>
    [Fact]
    public async Task skipping_the_rollout_step_misses_a_conflict()
    {
        await ConfigureAsync(DcbConflictScope.TagValueAndEventType);
        var legacy = OtherNode(DcbConflictScope.TagValue);
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(Enrollments(course), Ct);

        await AppendAsync(legacy, new StudentEnrolled("Bob", "Math"), course);

        boundary.AppendOne(Tagged(session, new StudentEnrolled("Cat", "Math"), course));
        await Should.NotThrowAsync(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     Archiving changes what a boundary reads without appending, so it moves the versions itself (#841).
    ///     A boundary checked by event type is checked against the per-event-type row, so that row has to move
    ///     too.
    /// </summary>
    [Fact]
    public async Task an_archive_moves_the_per_event_type_version()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(Enrollments(course), Ct);
        boundary.Events.Count.ShouldBe(1);

        await using (var archiver = theStore.LightweightSession())
        {
            archiver.Events.ArchiveStream(stream);
            await archiver.SaveChangesAsync(Ct);
        }

        boundary.AppendOne(Tagged(session, new StudentEnrolled("Bob", "Math"), course));
        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_tombstone_erases_the_per_event_type_rows_with_the_tag()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        await AppendToAsync(theStore, stream, new StudentDropped("moved"), course);
        (await RowsAsync()).ShouldBe(3);

        await TombstoneAsync(theStore, stream);

        (await RowsAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task a_per_event_type_row_stays_while_another_stream_carries_the_tag_with_that_type()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        await AppendToAsync(theStore, doomed, new StudentDropped("moved"), course);
        await AppendAsync(theStore, new StudentEnrolled("Bob", "Math"), course);

        await TombstoneAsync(theStore, doomed);

        (await VersionAsync("scope_course", course.Value)).ShouldNotBeNull();
        (await VersionAsync(TypeKey<StudentEnrolled>("scope_course"), course.Value)).ShouldNotBeNull();
        (await VersionAsync(TypeKey<StudentDropped>("scope_course"), course.Value)).ShouldBeNull();
    }

    /// <summary>
    ///     The tag value is the application's data whatever the scope, and a store that ran under another
    ///     scope earlier still holds the rows it wrote then.
    /// </summary>
    [Fact]
    public async Task a_tombstone_under_the_tag_value_scope_still_erases_per_event_type_rows()
    {
        await ConfigureAsync(DcbConflictScope.TagValueAndEventType);
        var course = new CourseId(Guid.NewGuid());
        var stream = await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        (await RowsAsync("CHARINDEX(':', tag_table) > 0")).ShouldBe(1);

        await TombstoneAsync(OtherNode(DcbConflictScope.TagValue), stream);

        (await RowsAsync()).ShouldBe(0);
    }

    [Fact]
    public void a_per_event_type_key_too_long_for_the_column_is_refused()
    {
        var name = new string('e', DcbTagVersionKeys.MaxLength);

        var ex = Should.Throw<InvalidOperationException>(() => DcbTagVersionKeys.ForEventType("course", name));
        ex.Message.ShouldContain(nameof(DcbConflictScope.TagValue));

        DcbTagVersionKeys.ForEventType("course", new string('e', DcbTagVersionKeys.MaxLength - "course:".Length))
            .Length.ShouldBe(DcbTagVersionKeys.MaxLength);
    }

    /// <summary>jasperfx#1184, fixed by JasperFx 3.2.0.</summary>
    [Fact]
    public async Task a_boundary_aggregate_with_an_id_member_can_be_fetched_for_writing()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(theStore, new StudentEnrolled("Ann", "Math"), course);
        await AppendAsync(theStore, new StudentEnrolled("Bob", "Math"), course);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<ScopeCourseRoster>(
            new EventTagQuery().Or(course), Ct);

        boundary.Aggregate.ShouldNotBeNull().Enrolled.ShouldBe(2);
        boundary.AppendOne(Tagged(session, new StudentDropped("moved"), course));
        await session.SaveChangesAsync(Ct);
    }

    private static async Task TombstoneAsync(DocumentStore store, Guid stream)
    {
        await using var session = store.LightweightSession();
        session.Events.TombstoneStream(stream);
        await session.SaveChangesAsync(Ct);
    }
}
