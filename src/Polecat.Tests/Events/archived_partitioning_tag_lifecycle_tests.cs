using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Events;

/// <summary>
///     Under <c>UseArchivedStreamPartitioning</c> a tag table has no foreign key to <c>pc_events</c>, so
///     nothing cascades and whatever deletes events has to take their tag rows itself (#841). The flag
///     on the tag row is also never updated, which is why this runs the reads on top of a real archive.
/// </summary>
[Collection("integration")]
public class archived_partitioning_tag_lifecycle_tests : OneOffConfigurationsContext
{
    private const string Schema = "archived_partitioning_tag_lifecycle_tests";
    private const string TagTable = $"[{Schema}].[pc_event_tag_arch_life]";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        // The partition function and scheme carry no schema, so another test's pc_events still sitting
        // on them would block the migration here. See PartitionTestCleanup.
        await PartitionTestCleanup.DropArchivedPartitionObjectsAsync();
        await base.InitializeAsync();
    }

    private async Task ConfigureAsync(TenancyStyle tenancy = TenancyStyle.Single)
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = tenancy;
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("arch_life").ForAggregate<StudentCourseEnrollment>();
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static IEvent Tagged(IDocumentSession session, CourseId course, string name = "Alice")
    {
        var e = session.Events.BuildEvent(new StudentEnrolled(name, "Math"));
        e.WithTag(course);
        return e;
    }

    private async Task<int> TagRowsAsync(CourseId course)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {TagTable} WHERE value = @value";
        cmd.Parameters.AddWithValue("@value", course.Value);
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    [Fact]
    public async Task tombstoning_a_tagged_stream_takes_its_tag_rows()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(doomed, Tagged(session, course), Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(2);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(0);

        await using var query = theStore.LightweightSession();
        (await query.Events.EventsExistAsync(new EventTagQuery().Or<CourseId>(course), Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task tombstoning_leaves_a_neighbouring_stream_with_the_same_tag_alone()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(doomed, Tagged(session, course));
            session.Events.Append(Guid.NewGuid(), Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(1);
    }

    [Fact]
    public async Task tombstoning_an_archived_stream_takes_its_tag_rows()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var doomed = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(doomed, Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(0);
    }

    [Fact]
    public async Task tombstoning_is_scoped_to_the_tenant_under_conjoined_tenancy()
    {
        await ConfigureAsync(TenancyStyle.Conjoined);
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(new SessionOptions { TenantId = tenant });
            session.Events.Append(streamId, Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "red" }))
        {
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(1);

        await using var blue = theStore.LightweightSession(new SessionOptions { TenantId = "blue" });
        (await blue.Events.EventsExistAsync(new EventTagQuery().Or<CourseId>(course), Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task compacting_takes_the_tag_rows_of_the_folded_events()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<StudentCourseEnrollment>(streamId,
                Tagged(session, course, "one"), Tagged(session, course, "two"), Tagged(session, course, "three"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<StudentCourseEnrollment>(streamId);
            await session.SaveChangesAsync(Ct);
        }

        // The last event became the Compacted<T> snapshot and keeps its row
        (await TagRowsAsync(course)).ShouldBe(1);
    }

    [Fact]
    public async Task cleaning_all_event_data_takes_the_tag_rows()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(Guid.NewGuid(), Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        (await TagRowsAsync(course)).ShouldBe(0);
    }

    [Fact]
    public async Task the_tag_reads_go_by_the_event_and_not_the_flag_on_the_tag_row()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId, Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        // The premise: the event moved to the archived partition, and its tag row still says live
        await using (var conn = await OpenConnectionAsync())
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM {TagTable} WHERE value = @value AND is_archived = 0";
            cmd.Parameters.AddWithValue("@value", course.Value);
            ((int)(await cmd.ExecuteScalarAsync(Ct))!).ShouldBe(1);
        }

        var query = new EventTagQuery().Or<CourseId>(course);

        await using var reader = theStore.LightweightSession();
        (await reader.Events.EventsExistAsync(query, Ct)).ShouldBeFalse();
        (await reader.Events.QueryByTagsAsync(query, Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task assigning_a_tag_to_an_archived_event_that_already_has_it_writes_no_second_row()
    {
        await ConfigureAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId, Tagged(session, course));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.AssignTagWhere(e => e.StreamId == streamId, course);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync(course)).ShouldBe(1);
    }

    /// <summary>
    ///     The foreign key used to name <c>pc_streams(id, is_archived)</c>, which has no unique index:
    ///     only <c>pc_events</c> is partitioned, and <c>pc_streams</c>' key is still <c>(id)</c>. SQL
    ///     Server refuses that with Msg 1776, so a <c>[NaturalKey]</c> aggregate and archived
    ///     partitioning could not be provisioned together at all.
    /// </summary>
    [Fact]
    public async Task a_natural_key_table_can_be_provisioned()
    {
        ConfigureStore(opts =>
        {
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        (await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct)).ShouldBe(SchemaPatchDifference.None);

        // And the foreign key does its job: the lookup row goes with the stream
        var streamId = Guid.NewGuid();
        var number = new OrderNumber("K-1");

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(number, "A"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await using var again = theStore.LightweightSession();
        again.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(number, "B"));
        await again.SaveChangesAsync(Ct);
    }
}
