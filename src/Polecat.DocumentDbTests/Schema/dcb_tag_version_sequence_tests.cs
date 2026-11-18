using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Schema;
using Polecat.Tests.Events;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Schema;

/// <summary>
///     #890: every <c>pc_dcb_tag_version</c> value comes from a sequence. A store upgrading to that already
///     has rows, counted from 1, and they have to keep working.
/// </summary>
public class dcb_tag_version_sequence_tests : OneOffConfigurationsContext
{
    private const string Schema = nameof(dcb_tag_version_sequence_tests);
    private const string Sequence = $"[{Schema}].[{DcbTagVersionTable.SequenceName}]";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("sequenced_course").ForAggregate<StudentCourseEnrollment>();
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    private async Task AppendAsync(CourseId course)
    {
        await using var session = theStore.LightweightSession();
        var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>What a store from before #890 has: rows counted from 1, and no sequence.</summary>
    private async Task BecomeAnOlderStoreAsync(CourseId course, long version)
    {
        await ConfigureAsync();
        await AppendAsync(course);
        await ExecuteAsync($"""
            UPDATE [{Schema}].[pc_dcb_tag_version] SET version = {version};
            DROP SEQUENCE {Sequence};
            """);
    }

    [Fact]
    public async Task the_sequence_is_one_per_schema_and_starts_above_a_counted_row()
    {
        await ConfigureAsync();

        (await ScalarAsync($"""
            SELECT COUNT(*) FROM sys.sequences q
            JOIN sys.schemas s ON s.schema_id = q.schema_id
            WHERE s.name = '{Schema}'
            """)).ShouldBe(1);

        (await ScalarAsync($"""
            SELECT CONVERT(bigint, start_value) FROM sys.sequences
            WHERE object_id = OBJECT_ID('{Sequence}')
            """)).ShouldBe(DcbTagVersionTable.SequenceStart);
    }

    [Fact]
    public async Task a_store_with_no_tag_types_has_no_sequence()
    {
        await TestSchema.DropSequencesAsync(Schema);

        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await ScalarAsync($"SELECT COUNT(*) FROM sys.sequences WHERE object_id = OBJECT_ID('{Sequence}')"))
            .ShouldBe(0);
    }

    [Fact]
    public async Task completely_remove_all_takes_the_sequence()
    {
        await ConfigureAsync();

        await theStore.Advanced.CompletelyRemoveAllAsync(Ct);

        (await ScalarAsync($"SELECT COUNT(*) FROM sys.sequences WHERE object_id = OBJECT_ID('{Sequence}')"))
            .ShouldBe(0);
    }

    /// <summary>The table's shape doesn't change, so the sequence is the whole of the migration.</summary>
    [Fact]
    public async Task the_first_delta_on_an_upgraded_store_only_adds_the_sequence()
    {
        var course = new CourseId(Guid.NewGuid());
        await BecomeAnOlderStoreAsync(course, 7);

        var migration = await theDatabase.CreateMigrationAsync(Ct);

        migration.Difference.ShouldBe(SchemaPatchDifference.Create);
        var deltas = migration.Deltas.Where(x => x.Difference != SchemaPatchDifference.None).ToArray();
        deltas.Length.ShouldBe(1);
        deltas[0].SchemaObject.Identifier.Name.ShouldBe(DcbTagVersionTable.SequenceName);
        deltas[0].Difference.ShouldBe(SchemaPatchDifference.Create);

        await Should.ThrowAsync<Exception>(() => theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct));

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await theDatabase.CreateMigrationAsync(Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
        await theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct);

        // The row kept its value, because a boundary in flight across the upgrade may have captured it
        (await ScalarAsync($"SELECT version FROM [{Schema}].[pc_dcb_tag_version]")).ShouldBe(7);
    }

    [Theory]
    [InlineData(AutoCreate.All)]
    [InlineData(AutoCreate.CreateOrUpdate)]
    [InlineData(AutoCreate.CreateOnly)]
    public async Task a_session_on_an_upgraded_store_creates_the_sequence_for_itself(AutoCreate autoCreate)
    {
        var course = new CourseId(Guid.NewGuid());
        await BecomeAnOlderStoreAsync(course, 7);

        ConfigureStore(opts =>
        {
            opts.AutoCreateSchemaObjects = autoCreate;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("sequenced_course").ForAggregate<StudentCourseEnrollment>();
        });

        await AppendAsync(course);

        (await ScalarAsync($"SELECT version FROM [{Schema}].[pc_dcb_tag_version]"))
            .ShouldBeGreaterThanOrEqualTo(DcbTagVersionTable.SequenceStart);
    }

    /// <summary>A row that counted up from 1 is below everything the sequence hands out.</summary>
    [Fact]
    public async Task an_upgraded_row_moves_above_its_old_value_and_a_capture_of_it_still_works()
    {
        var course = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(course);
        await BecomeAnOlderStoreAsync(course, 7);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // Read the old value, then let a second boundary move the row past it
        await using var stale = theStore.LightweightSession();
        var staleBoundary = await stale.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
            var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
            tagged.WithTag(course);
            boundary.AppendOne(tagged);
            await session.SaveChangesAsync(Ct);
        }

        (await ScalarAsync($"SELECT version FROM [{Schema}].[pc_dcb_tag_version]"))
            .ShouldBeGreaterThanOrEqualTo(DcbTagVersionTable.SequenceStart);

        var late = stale.Events.BuildEvent(new StudentEnrolled("Cid", "Math"));
        late.WithTag(course);
        staleBoundary.AppendOne(late);
        await Should.ThrowAsync<DcbConcurrencyException>(() => stale.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     What <c>AutoCreate.None</c> is for, and it is the one way to be caught out by this: nothing
    ///     creates the sequence, and an append under a tag can't run without it.
    /// </summary>
    [Fact]
    public async Task auto_create_none_reports_the_missing_sequence()
    {
        var course = new CourseId(Guid.NewGuid());
        await BecomeAnOlderStoreAsync(course, 7);

        ConfigureStore(opts =>
        {
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("sequenced_course").ForAggregate<StudentCourseEnrollment>();
        });

        var ex = await Should.ThrowAsync<Exception>(() => AppendAsync(course));
        ex.Message.ShouldContain(DcbTagVersionTable.SequenceName);
    }
}
