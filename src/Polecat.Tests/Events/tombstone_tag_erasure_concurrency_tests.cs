using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #890: a tombstone deleting a <c>pc_dcb_tag_version</c> row while another stream adds the tag. A
///     save holds that row from before it inserts an event until it commits, so the tombstone has to take
///     the row before it asks whether another stream carries the tag.
/// </summary>
/// <remarks>
///     <para>
///         These hold one save open at its commit point and watch what the other does. They run against a
///         database of their own, once each way, because the two isolation levels answer differently: READ
///         COMMITTED blocks on what the other save is doing, and READ COMMITTED SNAPSHOT reads past it as
///         of the start of the statement, whatever the statement then waits for.
///     </para>
/// </remarks>
public abstract class tombstone_tag_erasure_concurrency_tests : IAsyncLifetime
{
    private const string Schema = "tag_erasure_race";

    private readonly bool _snapshotReads;
    private readonly string _database;
    private DocumentStore _store = null!;

    protected tombstone_tag_erasure_concurrency_tests(string database, bool snapshotReads)
    {
        _database = ConnectionSource.Scoped(database);
        _snapshotReads = snapshotReads;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropDatabaseAsync();

        await using (var conn = new SqlConnection(ConnectionSource.MasterConnectionString))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE [{_database}];";
            await cmd.ExecuteNonQueryAsync(Ct);

            if (_snapshotReads)
            {
                cmd.CommandText = $"ALTER DATABASE [{_database}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;";
                await cmd.ExecuteNonQueryAsync(Ct);
            }
        }

        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(_database);
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("erase_course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<StudentId>("erase_student").ForAggregate<StudentCourseEnrollment>();
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public async ValueTask DisposeAsync()
    {
        _store.Dispose();
        SqlConnection.ClearAllPools();
        await DropDatabaseAsync();
    }

    private async Task DropDatabaseAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF DB_ID('{_database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{_database}];
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Stops a save between the last of its SQL and its commit.</summary>
    private sealed class HoldAtCommit : ITransactionParticipant
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Reached => _reached.Task;

        public void Release() => _release.TrySetResult();

        public async Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
        {
            _reached.TrySetResult();
            await _release.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
        }
    }

    private static IEvent Tagged(IDocumentSession session, params object[] tags)
    {
        var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        tagged.WithTag(tags);
        return tagged;
    }

    private async Task<Guid> AppendAsync(params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.Append(streamId, Tagged(session, tags));
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task<long> VersionRowsAsync(Guid tagValue)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(_database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE tag_value = '{tagValue}'";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    /// <summary>The other save holds what this one needs, so it can't have finished.</summary>
    private static async Task ShouldStillBeWaitingAsync(Task blocked)
    {
        await Task.Delay(750, Ct);
        blocked.IsCompleted.ShouldBeFalse();
    }

    [Fact]
    public async Task a_tombstone_behind_an_append_in_flight_sees_the_append_and_keeps_the_row()
    {
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        var hold = new HoldAtCommit();
        await using var appending = _store.LightweightSession();
        appending.AddTransactionParticipant(hold);
        appending.Events.Append(Guid.NewGuid(), Tagged(appending, course));
        var append = appending.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        await using var tombstoning = _store.LightweightSession();
        tombstoning.Events.TombstoneStream(doomed);
        var tombstone = tombstoning.SaveChangesAsync(Ct);

        await ShouldStillBeWaitingAsync(tombstone);

        hold.Release();
        await append;
        await tombstone;

        (await VersionRowsAsync(course.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task an_append_behind_a_tombstone_creates_the_row_again()
    {
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        var hold = new HoldAtCommit();
        await using var tombstoning = _store.LightweightSession();
        tombstoning.AddTransactionParticipant(hold);
        tombstoning.Events.TombstoneStream(doomed);
        var tombstone = tombstoning.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        await using var appending = _store.LightweightSession();
        appending.Events.Append(Guid.NewGuid(), Tagged(appending, course));
        var append = appending.SaveChangesAsync(Ct);

        await ShouldStillBeWaitingAsync(append);

        hold.Release();
        await tombstone;
        await append;

        (await VersionRowsAsync(course.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task a_boundary_behind_a_tombstone_loses()
    {
        var course = new CourseId(Guid.NewGuid());
        var doomed = await AppendAsync(course);

        await using var bounded = _store.LightweightSession();
        var boundary = await bounded.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        var hold = new HoldAtCommit();
        await using var tombstoning = _store.LightweightSession();
        tombstoning.AddTransactionParticipant(hold);
        tombstoning.Events.TombstoneStream(doomed);
        var tombstone = tombstoning.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        boundary.AppendOne(Tagged(bounded, course));
        var save = bounded.SaveChangesAsync(Ct);

        await ShouldStillBeWaitingAsync(save);

        hold.Release();
        await tombstone;
        await Should.ThrowAsync<DcbConcurrencyException>(() => save);

        (await VersionRowsAsync(course.Value)).ShouldBe(0);
    }

    /// <summary>
    ///     Every stream ends up tombstoned, so whatever order they landed in, nothing may be left. A row
    ///     left behind is a tombstone that gave way to a stream it then outlived.
    /// </summary>
    [Fact]
    public async Task appends_and_tombstones_racing_over_shared_tags_leave_the_table_right()
    {
        var courses = Enumerable.Range(0, 3).Select(_ => new CourseId(Guid.NewGuid())).ToArray();
        var students = Enumerable.Range(0, 3).Select(_ => new StudentId(Guid.NewGuid())).ToArray();

        var workers = Enumerable.Range(0, 6).Select(worker => Task.Run(async () =>
        {
            var random = new Random(worker);

            for (var round = 0; round < 15; round++)
            {
                var stream = await AppendAsync(courses[random.Next(3)], students[random.Next(3)]);

                await using var session = _store.LightweightSession();
                session.Events.TombstoneStream(stream);
                await session.SaveChangesAsync(Ct);
            }
        }, Ct)).ToArray();

        await Task.WhenAll(workers);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(_database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version]";
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe(0);

        foreach (var course in courses) await AppendAsync(course);
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version]";
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe(courses.Length);
    }
}

public class tombstone_tag_erasure_at_read_committed_tests()
    : tombstone_tag_erasure_concurrency_tests("tag_erasure_locking", false);

public class tombstone_tag_erasure_at_read_committed_snapshot_tests()
    : tombstone_tag_erasure_concurrency_tests("tag_erasure_rcsi", true);
