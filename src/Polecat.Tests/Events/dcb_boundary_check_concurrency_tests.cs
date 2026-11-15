using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #972: a boundary's check of a row it read and does not append under. It locks the row and compares it,
///     and leaves the version alone. These hold one save open at its commit point and watch what the other
///     does, to pin what the lock the check takes has to do.
/// </summary>
/// <remarks>
///     <para>
///         They run against a database of their own, once each way. Under READ COMMITTED SNAPSHOT a plain read
///         neither waits for an uncommitted write nor sees it, so a check that compared without a lock would
///         read past a producer in flight and commit through it. The check reads with <c>UPDLOCK</c>, which is
///         a locking read under either isolation level.
///     </para>
///     <para>
///         Every boundary here appends an event carrying only a <c>StudentId</c>, so its <c>CourseId</c> row
///         is checked and never bumped.
///     </para>
/// </remarks>
public abstract class dcb_boundary_check_concurrency_tests : IAsyncLifetime
{
    private const string Schema = "dcb_check_race";

    private readonly bool _snapshotReads;
    private readonly string _database;
    private DocumentStore _store = null!;

    protected dcb_boundary_check_concurrency_tests(string database, bool snapshotReads)
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

            // A retried deadlock commits, and would pass a fact that should have failed
            opts.ConfigurePolly(_ => { });

            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.AddEventType<AssignmentSubmitted>();
            opts.Events.RegisterTagType<CourseId>("check_course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<StudentId>("check_student").ForAggregate<StudentCourseEnrollment>();
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

    private static IEvent Tagged(IDocumentSession session, object data, params object[] tags)
    {
        var tagged = session.Events.BuildEvent(data);
        tagged.WithTag(tags);
        return tagged;
    }

    private async Task AppendAsync(CourseId course)
    {
        await using var session = _store.LightweightSession();
        session.Events.Append(Guid.NewGuid(), Tagged(session, new StudentEnrolled("Ann", "Math"), course));
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>A session whose boundary read the course, and which appends nothing carrying it.</summary>
    private async Task<IDocumentSession> CheckingAsync(CourseId course)
    {
        var session = _store.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or(course), Ct);
        boundary.AppendOne(Tagged(session, new AssignmentSubmitted("essay", 90), new StudentId(Guid.NewGuid())));
        return session;
    }

    private static async Task ShouldStillBeWaitingAsync(Task blocked)
    {
        await Task.Delay(750, Ct);
        blocked.IsCompleted.ShouldBeFalse();
    }

    /// <summary>
    ///     The producer holds the row exclusively, uncommitted. The check has to wait for it and then see its
    ///     version, which a non-locking read under READ COMMITTED SNAPSHOT would not.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_check_behind_an_append_in_flight_waits_for_it_and_loses(bool seeded)
    {
        var course = new CourseId(Guid.NewGuid());
        if (seeded) await AppendAsync(course);

        await using var checking = await CheckingAsync(course);

        var hold = new HoldAtCommit();
        await using var appending = _store.LightweightSession();
        appending.AddTransactionParticipant(hold);
        appending.Events.Append(Guid.NewGuid(), Tagged(appending, new StudentEnrolled("Bob", "Math"), course));
        var append = appending.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        var check = checking.SaveChangesAsync(Ct);
        await ShouldStillBeWaitingAsync(check);

        hold.Release();
        await append;
        await Should.ThrowAsync<DcbConcurrencyException>(() => check);
    }

    /// <summary>
    ///     The other way round. The check holds the row from before the producer reaches it, so the producer
    ///     waits, and both commit: the check's save came first, so the append is not a conflict for it. With
    ///     <paramref name="seeded" /> false there was no row to hold, and the check creates one to hold
    ///     (#4591's hazard), or the producer would commit through it.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_append_behind_a_check_in_flight_waits_for_it_and_both_commit(bool seeded)
    {
        var course = new CourseId(Guid.NewGuid());
        if (seeded) await AppendAsync(course);

        await using var checking = await CheckingAsync(course);
        var hold = new HoldAtCommit();
        checking.AddTransactionParticipant(hold);
        var check = checking.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        await using var appending = _store.LightweightSession();
        appending.Events.Append(Guid.NewGuid(), Tagged(appending, new StudentEnrolled("Bob", "Math"), course));
        var append = appending.SaveChangesAsync(Ct);
        await ShouldStillBeWaitingAsync(append);

        hold.Release();
        await check;
        await append;
    }

    /// <summary>Two checks of one row still queue on it, and neither is a conflict for the other.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task two_checks_of_one_row_queue_and_both_commit(bool seeded)
    {
        var course = new CourseId(Guid.NewGuid());
        if (seeded) await AppendAsync(course);

        await using var first = await CheckingAsync(course);
        await using var second = await CheckingAsync(course);

        var hold = new HoldAtCommit();
        first.AddTransactionParticipant(hold);
        var firstSave = first.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        var secondSave = second.SaveChangesAsync(Ct);
        await ShouldStillBeWaitingAsync(secondSave);

        hold.Release();
        await firstSave;
        await secondSave;
    }

    /// <summary>
    ///     A check takes an update lock, not an exclusive one, so a boundary READING the row it is checking is
    ///     not held up for the length of the other save. Under plain READ COMMITTED an exclusive lock would
    ///     block that read.
    /// </summary>
    [Fact]
    public async Task a_boundary_read_does_not_wait_for_a_check_in_flight()
    {
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(course);

        await using var checking = await CheckingAsync(course);
        var hold = new HoldAtCommit();
        checking.AddTransactionParticipant(hold);
        var check = checking.SaveChangesAsync(Ct);
        await hold.Reached.WaitAsync(Ct);

        try
        {
            await using var reading = _store.LightweightSession();
            var read = reading.Events.FetchForWritingByTags<StudentCourseEnrollment>(new EventTagQuery().Or(course), Ct);
            var boundary = await read.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            boundary.Events.Count.ShouldBe(1);
        }
        finally
        {
            hold.Release();
            await check;
        }
    }
}

public class dcb_boundary_check_at_read_committed_tests()
    : dcb_boundary_check_concurrency_tests("dcb_check_locking", false);

public class dcb_boundary_check_at_read_committed_snapshot_tests()
    : dcb_boundary_check_concurrency_tests("dcb_check_rcsi", true);
