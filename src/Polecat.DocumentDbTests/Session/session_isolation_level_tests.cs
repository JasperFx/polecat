using System.Data;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Session;

/// <summary>
///     #844, the mirror image of marten#5586: <see cref="SessionOptions.IsolationLevel" /> was read by
///     <c>OpenSessionAsync</c> alone, and even there only for the first transaction.
/// </summary>
/// <remarks>
///     The facts read <c>sys.dm_exec_sessions.transaction_isolation_level</c> from inside the save's own
///     transaction. <c>open_serializable_session_tests</c> only ever asserted that rows land, which is
///     how a session could report success at a level it never ran at.
/// </remarks>
public class session_isolation_level_tests : OneOffConfigurationsContext
{
    internal const int ReadCommittedLevel = 2;
    private const int SnapshotUpdateConflict = 3960;

    private const string ReadLevelSql =
        "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string ProbeTable => $"[{theStore.Options.DatabaseSchemaName}].[isolation_probe]";

    public class Note
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class VersionedNote : JasperFx.Metadata.IVersioned
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public Guid Version { get; set; }
    }

    /// <summary>
    ///     On by default in <c>master</c>, where CI runs, and off in a catalog made for a parallel worker.
    ///     See <see cref="SnapshotIsolation" />.
    /// </summary>
    internal static Task AllowSnapshotIsolationAsync(CancellationToken token) => SnapshotIsolation.AllowAsync(token);

    internal static Task AllowSnapshotIsolationAsync(string connectionString, CancellationToken token)
        => SnapshotIsolation.AllowAsync(connectionString, token);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await AllowSnapshotIsolationAsync(Ct);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE {ProbeTable} (id int IDENTITY PRIMARY KEY, lvl int NOT NULL);";
        await cmd.ExecuteNonQueryAsync(Ct);

        // A Snapshot transaction cannot see a table created after it began, so make this one's now.
        await using var seed = theStore.LightweightSession();
        seed.Store(new Note { Id = Guid.NewGuid() });
        seed.Store(new VersionedNote { Id = Guid.NewGuid() });
        await seed.SaveChangesAsync(Ct);
    }

    public static TheoryData<string, IsolationLevel, int> SessionsAndLevels()
    {
        var data = new TheoryData<string, IsolationLevel, int>();
        foreach (var kind in SessionKinds)
        {
            data.Add(kind, IsolationLevel.ReadUncommitted, 1);
            data.Add(kind, IsolationLevel.ReadCommitted, ReadCommittedLevel);
            data.Add(kind, IsolationLevel.RepeatableRead, 3);
            data.Add(kind, IsolationLevel.Serializable, 4);
            data.Add(kind, IsolationLevel.Snapshot, 5);
        }

        return data;
    }

    private static readonly string[] SessionKinds =
    [
        "lightweight",
        "identity",
        "open_session",
        "open_session_identity",
        "open_session_async",
        "open_session_async_identity",
        "without_held_connection"
    ];

    public static TheoryData<string> Kinds() => new(SessionKinds);

    private async Task<IDocumentSession> OpenAsync(string kind, IsolationLevel level)
    {
        var options = new SessionOptions { IsolationLevel = level };
        var identity = new SessionOptions { IsolationLevel = level, Tracking = DocumentTracking.IdentityOnly };

        return kind switch
        {
            "lightweight" => theStore.LightweightSession(options),
            "identity" => theStore.IdentitySession(options),
            "open_session" => theStore.OpenSession(options),
            "open_session_identity" => theStore.OpenSession(identity),
            "open_session_async" => await theStore.OpenSessionAsync(options, Ct),
            "open_session_async_identity" => await theStore.OpenSessionAsync(identity, Ct),
            "without_held_connection" => theStore.LightweightSessionWithoutHeldConnection(options),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
    }

    /// <summary>Saves a statement that records the level the save's transaction is running at.</summary>
    private async Task<int> LevelInsideASaveAsync(IDocumentSession session)
    {
        session.QueueSqlCommand($"INSERT INTO {ProbeTable}(lvl) {ReadLevelSql}");
        await session.SaveChangesAsync(Ct);

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT TOP 1 lvl FROM {ProbeTable} ORDER BY id DESC;";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(Ct));
    }

    internal static async Task<(int Level, int Spid, int TranCount)> DescribeConnectionAsync(QuerySession session)
    {
        await using var cmd = new SqlCommand($"""
            SELECT CONVERT(varchar(20), ({ReadLevelSql})) + ',' + CONVERT(varchar(20), @@SPID)
                 + ',' + CONVERT(varchar(20), @@TRANCOUNT);
            """);
        var parts = ((string)(await session.ExecuteScalarAsync(cmd, Ct))!).Split(',').Select(int.Parse).ToArray();
        return (parts[0], parts[1], parts[2]);
    }

    [Theory]
    [MemberData(nameof(SessionsAndLevels))]
    public async Task every_save_runs_at_the_requested_level(string kind, IsolationLevel level, int expected)
    {
        await using var session = await OpenAsync(kind, level);

        // Three, not two: the first save after the first is what used to fall back to ReadCommitted, and
        // the third shows the second did not merely get lucky.
        (await LevelInsideASaveAsync(session)).ShouldBe(expected);
        (await LevelInsideASaveAsync(session)).ShouldBe(expected);
        (await LevelInsideASaveAsync(session)).ShouldBe(expected);
    }

    /// <summary>
    ///     Why every command runs inside a transaction at these levels, and not just the save. The
    ///     snapshot has to be the one the read saw, so a transaction begun at the save, which is after
    ///     the concurrent write, finds no conflict to report.
    /// </summary>
    /// <remarks>
    ///     Asserted on an append; <c>snapshot_document_write_conflict_tests</c> covers <c>session.Store</c>.
    /// </remarks>
    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task a_read_and_the_save_after_it_are_one_snapshot_on_every_unit_of_work(string kind)
    {
        var stream = Guid.NewGuid();
        await using (var seed = theStore.LightweightSession())
        {
            seed.Events.StartStream(stream, new QuestStarted("original"));
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = await OpenAsync(kind, IsolationLevel.Snapshot);

        // The issue's scenario is the second save, so make this the second unit of work.
        await LevelInsideASaveAsync(session);

        await session.Events.FetchStreamAsync(stream, token: Ct);

        await using (var writer = theStore.LightweightSession())
        {
            writer.Events.Append(stream, new MonsterSlain("Theirs", 1));
            await writer.SaveChangesAsync(Ct);
        }

        session.Events.Append(stream, new MonsterSlain("Mine", 1));

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).Count.ShouldBe(2);
    }

    /// <summary>
    ///     A guarded document reports its own exception at <c>Snapshot</c> rather than 3960.
    ///     <c>docs/configuration/retries.md</c> says so, and this is what holds that claim to the truth.
    /// </summary>
    [Fact]
    public async Task optimistic_concurrency_protects_a_document_written_at_snapshot()
    {
        var id = Guid.NewGuid();
        await using (var seed = theStore.LightweightSession())
        {
            seed.Store(new VersionedNote { Id = id, Name = "original" });
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = IsolationLevel.Snapshot });

        var mine = await session.LoadAsync<VersionedNote>(id, Ct);

        await using (var writer = theStore.LightweightSession())
        {
            var theirs = await writer.LoadAsync<VersionedNote>(id, Ct);
            theirs!.Name = "theirs";
            writer.Store(theirs);
            await writer.SaveChangesAsync(Ct);
        }

        mine!.Name = "mine";
        session.Store(mine);

        await Should.ThrowAsync<JasperFx.ConcurrencyException>(() => session.SaveChangesAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VersionedNote>(id, Ct))!.Name.ShouldBe("theirs");
    }

    [Fact]
    public async Task a_session_holding_a_transaction_reads_inside_it()
    {
        await using var session = theStore.LightweightSession(
            new SessionOptions { IsolationLevel = IsolationLevel.Serializable });

        var internalSession = (DocumentSessionBase)session;
        var described = await DescribeConnectionAsync(internalSession);

        described.Level.ShouldBe(4);
        described.TranCount.ShouldBe(1);
        internalSession.ActiveTransaction.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_read_committed_session_still_reads_outside_a_transaction()
    {
        await using var session = theStore.LightweightSession();

        var internalSession = (DocumentSessionBase)session;
        var described = await DescribeConnectionAsync(internalSession);

        described.Level.ShouldBe(ReadCommittedLevel);
        described.TranCount.ShouldBe(0);
        internalSession.ActiveTransaction.ShouldBeNull();
    }

    /// <summary>
    ///     What a query session does, which is not what its <c>SessionOptions</c> might suggest. It never
    ///     opens a transaction, so there is nothing for the level to apply to. Pinned so a future reader
    ///     does not take the option being accepted for the option being honoured.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.Snapshot)]
    public async Task a_query_session_never_opens_a_transaction_whatever_its_options_say(IsolationLevel level)
    {
        await using var session = theStore.QuerySession(new SessionOptions { IsolationLevel = level });

        var described = await DescribeConnectionAsync((QuerySession)session);

        described.Level.ShouldBe(ReadCommittedLevel);
        described.TranCount.ShouldBe(0);
    }
}

/// <summary>
///     Split out of <see cref="session_isolation_level_tests" /> so it runs alone: the connection under test has
///     to stay in its pool between the two sessions, and another test calling <c>SqlConnection.ClearAllPools()</c>
///     in between discarded it (see <see cref="ConnectionPoolCollection" />).
/// </summary>
[Collection("connection-pool")]
public class session_isolation_level_pool_tests : OneOffConfigurationsContext
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     <c>BeginTransaction(level)</c> sets the connection's level, and <c>sp_reset_connection</c> does
    ///     not undo it. So honouring the option would have made a pooled connection carry Serializable
    ///     into whatever opened it next, and a query session reads at the connection's level.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.Snapshot)]
    public async Task a_sessions_level_does_not_leak_through_the_connection_pool(IsolationLevel level)
    {
        // Its own pool, so nothing else in the process can take the connection in between.
        var connectionString = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            ApplicationName = $"polecat_844_{Guid.NewGuid():N}"
        }.ConnectionString;

        ConfigureStore(opts => opts.ConnectionString = connectionString);

        int spid;
        await using (var session = theStore.LightweightSession(new SessionOptions { IsolationLevel = level }))
        {
            spid = (await session_isolation_level_tests.DescribeConnectionAsync((DocumentSessionBase)session)).Spid;
        }

        await using var query = theStore.QuerySession();
        var described = await session_isolation_level_tests.DescribeConnectionAsync((QuerySession)query);

        // Otherwise this is not the connection under test and the assertion below proves nothing.
        described.Spid.ShouldBe(spid);
        described.Level.ShouldBe(session_isolation_level_tests.ReadCommittedLevel);

        SqlConnection.ClearPool(new SqlConnection(connectionString));
    }
}

/// <summary>
///     #1039: the snapshot-isolation helper every snapshot test calls has to work against the server's
///     <c>master</c> catalog, where a plain run of the suite points by default.
/// </summary>
public class allow_snapshot_isolation_helper_tests
{
    [Fact]
    public async Task works_against_a_system_database()
    {
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(
            ConnectionSource.MasterConnectionString, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task turns_snapshot_isolation_on_in_a_catalog_where_it_is_off()
    {
        var ct = TestContext.Current.CancellationToken;
        var catalog = ConnectionSource.Scoped("snapshot_helper");

        await using (var conn = new SqlConnection(ConnectionSource.MasterConnectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{catalog}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{catalog}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{catalog}];
                END
                CREATE DATABASE [{catalog}];
                ALTER DATABASE [{catalog}] SET ALLOW_SNAPSHOT_ISOLATION OFF;
                """;
            await cmd.ExecuteNonQueryAsync(ct);
        }

        try
        {
            await session_isolation_level_tests.AllowSnapshotIsolationAsync(
                ConnectionSource.ConnectionStringFor(catalog), ct);

            await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT snapshot_isolation_state FROM sys.databases WHERE name = '{catalog}'";
            Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)).ShouldBe(1);
        }
        finally
        {
            SqlConnection.ClearAllPools();
            await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                ALTER DATABASE [{catalog}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{catalog}];
                """;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
