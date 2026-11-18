using JasperFx;
using Microsoft.Data.SqlClient;
using Polly;
using Polecat.Internal;
using Weasel.Core;
using Weasel.Core.Sequences;
using Weasel.SqlServer;

namespace Polecat.Schema.Identity.Sequences;

/// <summary>
///     SQL Server Hi-Lo sequence. The dialect-agnostic hi/lo state and client-side
///     id arithmetic live on <see cref="HiloSequenceBase" /> (lifted into Weasel.Core
///     in weasel#287); this subclass supplies only the SQL Server I/O — an optimistic
///     UPDATE/INSERT against <c>pc_hilo</c>, wrapped in a resilience pipeline.
/// </summary>
internal class HiloSequence : HiloSequenceBase
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly string _schemaName;

    /// <summary>#390: one escaped builder for the name, rather than seven hand-written copies.</summary>
    private readonly string _hiloTable;

    private readonly StoreOptions _options;
    private readonly ResiliencePipeline _resilience;
    private volatile bool _tableEnsured;

    // #1088: serializes the synchronous ensure of pc_hilo, so a burst of cold callers runs it once
    private readonly Lock _synchronousEnsure = new();

    public HiloSequence(ConnectionFactory connectionFactory, StoreOptions options, string entityName,
        HiloSettings settings)
        : base(entityName, settings)
    {
        _connectionFactory = connectionFactory;
        _options = options;
        _schemaName = options.DatabaseSchemaName;
        _hiloTable = Polecat.Internal.SqlEscaping.QualifiedName(_schemaName, HiloTable.TableName);
        _resilience = options.ResiliencePipeline;
    }

    /// <summary>
    ///     Ensure <c>pc_hilo</c> on behalf of a caller that knows whether it has a transaction open, which
    ///     is a session about to assign an id. See <see cref="EnsureHiloTableAsync" />.
    /// </summary>
    internal void EnsureStorage(bool insideTransaction)
    {
        if (_tableEnsured) return;

        _resilience.Execute(static state => state.Item1.ensureHiloTable(null, state.insideTransaction),
            (this, insideTransaction));
    }

    /// <summary>
    ///     The synchronous ensure of <c>pc_hilo</c>, for <see cref="EnsureStorage" /> and
    ///     <see cref="AdvanceToNextHiSync" />. The same rules as <see cref="EnsureHiloTableAsync" />, with blocking
    ///     I/O from start to finish.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     #1088 (marten#5969): these used to run <see cref="EnsureHiloTableAsync" /> and block on it. The first burst
    ///     of synchronous <c>Store()</c> calls on a fresh schema then had every writer holding a pool thread while it
    ///     waited on asynchronous work that needed a free pool thread to finish, and a 10 ms heartbeat on the pool
    ///     came back up to two seconds late.
    ///     </para>
    ///     <para>
    ///     Creates the table if it is missing, with the guarded DDL Weasel writes for it, and leaves an existing one
    ///     alone; the asynchronous ensure and the store's migrations are what update one. One caller per sequence
    ///     runs it, and the others wait on that caller's I/O, except a caller inside a transaction, which never
    ///     waits (see <see cref="SchemaMigrationLock.HoldForFirstUseAsync" />) and skips the ensure instead.
    ///     Waiting comes before opening a connection, so a burst of cold callers opens one, not one each.
    ///     </para>
    /// </remarks>
    /// <param name="openConnection">A connection the caller already has open, or null to open one if needed.</param>
    /// <param name="insideTransaction"></param>
    private void ensureHiloTable(SqlConnection? openConnection, bool insideTransaction)
    {
        if (_tableEnsured) return;

        // #267, as in EnsureHiloTableAsync
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _tableEnsured = true;
            return;
        }

        if (insideTransaction)
        {
            if (!_synchronousEnsure.TryEnter()) return;
        }
        else
        {
            _synchronousEnsure.Enter();
        }

        try
        {
            if (_tableEnsured) return;

            if (openConnection != null)
            {
                ensureHiloTableOn(openConnection, insideTransaction);
            }
            else
            {
                using var conn = _connectionFactory.Create();
                conn.Open();
                ensureHiloTableOn(conn, insideTransaction);
            }
        }
        finally
        {
            _synchronousEnsure.Exit();
        }
    }

    private void ensureHiloTableOn(SqlConnection conn, bool insideTransaction)
    {
        if (SchemaMigrationLock.TryRunForFirstUse(_options, conn, insideTransaction, hiloTableDdl()))
        {
            _tableEnsured = true;
        }
    }

    private string hiloTableDdl()
    {
        var writer = new StringWriter();
        writer.WriteLine(SqlServerMigrator.CreateSchemaStatementFor(_schemaName));
        new HiloTable(_schemaName).WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    internal void ForgetEnsuredTable() => _tableEnsured = false;

    /// <remarks>
    ///     #1087 (marten#6058, weasel#849): the block reserved above the floor follows the overlap rule an
    ///     advance does. An advance that reserves after the row moved, and installs first, holds a higher block
    ///     that is above the floor too. Installing the floor's lower block on top of it would hand out ids behind
    ///     ones already issued: above the floor and unique, but out of order. So <see cref="HiloSequenceBase.InstallCount" />
    ///     is taken once the row has moved, and the floor's block is dropped as a gap when another one was
    ///     installed since and is above it.
    /// </remarks>
    public override async Task SetFloor(long floor)
    {
        var numberOfPages = (long)Math.Ceiling((double)floor / MaxLo);

        // Guarantee the hilo row exists
        await AdvanceToNextHi().ConfigureAwait(false);

        await _resilience.ExecuteAsync(async (state, ct) =>
        {
            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"UPDATE {_hiloTable} SET hi_value = @floor WHERE entity_name = @name;";
            cmd.Parameters.AddWithValue("@floor", state);
            cmd.Parameters.AddVarChar("@name", EntityName);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, numberOfPages).ConfigureAwait(false);

        // Counted once the row has moved: any block installed from here on was reserved above the floor too
        var installsBefore = InstallCount;

        // Reserve the block above the new floor, and install it unless a higher one got there first
        TrySetCurrentHi(await FetchNextHiAsync(CancellationToken.None).ConfigureAwait(false), installsBefore);
    }

    public override async Task AdvanceToNextHi(CancellationToken ct = default)
    {
        TrySetCurrentHi(await FetchNextHiAsync(ct).ConfigureAwait(false));
    }

    /// <summary>
    ///     Reserve the next hi from <c>pc_hilo</c> without installing it. Every asynchronous reservation goes
    ///     through here: <see cref="AdvanceToNextHi" />, and the block <see cref="SetFloor" /> reserves above the
    ///     floor.
    /// </summary>
    internal virtual async Task<long> FetchNextHiAsync(CancellationToken ct)
    {
        return await _resilience.ExecuteAsync(async (_, cancellation) =>
        {
            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(cancellation).ConfigureAwait(false);

            await EnsureHiloTableAsync(conn, insideTransaction: true, cancellation).ConfigureAwait(false);

            // gh-1004: the MERGE secures a hi on its first attempt, so this loop no longer spins under
            // contention; it stays only so a value that somehow reads as "not secured" is never installed.
            // Transient failures are the resilience pipeline's to retry.
            for (var attempts = 0; attempts < Settings.MaxAdvanceToNextHiAttempts; attempts++)
            {
                var result = await TryGetNextHiAsync(conn, cancellation).ConfigureAwait(false);
                if (result >= 0)
                {
                    return result;
                }
            }

            throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
        }, ct).ConfigureAwait(false);
    }

    protected override void AdvanceToNextHiSync()
    {
        // #148: the async AdvanceToNextHi wraps its work in the resilience
        // pipeline; the synchronous path must do the same (via the pipeline's
        // synchronous Execute) so all pc_hilo access is covered.
        _resilience.Execute(() =>
        {
            using var conn = _connectionFactory.Create();
            conn.Open();

            // #1088: synchronous from start to finish, never blocking on the asynchronous ensure
            ensureHiloTable(conn, insideTransaction: true);

            // gh-1004: see AdvanceToNextHi -- one attempt now always secures a hi.
            for (var attempts = 0; attempts < Settings.MaxAdvanceToNextHiAttempts; attempts++)
            {
                var result = TryGetNextHiSync(conn);
                if (TrySetCurrentHi(result))
                {
                    return;
                }
            }

            throw new HiloSequenceAdvanceToNextHiAttemptsExceededException();
        });
    }

    /// <remarks>
    ///     <para>
    ///     #883: on the same lock as every other path that provisions this schema, because the daemon's
    ///     ensure declares <c>pc_hilo</c> too. <paramref name="insideTransaction" /> means what it means for
    ///     <see cref="SchemaMigrationLock.HoldForFirstUseAsync" />, with a difference that decides who
    ///     passes what: nothing below <see cref="ISequence" /> can see a transaction, and an id can be asked
    ///     for at any point in one. So the sequence's own paths always pass true and never wait, whatever the
    ///     caller is doing. A busy lock skips the ensure, a table that exists is simply used, and one that
    ///     does not fails with nothing remembered, for the next id to try again.
    ///     </para>
    ///     <para>
    ///     That alone would be worse than what it replaced on a fresh schema: every <c>Store()</c> during
    ///     a migration failing on a missing table, where the old code only collided in the instant both
    ///     ran the CREATE. So a session, which does know, comes through <see cref="EnsureStorage" /> first
    ///     and waits like any other first use when it has no transaction open.
    ///     </para>
    /// </remarks>
    private async Task EnsureHiloTableAsync(SqlConnection conn, bool insideTransaction, CancellationToken ct)
    {
        if (_tableEnsured) return;

        // #267: honor AutoCreate.None — the user manages the pc_hilo table themselves, so never
        // emit DDL at runtime (mirrors DocumentTableEnsurer). A subsequent UPDATE/INSERT against a
        // missing table surfaces as a clean "invalid object" error, the same opt-out contract as
        // document and event-store tables.
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _tableEnsured = true;
            return;
        }

        await using var migrationLock =
            (await SchemaMigrationLock.HoldForFirstUseAsync(_options, conn, insideTransaction, ct).ConfigureAwait(false));
        if (migrationLock is null) return;

        var table = new HiloTable(_schemaName);
        var migrator = new SqlServerMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, ct, table).ConfigureAwait(false);
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct).ConfigureAwait(false);
        _tableEnsured = true;
    }

    /// <summary>
    ///     gh-1004 (marten#5938): reserve the next hi with one statement that cannot lose. The old
    ///     read-then-compare-and-set returned -1 to every caller whose read another caller's commit had
    ///     overtaken, and under contention a caller could spend its whole
    ///     <c>MaxAdvanceToNextHiAttempts</c> budget that way (347 of 400 advances failed at one attempt).
    ///     Now concurrent callers queue on the row's lock and each one gets a hi of its own.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     The first block for an entity is 0 and every later one is the stored value + 1, exactly as
    ///     before, so the meaning of <c>pc_hilo.hi_value</c> and <see cref="SetFloor" /> are unchanged.
    ///     </para>
    ///     <para>
    ///     <c>HOLDLOCK</c> is what makes the first use race-safe: without it two MERGEs that both find no
    ///     row both take the insert branch and one of them fails on the primary key. With it the
    ///     not-matched probe holds a key-range lock until the statement ends, so the second caller waits
    ///     and then takes the matched branch.
    ///     </para>
    /// </remarks>
    private string NextHiSql =>
        $"""
         MERGE {_hiloTable} WITH (HOLDLOCK) AS t
         USING (SELECT @entity AS entity_name) AS s ON t.entity_name = s.entity_name
         WHEN MATCHED THEN UPDATE SET hi_value = t.hi_value + 1
         WHEN NOT MATCHED THEN INSERT (entity_name, hi_value) VALUES (s.entity_name, 0)
         OUTPUT inserted.hi_value;
         """;

    private async Task<long> TryGetNextHiAsync(SqlConnection conn, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = NextHiSql;
        cmd.Parameters.AddVarChar("@entity", EntityName);
        return ToHi(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false));
    }

    private long TryGetNextHiSync(SqlConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = NextHiSql;
        cmd.Parameters.AddVarChar("@entity", EntityName);
        return ToHi(cmd.ExecuteScalar());
    }

    // The MERGE always outputs one row. A missing value must still read as "not secured" (-1) rather
    // than 0, which Convert.ToInt64 would make of a null and TrySetCurrentHi would install as a block.
    private static long ToHi(object? raw) => raw is null || raw == DBNull.Value ? -1 : Convert.ToInt64(raw);
}
