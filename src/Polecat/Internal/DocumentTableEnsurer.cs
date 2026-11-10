using System.Collections.Concurrent;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Schema.Identity.Sequences;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal;

/// <summary>
///     Ensures document tables exist on demand. Uses Weasel SchemaMigration to create
///     or update tables, and tracks which types have been ensured.
/// </summary>
internal class DocumentTableEnsurer
{
    /// <summary>
    ///     SQL Server error 243, "Type %ls is not a defined system type." — what an instance without
    ///     the VECTOR type answers when a schema declares one.
    /// </summary>
    private const int UndefinedSystemType = 243;

    /// <summary>
    ///     Whether this failure means the instance has no VECTOR type, as opposed to any other way
    ///     the ALTER TABLE that adds a vector column can fail.
    /// </summary>
    /// <remarks>
    ///     Keyed on the error NUMBER on purpose. The first cut of this guard asked whether the
    ///     message contained "VECTOR", which over-matched: the computed column is named
    ///     <c>vec_&lt;member&gt;</c>, so a member called <c>Vector</c> yields <c>vec_vector</c>, and
    ///     any failure quoting that column name — a duplicate column, a permissions problem — was
    ///     reported to the user as a missing type on an instance that has one. The number is also
    ///     locale-proof, where a message-text rule is not.
    /// </remarks>
    internal static bool IsMissingVectorType(SqlException e) => e.Number == UndefinedSystemType;

    private readonly ConcurrentDictionary<Type, bool> _ensured = new();
    private readonly ConcurrentDictionary<Type, bool> _fksEnsured = new();
    private readonly ConcurrentDictionary<Type, DocumentProvider> _foundInsideTransaction = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly ConnectionFactory _connectionFactory;
    private readonly StoreOptions _options;
    private DocumentProviderRegistry? _providerRegistry;

    public DocumentTableEnsurer(ConnectionFactory connectionFactory, StoreOptions options)
    {
        _connectionFactory = connectionFactory;
        _options = options;
    }

    internal void SetProviderRegistry(DocumentProviderRegistry registry)
    {
        _providerRegistry = registry;
    }

    private bool _hiloTableEnsured;

    // #919: here because there's one of these per database, which is how far an answer about a collation
    // can be trusted. A tenant database can have a different one.
    private readonly ConcurrentDictionary<string, VarcharKeyColumn> _keyColumns =
        new(StringComparer.OrdinalIgnoreCase);

    internal SqlConnection CreateConnection() => _connectionFactory.Create();

    internal VarcharKeyColumn? KeyColumnFor(string table) => _keyColumns.GetValueOrDefault(table);

    internal VarcharKeyColumn RememberKeyColumn(string table, VarcharKeyColumn column)
        => _keyColumns.GetOrAdd(table, column);

    /// <summary>
    ///     Forget everything this ensurer has checked, because the tables it checked have been dropped.
    ///     Otherwise the next session trusts the memo and fails on a table nothing will create.
    /// </summary>
    internal void ForgetEnsuredTables()
    {
        _ensured.Clear();
        _keyColumns.Clear();
        _fksEnsured.Clear();
        _hiloTableEnsured = false;
        _eventStoreEnsured = false;
    }

    private int _schemaEnsureExecutions;

    /// <summary>
    ///     #538: how many times the actual schema-ensure work (migration diff + DDL) has run on this
    ///     instance — i.e. slow-path executions past the per-type memo, not calls. Test seam for the
    ///     regression where the daemon constructed a fresh (empty-memo) ensurer per projection batch
    ///     and re-ran the full schema check per document type on every batch.
    /// </summary>
    internal int SchemaEnsureExecutions => Volatile.Read(ref _schemaEnsureExecutions);

    /// <param name="provider">The document type whose table is needed.</param>
    /// <param name="insideTransaction">
    ///     Whether the caller has a transaction open on its own connection, from
    ///     <c>IConnectionLifetime.HasOpenTransaction</c>. Required rather than defaulted, because a caller
    ///     that gets this wrong can stall a session for the whole lock timeout: see
    ///     <see cref="SchemaMigrationLock.HoldForFirstUseAsync" />.
    /// </param>
    /// <param name="token"></param>
    public async Task EnsureTableAsync(DocumentProvider provider, bool insideTransaction, CancellationToken token)
    {
        var docType = provider.Mapping.DocumentType;

        if (_ensured.ContainsKey(docType))
        {
            return;
        }

        // #219: honor the user's explicit opt-out. AutoCreate.None means "I manage the schema",
        // so never create/alter tables implicitly on first use (mirrors Marten).
        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _ensured.TryAdd(docType, true);
            return;
        }

        await _semaphore.WaitAsync(token).ConfigureAwait(false);
        try
        {
            // Double-check after acquiring lock
            if (_ensured.ContainsKey(docType))
            {
                return;
            }

            Interlocked.Increment(ref _schemaEnsureExecutions);

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token).ConfigureAwait(false);

            // #868: serialized against the daemon and against other stores and processes. The semaphore
            // above only keeps this ensurer from racing itself. Always taken in that order, semaphore then
            // lock, and nothing here waits on a semaphore while it holds the lock.
            await using var migrationLock = (await SchemaMigrationLock.HoldForFirstUseAsync(
                _options, conn, insideTransaction, token).ConfigureAwait(false));
            if (migrationLock is null) return;

            var migrator = new SqlServerMigrator();

            // Ensure pc_hilo table for numeric ID types
            if (provider.Mapping.IsNumericId && !_hiloTableEnsured)
            {
                var hiloTable = new HiloTable(provider.Mapping.DatabaseSchemaName);
                var hiloMigration = await SchemaMigration.DetermineAsync(conn, token, hiloTable).ConfigureAwait(false);
                await migrator.ApplyAllAsync(conn, hiloMigration, AutoCreate.CreateOrUpdate, ct: token).ConfigureAwait(false);
                _hiloTableEnsured = true;
            }

            // Decision D2 widened the document `version` column to bigint. Existing tables created
            // before this carry an int column; widen it in place before Weasel diffs the table.
            // SQL Server rejects ALTER COLUMN while a default constraint references the column, and
            // Weasel's diff emits only a bare ALTER COLUMN — so this drops the default, widens (data
            // preserved — never a drop/recreate), then restores the default. No-op once bigint.
            await WidenVersionColumnIfNeededAsync(conn, provider.Mapping.QualifiedTableName, token).ConfigureAwait(false);

            // Use Weasel SchemaMigration to create or update the document table.
            // #255: an externally-managed partitioned table is provisioned ONCE (CreateOnly) and
            // never reconciled afterward, so a later schema apply can't clobber the partitions the
            // app/DBA manages at runtime (SPLIT new months, SWITCH/DROP old ones for retention).
            // #684: WITHOUT the foreign-key constraints. They are added by EnsureForeignKeysAsync below,
            // once every referenced table exists. See DeferForeignKeys' remarks for why deferring them
            // inside this migration is not enough.
            var table = new DocumentTable(provider.Mapping, includeForeignKeys: false);

            // #296: strongly-typed-id tables created before the inner-type fix carry a varchar(250)
            // id column, which trips InvalidCastException in the shared writeable selectors (they
            // read GetFieldValue<TInner>). Convert it to the inner primitive type in place — drop
            // PK, ALTER COLUMN, re-add PK — before Weasel diffs the table, so the diff stays clean.
            await ConvertStrongTypedIdColumnIfNeededAsync(conn, provider.Mapping, table, token).ConfigureAwait(false);

            // #335: a tenant-partitioned document table created after tenants already exist must
            // bake the full boundary set into its CREATE — hydrate the shared partition manager's
            // tenant map from pc_tenant_partitions first (cached; a no-op after the first call).
            if (provider.Mapping.TenantPartitioned)
            {
                await _options.EventGraph.TenantPartitionManager.InitializeAsync(conn, token).ConfigureAwait(false);
            }
            var autoCreate = provider.Mapping.Partitioning is { ExternallyManaged: true }
                ? AutoCreate.CreateOnly
                : AutoCreate.CreateOrUpdate;
            var migration = await SchemaMigration.DetermineAsync(conn, token, table).ConfigureAwait(false);

            // #684: the computed columns, secondary indexes and foreign keys the mapping declares are
            // part of `table` now, so this ONE migration applies them -- they used to follow as raw DDL
            // loops here, which is what kept them out of the generated script and out of
            // AssertDatabaseMatchesConfigurationAsync. See DocumentTable.DeclaredObjects.cs.
            //
            // The VECTOR translation moved out here with them: a server with no VECTOR type now fails
            // inside the migration rather than on one isolated ALTER, so the catch has to sit where the
            // migration is. It reads the declaration list to name the member, which is all the old
            // per-statement catch had to work with anyway.
            try
            {
                await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: token).ConfigureAwait(false);
            }
            catch (SqlException e) when (IsMissingVectorType(e) && provider.Mapping.VectorIndexes.Count > 0)
            {
                // Azure SQL Edge and anything before SQL Server 2025 have no VECTOR type, and the
                // server's own answer — "Type VECTOR is not a defined system type" — names neither
                // Polecat nor the line of configuration that asked for it.
                // See IsMissingVectorType for why this keys on the number, not the message.
                var members = provider.Mapping.VectorIndexes
                    .Select(x => $"{provider.Mapping.DocumentType.Name}.{x.MemberName}")
                    .ToArray();

                throw new InvalidOperationException(
                    $"'{string.Join("', '", members)}' {(members.Length == 1 ? "is" : "are")} declared as a "
                    + "vector, but this SQL Server instance has no VECTOR type. Vector search needs "
                    + "SQL Server 2025 or later; Azure SQL Edge does not have it. Remove the "
                    + "VectorIndex(...) declaration or move to an instance that supports it.", e);
            }

            // #685: the Polecat-owned full-text index is no longer rendered here. Its token table and
            // maintaining trigger are declared schema objects (FullTextSchemaObjects.For) and reconciled
            // by a migration like everything else, so they now appear in the generated script, are
            // compared by AssertDatabaseMatchesConfigurationAsync, and can be refused under
            // AutoCreate.None -- which could not refuse what it could not see.
            //
            // A SECOND migration rather than extra objects on the one above, for two reasons: the
            // trigger's target and the table its body writes to both have to exist before CREATE
            // TRIGGER runs, which only holds if the document table's migration has already been
            // applied; and the vector-type translation above stays scoped to the statement that can
            // actually raise it.
            var ftObjects = Storage.FullText.FullTextSchemaObjects.For(provider.Mapping);
            if (ftObjects.Length > 0)
            {
                // #877: created here and never updated here. Changing an existing token table's column,
                // or adding an index to it (#916), is a size-of-data operation that can outlast the command
                // timeout inside whichever request got here first, and it would fail the same way on every
                // request after it. A table left as it was keeps working as it did; see FullTextTokenTable.
                var tokenTables = ftObjects.Where(x => x is Storage.FullText.FullTextTokenTable).ToArray();
                var tokenTableMigration = await SchemaMigration.DetermineAsync(conn, token, tokenTables).ConfigureAwait(false);
                if (tokenTableMigration.Difference == SchemaPatchDifference.Create)
                {
                    await migrator.ApplyAllAsync(conn, tokenTableMigration, AutoCreate.CreateOrUpdate, ct: token).ConfigureAwait(false);
                }

                var ftMigration = await SchemaMigration.DetermineAsync(conn, token, ftObjects.Except(tokenTables).ToArray()).ConfigureAwait(false);
                await migrator.ApplyAllAsync(conn, ftMigration, AutoCreate.CreateOrUpdate, ct: token).ConfigureAwait(false);

                // The backfill is a DATA migration and deliberately not a schema object: a trigger only
                // fires on writes made after it exists, so a declaration against a store that already
                // holds documents would otherwise index nothing -- no error, just a search that quietly
                // answers empty. Idempotent, so running it on every ensure is correct rather than merely
                // harmless. It follows the migration because it writes to the table that migration
                // creates.
                await Storage.FullText.FullTextBackfill.RunAsync(conn, provider.Mapping, _options.CommandTimeout).ConfigureAwait(false);
            }

            // #685: JSON indexes are no longer rendered here. They are declared on DocumentTable
            // (AddDeclaredJsonIndexes) and created by the Weasel migration above with everything else,
            // so they now appear in the generated script, are compared by
            // AssertDatabaseMatchesConfigurationAsync, and can be refused under AutoCreate.None.

            _ensured.TryAdd(docType, true);
            _foundInsideTransaction.TryRemove(docType, out _);
        }
        finally
        {
            _semaphore.Release();
        }

        // Create foreign keys (deferred — referenced tables must exist first)
        await EnsureForeignKeysAsync(provider, insideTransaction, token).ConfigureAwait(false);
    }

    private async Task EnsureForeignKeysAsync(DocumentProvider provider, bool insideTransaction,
        CancellationToken token)
    {
        if (provider.Mapping.ForeignKeys.Count == 0) return;
        if (_fksEnsured.ContainsKey(provider.Mapping.DocumentType)) return;
        if (_providerRegistry == null) return;

        // Ensure all referenced tables exist BEFORE acquiring the FK semaphore
        // to avoid deadlock (EnsureTableAsync also acquires _semaphore)
        foreach (var fk in provider.Mapping.ForeignKeys)
        {
            var refProvider = _providerRegistry.GetProvider(fk.ReferenceDocumentType);
            await EnsureTableAsync(refProvider, insideTransaction, token).ConfigureAwait(false);
        }

        await _semaphore.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_fksEnsured.ContainsKey(provider.Mapping.DocumentType)) return;

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token).ConfigureAwait(false);

            // #868: a new constraint is DDL like any other. See EnsureTableAsync.
            await using var migrationLock = (await SchemaMigrationLock.HoldForFirstUseAsync(
                _options, conn, insideTransaction, token).ConfigureAwait(false));
            if (migrationLock is null) return;

            // #684: re-diff the table WITH its constraints, now that every referenced table exists. The
            // first pass built it with `includeForeignKeys: false`, so the constraints are all this delta
            // consists of; everything else about the table already matches, which makes it cheap as well
            // as idempotent. No raw DDL, and no second description of the constraint to keep in sync.
            //
            // ⚠️ Omitting the constraints from the first pass is not the same as DEFERRING them there,
            // and deferring is what this originally did. `SchemaMigration` defers a foreign key whose
            // linked table is created later in the same migration, and Weasel's SQL Server migrator then
            // writes the deferred set as its LAST command of the same ApplyAllAsync call — which is
            // exactly right when every table is in one migration, and useless here, where each table is
            // migrated alone: the constraint still ran, just at the end, against a table that may not
            // exist. Leaving it out of the model is the only thing that actually holds it back.
            //
            // ⚠️ `autoCreate` is carried through rather than forced to CreateOrUpdate, which means an
            // EXTERNALLY-MANAGED partitioned table (#255, CreateOnly) does not get a foreign key added
            // to it after creation. That is the narrower behaviour of the two and the deliberate choice:
            // #255's guarantee is that nothing reconciles those tables after provisioning, and a
            // constraint added by a later pass is a reconciliation.
            var table = new DocumentTable(provider.Mapping);
            var autoCreate = provider.Mapping.Partitioning is { ExternallyManaged: true }
                ? AutoCreate.CreateOnly
                : AutoCreate.CreateOrUpdate;

            var migrator = new SqlServerMigrator();
            var migration = await SchemaMigration.DetermineAsync(conn, token, table).ConfigureAwait(false);
            await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: token).ConfigureAwait(false);

            _fksEnsured.TryAdd(provider.Mapping.DocumentType, true);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    ///     Normalizes an existing document <c>version</c> column to the Decision D2 shape:
    ///     <c>bigint NOT NULL</c> with no default constraint. Drops any lingering default (the model
    ///     carries none, and a default would block the ALTER below), then widens an int column to
    ///     bigint in place — never a drop/recreate, so rows are preserved. Idempotent: a no-op once
    ///     the table is absent or already bigint with no default.
    /// </summary>
    private static async Task WidenVersionColumnIfNeededAsync(SqlConnection conn, string qualifiedTableName,
        CancellationToken token)
    {
        // The table name is derived from the document type (not user input), so it is inlined
        // directly; only the discovered default-constraint name needs dynamic SQL (and SQL Server
        // forbids function calls inside EXEC(...), hence the SET-then-EXEC pattern).
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @oid int = OBJECT_ID({SqlEscaping.Literal(qualifiedTableName)});
            IF @oid IS NOT NULL
            BEGIN
                DECLARE @df sysname, @sql nvarchar(max);
                SELECT @df = dc.name FROM sys.default_constraints dc
                    JOIN sys.columns col ON col.default_object_id = dc.object_id
                    WHERE dc.parent_object_id = @oid AND col.name = 'version';
                IF @df IS NOT NULL
                BEGIN
                    SET @sql = '{SqlEscaping.LiteralBody($"ALTER TABLE {qualifiedTableName} DROP CONSTRAINT ")}' + QUOTENAME(@df);
                    EXEC(@sql);
                END
                IF EXISTS (
                    SELECT 1 FROM sys.columns c
                    WHERE c.object_id = @oid AND c.name = 'version' AND TYPE_NAME(c.system_type_id) = 'int')
                    ALTER TABLE {qualifiedTableName} ALTER COLUMN [version] bigint NOT NULL;
            END
            """;
        await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    /// <summary>
    ///     #296: converts a legacy <c>varchar(250)</c> id column on a strongly-typed-id table to the
    ///     inner primitive type (<c>uniqueidentifier</c>/<c>int</c>/<c>bigint</c>) in place, so the
    ///     shared writeable selectors (which read the inner value via <c>GetFieldValue&lt;TInner&gt;</c>)
    ///     can materialize it. Drops the primary key, widens the column — existing guid/numeric string
    ///     values convert implicitly — then restores the primary key from the modeled definition. A
    ///     no-op once the column is already the native type (or the table is absent), and skipped for
    ///     string-backed wrappers (they legitimately stay <c>varchar(250)</c>) and partitioned tables
    ///     (whose clustered-PK/partition-scheme coupling makes an in-place rebuild unsafe — those keep
    ///     the correct type from creation and any pre-existing table is a manual migration).
    /// </summary>
    private static async Task ConvertStrongTypedIdColumnIfNeededAsync(SqlConnection conn,
        DocumentMapping mapping, DocumentTable table, CancellationToken token)
    {
        if (!mapping.IsStrongTypedId || mapping.Partitioning is not null)
        {
            return;
        }

        var targetType = mapping.InnerIdType == typeof(Guid) ? "uniqueidentifier"
            : mapping.InnerIdType == typeof(int) ? "int"
            : mapping.InnerIdType == typeof(long) ? "bigint"
            : null;
        if (targetType is null)
        {
            return; // string-backed wrapper — the varchar(250) column is correct.
        }

        var qualifiedTableName = mapping.QualifiedTableName;
        var pkColumnList = string.Join(", ", table.PrimaryKeyColumns.Select(SqlEscaping.QuoteIdentifier));

        // The table/PK names are derived from the document type (not user input) and inlined; only
        // the discovered PK-constraint name needs dynamic SQL (SQL Server forbids function calls
        // inside EXEC(...), hence SET-then-EXEC). ALTER COLUMN of a PK member requires dropping the
        // key first, so the whole convert runs as one guarded batch.
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @oid int = OBJECT_ID({SqlEscaping.Literal(qualifiedTableName)});
            IF @oid IS NOT NULL AND EXISTS (
                SELECT 1 FROM sys.columns c
                WHERE c.object_id = @oid AND c.name = 'id'
                  AND TYPE_NAME(c.system_type_id) IN ('varchar', 'nvarchar', 'char', 'nchar'))
            BEGIN
                DECLARE @pk sysname, @sql nvarchar(max);
                SELECT @pk = kc.name FROM sys.key_constraints kc
                    WHERE kc.parent_object_id = @oid AND kc.type = 'PK';
                IF @pk IS NOT NULL
                BEGIN
                    SET @sql = '{SqlEscaping.LiteralBody($"ALTER TABLE {qualifiedTableName} DROP CONSTRAINT ")}' + QUOTENAME(@pk);
                    EXEC(@sql);
                END
                ALTER TABLE {qualifiedTableName} ALTER COLUMN [id] {targetType} NOT NULL;
                ALTER TABLE {qualifiedTableName} ADD CONSTRAINT {table.PrimaryKeyName} PRIMARY KEY ({pkColumnList});
            END
            """;
        await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    public async Task EnsureTablesAsync(IEnumerable<DocumentProvider> providers, bool insideTransaction,
        CancellationToken token)
    {
        foreach (var provider in providers)
        {
            await EnsureTableAsync(provider, insideTransaction, token).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Ensure the tables a save's inline projections turned out to write to, from inside that save's
    ///     transaction.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #870: an event projection that calls <c>Store</c> on a type it never declared is only found
    ///         out once it has run. <see cref="EnsureTableAsync" /> needs a connection of its own for that,
    ///         which is a second one for a session already holding a transaction, so with the pool at its
    ///         limit every save waited on a connection only another save could give back, until they timed
    ///         out: #846's stall again, once per type on every process start, and on a pool of one it could
    ///         not succeed at all.
    ///     </para>
    ///     <para>
    ///         Almost every time, the table is there, and the transaction's own connection can say so. Such
    ///         a table is left for <see cref="EnsureTablesFoundInsideTransactionAsync" /> rather than
    ///         remembered as ensured, because a table being there says nothing about its columns: the next
    ///         save does the whole ensure, ahead of its transaction, and memoizes it there. Only a table that
    ///         really is missing still has to go round by a second connection. DDL cannot run on this one,
    ///         because Weasel's migration does not take a transaction, and a type the projection declares
    ///         with <c>RegisterPublishedType</c> never reaches this at all.
    ///     </para>
    /// </remarks>
    /// <param name="providers">The types the save's projections wrote.</param>
    /// <param name="tableExists">Asks the session's own connection whether a table exists.</param>
    /// <param name="token"></param>
    public async Task EnsureProjectedTablesAsync(IEnumerable<DocumentProvider> providers,
        Func<string, CancellationToken, Task<bool>> tableExists, CancellationToken token)
    {
        foreach (var provider in providers)
        {
            var docType = provider.Mapping.DocumentType;

            if (_ensured.ContainsKey(docType) || _foundInsideTransaction.ContainsKey(docType)) continue;

            if (_options.AutoCreateSchemaObjects != AutoCreate.None
                && await tableExists(provider.Mapping.QualifiedTableName, token).ConfigureAwait(false))
            {
                _foundInsideTransaction.TryAdd(docType, provider);
                continue;
            }

            await EnsureTableAsync(provider, insideTransaction: true, token).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Finish what <see cref="EnsureProjectedTablesAsync" /> left, for a caller with no transaction open.
    /// </summary>
    public Task EnsureTablesFoundInsideTransactionAsync(CancellationToken token)
    {
        return _foundInsideTransaction.IsEmpty
            ? Task.CompletedTask
            : EnsureTablesAsync(_foundInsideTransaction.Values.ToArray(), insideTransaction: false, token);
    }

    private volatile bool _eventStoreEnsured;
    private readonly SemaphoreSlim _eventStoreSemaphore = new(1, 1);

    /// <summary>
    ///     #219: ensures the event store schema (streams / events / progression tables, plus any tag
    ///     and natural-key tables) exists on first usage of the event store — the event-sourcing
    ///     analogue of EnsureTableAsync for documents. Idempotent and applied once per process; a
    ///     no-op under AutoCreate.None so the user's manual-schema opt-out is respected.
    /// </summary>
    /// <param name="insideTransaction">
    ///     Whether the caller has a transaction open on its own connection. See
    ///     <see cref="EnsureTableAsync" />. A real way to get here: <c>AppendExclusive</c> takes a stream
    ///     lock in a transaction, and only then does the save reach this.
    /// </param>
    /// <param name="token"></param>
    public async Task EnsureEventStoreSchemaAsync(bool insideTransaction, CancellationToken token)
    {
        if (_eventStoreEnsured) return;

        if (_options.AutoCreateSchemaObjects == AutoCreate.None)
        {
            _eventStoreEnsured = true;
            return;
        }

        await _eventStoreSemaphore.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_eventStoreEnsured) return;

            await using var conn = _connectionFactory.Create();
            await conn.OpenAsync(token).ConfigureAwait(false);

            // #868: the semaphore above covers this ensurer in this process. The daemon applies this same
            // schema through PolecatDatabase.EnsureStorageExistsAsync, so without this a session's first
            // write and the daemon both read an empty catalog, and whoever ran second failed with
            // "There is already an object named 'fk_pc_events_stream_id' in the database". A loser waits
            // here, then the diff below finds the schema already there and comes out empty.
            await using var migrationLock = (await SchemaMigrationLock.HoldForFirstUseAsync(
                _options, conn, insideTransaction, token).ConfigureAwait(false));
            if (migrationLock is null) return;

            var migrator = new SqlServerMigrator();

            // #335: under per-tenant partitioning, hydrate the shared partition manager's tenant map
            // first so pc_events/pc_streams CREATEd against an existing registry bake the full
            // boundary set (cached; a no-op after the first call).
            if (_options.EventGraph.AnyTenantPartitioning)
            {
                await _options.EventGraph.TenantPartitionManager.InitializeAsync(conn, token).ConfigureAwait(false);
            }

            var eventSchema = new Events.Schema.EventStoreFeatureSchema(_options);
            var migration = await SchemaMigration.DetermineAsync(conn, token, eventSchema.Objects).ConfigureAwait(false);
            await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: token).ConfigureAwait(false);

            _eventStoreEnsured = true;
        }
        finally
        {
            _eventStoreSemaphore.Release();
        }
    }
}
