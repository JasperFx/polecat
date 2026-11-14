using System.Globalization;
using System.Text;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat;
using Polecat.Events;
using Weasel.Core.Migrations;

namespace DcbLoadTest;

/// <summary>Command line: <c>&lt;scenario&gt; [--key value]...</c>.</summary>
public sealed class Settings
{
    public const string DefaultDatabase = "polecat_dcb_load";

    private readonly Dictionary<string, string> _values = new(StringComparer.OrdinalIgnoreCase);

    public Settings(string[] args)
    {
        Scenario = args.Length > 0 && !args[0].StartsWith("--") ? args[0] : "help";
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--")) continue;
            var key = args[i][2..];
            var value = i + 1 < args.Length && !args[i + 1].StartsWith("--") ? args[++i] : "true";
            _values[key] = value;
        }

        // The harness owns its catalog: every statistic is read as a delta, and another workload's
        // load in the same database would show up in it. POLECAT_DCB_LOAD_DATABASE is a full
        // connection string; without an Initial Catalog it still gets polecat_dcb_load.
        var builder = new SqlConnectionStringBuilder(
            Environment.GetEnvironmentVariable("POLECAT_DCB_LOAD_DATABASE")
            ?? "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Encrypt=False;TrustServerCertificate=True");
        if (string.IsNullOrEmpty(builder.InitialCatalog) ||
            builder.InitialCatalog.Equals("master", StringComparison.OrdinalIgnoreCase))
        {
            builder.InitialCatalog = Get("database", DefaultDatabase);
        }

        // 50 concurrent requests, each holding a connection across fetch + save, plus seeding writers.
        builder.MaxPoolSize = Math.Max(builder.MaxPoolSize, Int("max-pool", 400));
        if (builder.ConnectTimeout < 30) builder.ConnectTimeout = 30;
        DatabaseName = builder.InitialCatalog;
        ConnectionString = builder.ConnectionString;

        builder.InitialCatalog = "master";
        MasterConnectionString = builder.ConnectionString;
    }

    public string Scenario { get; }
    public string ConnectionString { get; }
    public string MasterConnectionString { get; }
    public string DatabaseName { get; }

    public string? OutPath => _values.TryGetValue("out", out var v) ? v : null;

    public string Get(string key, string fallback) => _values.TryGetValue(key, out var v) ? v : fallback;
    public int Int(string key, int fallback) => _values.TryGetValue(key, out var v) ? int.Parse(v) : fallback;

    public double Double(string key, double fallback)
        => _values.TryGetValue(key, out var v) ? double.Parse(v, CultureInfo.InvariantCulture) : fallback;

    public DcbConflictScope[] Scopes => Get("scopes", "TagValue,TagValueAndEventType").Split(',')
        .Select(Enum.Parse<DcbConflictScope>).ToArray();
}

public static class Stores
{
    public static string Short(DcbConflictScope scope) => scope switch
    {
        DcbConflictScope.TagValue => "tv",
        DcbConflictScope.TagValueRecordingEventTypes => "tvr",
        DcbConflictScope.TagValueAndEventType => "tvet",
        _ => scope.ToString()
    };

    public static DocumentStore Build(Settings settings, string schema, DcbConflictScope scope, bool writeRetries = true)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = settings.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
            opts.CommandTimeout = settings.Int("command-timeout", 300);
            opts.Events.DcbConflictScope = scope;

            // Polecat's commit "retries" are whatever StoreOptions.ResiliencePipeline holds. Its
            // default pipeline adds no strategy at all (PolecatResilienceDefaults, #724), so "on" is
            // the default pipeline and "off" an explicitly empty one: the two should measure the same,
            // and the column is there to show that nothing is absorbing deadlocks.
            if (!writeRetries) opts.ConfigurePolly(_ => { });

            opts.Events.AddEventType<OrderPlaced>();
            opts.Events.AddEventType<OrderShipped>();
            opts.Events.AddEventType<ClassScheduled>();
            opts.Events.AddEventType<SpotBooked>();
            opts.Events.AddEventType<WaitlistJoined>();

            opts.Events.RegisterTagType<CustomerId>("customer");
            opts.Events.RegisterTagType<RegionId>("region");
            opts.Events.RegisterTagType<ClassId>("class");
        });
    }

    /// <summary>
    /// Drop every pc_ object in the schema through the store's own tooling and build it again, so every
    /// measurement starts from empty tables (and from fresh sys.dm_db_index_operational_stats counters).
    /// </summary>
    public static async Task<DocumentStore> Fresh(Settings settings, string schema, DcbConflictScope scope,
        bool writeRetries = true)
    {
        var store = Build(settings, schema, scope, writeRetries);

        // Weasel's default migration logger prints every DDL statement to stdout, which would land in
        // the middle of the markdown tables.
        if (store.Database is IDatabaseWithMigrationLogger logged)
        {
            logged.MigrationLogger = new DefaultMigrationLogger(TextWriter.Null);
        }

        await store.Advanced.CompletelyRemoveAllAsync();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        return store;
    }
}

/// <summary>Thread-safe latency sample, in milliseconds.</summary>
public sealed class Latencies
{
    private readonly List<double> _values = new();

    public void Add(double ms)
    {
        lock (_values) _values.Add(ms);
    }

    public void Add(TimeSpan elapsed) => Add(elapsed.TotalMilliseconds);

    public void AddRange(Latencies other)
    {
        double[] copy;
        lock (other._values) copy = other._values.ToArray();
        lock (_values) _values.AddRange(copy);
    }

    public int Count
    {
        get { lock (_values) return _values.Count; }
    }

    public double Percentile(double p)
    {
        double[] sorted;
        lock (_values) sorted = _values.ToArray();
        if (sorted.Length == 0) return double.NaN;
        Array.Sort(sorted);
        var rank = (int)Math.Ceiling(p / 100.0 * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }

    public double Max
    {
        get
        {
            lock (_values) return _values.Count == 0 ? double.NaN : _values.Max();
        }
    }
}

/// <summary>
/// Cumulative counters from <c>sys.dm_db_index_operational_stats</c> for one table. Row counts come from
/// the heap or clustered index only (index_id &lt;= 1), so a row is counted once whatever indexes it has;
/// lock counters are summed over every index.
/// </summary>
public sealed record OpStats(
    long LeafInserts,
    long LeafUpdates,
    long LeafDeletes,
    long LeafGhosts,
    long LeafAllocations,
    long LockPromotions,
    long RowLockWaits,
    long RowLockWaitMs,
    long PageLockWaits,
    long PageLockWaitMs)
{
    public static readonly OpStats Zero = new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    public long RowWrites => LeafInserts + LeafUpdates;

    public OpStats Minus(OpStats before) => new(
        LeafInserts - before.LeafInserts,
        LeafUpdates - before.LeafUpdates,
        LeafDeletes - before.LeafDeletes,
        LeafGhosts - before.LeafGhosts,
        LeafAllocations - before.LeafAllocations,
        LockPromotions - before.LockPromotions,
        RowLockWaits - before.RowLockWaits,
        RowLockWaitMs - before.RowLockWaitMs,
        PageLockWaits - before.PageLockWaits,
        PageLockWaitMs - before.PageLockWaitMs);
}

/// <summary>
/// Leaf level of the heap / clustered index from <c>sys.dm_db_index_physical_stats</c>, plus sizes from
/// <c>sys.dm_db_partition_stats</c>. "Data" is the leaf level; "index" is everything else in use
/// (non-leaf levels and any nonclustered index).
/// </summary>
public sealed record PhysicalStats(
    long Rows,
    long LeafPages,
    double FragmentationPct,
    long GhostRecords,
    long VersionGhostRecords,
    double PageSpaceUsedPct,
    long ReservedBytes,
    long DataBytes,
    long IndexBytes);

public static class Sql
{
    public const string VersionTable = "pc_dcb_tag_version";

    public static async Task EnsureDatabase(Settings settings)
    {
        await using var conn = new SqlConnection(settings.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            IF DB_ID(@name) IS NULL
            BEGIN
                DECLARE @sql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@name);
                EXEC sp_executesql @sql;
                SET @sql = N'ALTER DATABASE ' + QUOTENAME(@name) + N' SET RECOVERY SIMPLE';
                EXEC sp_executesql @sql;
            END
            """;
        cmd.Parameters.AddWithValue("@name", settings.DatabaseName);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task Execute(Settings settings, string sql)
    {
        await using var conn = new SqlConnection(settings.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 0 };
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<long> Scalar(Settings settings, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(settings.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 0 };
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);
        var result = await cmd.ExecuteScalarAsync();
        return result is null or DBNull ? 0 : Convert.ToInt64(result);
    }

    public static string Qualified(string schema, string table) => $"[{schema}].[{table}]";

    public static Task<long> Count(Settings settings, string schema, string table)
        => Scalar(settings, $"select count_big(*) from {Qualified(schema, table)}");

    /// <summary>
    /// SQL Server's cumulative deadlock count. SERVER-WIDE: despite the name the raw value is a running
    /// total, but it counts every database on the instance, so anything else using the server shows up.
    /// </summary>
    public static Task<long> ServerDeadlocks(Settings settings) => Scalar(settings, """
        select cntr_value from sys.dm_os_performance_counters
        where object_name like '%:Locks' and rtrim(counter_name) = 'Number of Deadlocks/sec' and rtrim(instance_name) = '_Total'
        """);

    public static async Task<DateTime> ServerUtcNow(Settings settings)
    {
        await using var conn = new SqlConnection(settings.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("select sysutcdatetime()", conn);
        return (DateTime)(await cmd.ExecuteScalarAsync())!;
    }

    /// <summary>
    /// <c>xml_deadlock_report</c> events in the system_health session since <paramref name="sinceUtc"/> that
    /// name this database. Per-database, unlike the performance counter, but system_health dispatches
    /// its buffers lazily (up to 120 s), so a fresh deadlock can be missing. -1 when unreadable.
    /// </summary>
    public static async Task<long> SystemHealthDeadlocks(Settings settings, DateTime sinceUtc)
    {
        try
        {
            return await Scalar(settings, """
                select count(*) from sys.fn_xe_file_target_read_file('system_health*.xel', null, null, null)
                where object_name = 'xml_deadlock_report' and timestamp_utc > @since
                  and cast(event_data as nvarchar(max)) like @db
                """, ("@since", sinceUtc), ("@db", $"%currentdbname=\"{settings.DatabaseName}\"%"));
        }
        catch
        {
            return -1;
        }
    }

    public static async Task<OpStats> Operational(Settings settings, string schema, string table = VersionTable)
    {
        // OBJECT_ID null would make the DMF report EVERY object in the database, so guard it.
        await using var conn = new SqlConnection(settings.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("""
            declare @id int = object_id(@name);
            if @id is null
                select cast(0 as bigint), cast(0 as bigint), cast(0 as bigint), cast(0 as bigint), cast(0 as bigint),
                       cast(0 as bigint), cast(0 as bigint), cast(0 as bigint), cast(0 as bigint), cast(0 as bigint)
            else
                select isnull(sum(case when index_id <= 1 then leaf_insert_count end), 0),
                       isnull(sum(case when index_id <= 1 then leaf_update_count end), 0),
                       isnull(sum(case when index_id <= 1 then leaf_delete_count end), 0),
                       isnull(sum(case when index_id <= 1 then leaf_ghost_count end), 0),
                       isnull(sum(case when index_id <= 1 then leaf_allocation_count end), 0),
                       isnull(sum(index_lock_promotion_count), 0),
                       isnull(sum(row_lock_wait_count), 0), isnull(sum(row_lock_wait_in_ms), 0),
                       isnull(sum(page_lock_wait_count), 0), isnull(sum(page_lock_wait_in_ms), 0)
                from sys.dm_db_index_operational_stats(db_id(), @id, null, null)
            """, conn);
        cmd.Parameters.AddWithValue("@name", Qualified(schema, table));
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        return new OpStats(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
            reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8),
            reader.GetInt64(9));
    }

    /// <summary>
    /// Physical shape of a table. <paramref name="mode"/> is passed to sys.dm_db_index_physical_stats:
    /// DETAILED reads every page (ghost counts and page fullness need SAMPLED or DETAILED).
    /// </summary>
    public static async Task<PhysicalStats> Physical(Settings settings, string schema, string table = VersionTable,
        string mode = "DETAILED")
    {
        await using var conn = new SqlConnection(settings.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("""
            declare @id int = object_id(@name);
            select isnull(sum(case when index_id <= 1 then row_count end), 0),
                   isnull(sum(reserved_page_count), 0) * 8192,
                   isnull(sum(case when index_id <= 1 then in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count end), 0) * 8192,
                   isnull(sum(used_page_count), 0) * 8192
            from sys.dm_db_partition_stats where object_id = @id;

            if @id is not null
                select isnull(sum(page_count), 0),
                       isnull(max(avg_fragmentation_in_percent), 0),
                       isnull(sum(ghost_record_count), 0),
                       isnull(sum(version_ghost_record_count), 0),
                       isnull(avg(avg_page_space_used_in_percent), 0)
                from sys.dm_db_index_physical_stats(db_id(), @id, null, null, @mode)
                where index_id <= 1 and index_level = 0 and alloc_unit_type_desc = 'IN_ROW_DATA';
            """, conn) { CommandTimeout = 0 };
        cmd.Parameters.AddWithValue("@name", Qualified(schema, table));
        cmd.Parameters.AddWithValue("@mode", mode);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        var rows = reader.GetInt64(0);
        var reserved = reader.GetInt64(1);
        var data = reader.GetInt64(2);
        var used = reader.GetInt64(3);

        long pages = 0, ghosts = 0, versionGhosts = 0;
        double fragmentation = 0, spaceUsed = 0;
        if (await reader.NextResultAsync() && await reader.ReadAsync())
        {
            pages = reader.GetInt64(0);
            fragmentation = reader.GetDouble(1);
            ghosts = reader.GetInt64(2);
            versionGhosts = reader.GetInt64(3);
            spaceUsed = reader.GetDouble(4);
        }

        return new PhysicalStats(rows, pages, fragmentation, ghosts, versionGhosts, spaceUsed, reserved, data,
            used - data);
    }

    public static string ServerDescription(Settings settings)
    {
        try
        {
            using var conn = new SqlConnection(settings.ConnectionString);
            conn.Open();
            using var cmd = new SqlCommand("""
                select convert(nvarchar(128), serverproperty('ProductVersion')),
                       convert(nvarchar(128), serverproperty('Edition')),
                       d.name, d.is_read_committed_snapshot_on, d.snapshot_isolation_state_desc,
                       d.is_accelerated_database_recovery_on, d.compatibility_level
                from sys.databases d where d.database_id = db_id()
                """, conn);
            using var r = cmd.ExecuteReader();
            r.Read();
            return $"SQL Server {r.GetString(0)} {r.GetString(1)}, db {r.GetString(2)}, " +
                   $"RCSI {(r.GetBoolean(3) ? "on" : "off")}, snapshot {r.GetString(4)}, " +
                   $"ADR {(r.GetBoolean(5) ? "on" : "off")}, compat {r.GetByte(6)}";
        }
        catch (Exception e)
        {
            return "unreachable: " + e.Message;
        }
    }
}

/// <summary>Markdown output, mirrored to <c>--out</c> when given.</summary>
public static class Report
{
    private static string? _outPath;

    public static void Open(Settings settings)
    {
        _outPath = settings.OutPath;
        if (_outPath != null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(_outPath))!);
        }
    }

    public static void Line(string text = "")
    {
        Console.WriteLine(text);
        if (_outPath != null) File.AppendAllText(_outPath, text + Environment.NewLine);
    }

    /// <summary>Progress chatter: console only.</summary>
    public static void Progress(string text) => Console.WriteLine("    " + text);

    public static void Table(string[] headers, IEnumerable<string[]> rows)
    {
        var sb = new StringBuilder();
        sb.Append("| ").Append(string.Join(" | ", headers)).AppendLine(" |");
        sb.Append('|').Append(string.Join("|", headers.Select(_ => "---"))).AppendLine("|");
        foreach (var row in rows)
        {
            sb.Append("| ").Append(string.Join(" | ", row)).AppendLine(" |");
        }

        Line(sb.ToString());
    }

    private static int MinThreads()
    {
        ThreadPool.GetMinThreads(out var worker, out _);
        return worker;
    }

    public static string Ms(double ms) => double.IsNaN(ms) ? "-" : ms.ToString(ms < 10 ? "N2" : "N1");
    public static string N0(double value) => value.ToString("N0");
    public static string Mb(long bytes) => (bytes / 1024.0 / 1024.0).ToString("N1");
    public static string Pct(double ratio) => double.IsNaN(ratio) ? "-" : (ratio * 100).ToString("N1") + "%";

    public static void Header(Settings settings)
    {
        Line($"_Run {DateTime.Now:yyyy-MM-dd HH:mm}, {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}, " +
             $"{Environment.ProcessorCount} cores, min threads {MinThreads()}, {Sql.ServerDescription(settings)}_");
        Line();
    }
}

public static class Errors
{
    /// <summary>Walk wrappers (AggregateException, InnerException chains) down to what was actually thrown.</summary>
    public static IEnumerable<Exception> Chain(Exception e)
    {
        var stack = new Stack<Exception>();
        stack.Push(e);
        while (stack.Count > 0)
        {
            var current = stack.Pop();
            yield return current;
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions) stack.Push(inner);
            }
            else if (current.InnerException != null)
            {
                stack.Push(current.InnerException);
            }
        }
    }

    public static bool Is<T>(Exception e) where T : Exception => Chain(e).Any(x => x is T);

    /// <summary>Every SQL Server error number anywhere in the chain.</summary>
    public static IEnumerable<int> SqlNumbers(Exception e)
        => Chain(e).OfType<SqlException>().SelectMany(x => x.Errors.Cast<SqlError>().Select(err => err.Number).Append(x.Number));

    public static bool IsDeadlock(Exception e) => SqlNumbers(e).Contains(1205);

    public static string Describe(Exception e)
    {
        var number = SqlNumbers(e).FirstOrDefault();
        var innermost = Chain(e).Last();
        return number != 0 ? $"SQL {number}" : innermost.GetType().Name;
    }
}
