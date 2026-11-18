using Weasel.SqlServer;

namespace Polecat.Events.Schema;

/// <summary>
///     #1054 (marten#6007): <c>pc_high_water_skips</c>, one row for every sequence range the high water mark moved
///     past without every number in it holding a committed event: why it moved, and which numbers were still
///     missing. The daemon checks those numbers for events that commit late. See
///     <see cref="Daemon.LateEventRecovery" />.
/// </summary>
/// <remarks>
///     <para>
///         Keyed by the ending sequence and the tenant. Under per-tenant partitioned events every tenant draws
///         from its own sequence, so two tenants can skip ranges that end at the same number. The store-global
///         mark is stored with the empty tenant, as the projection floors are.
///     </para>
///     <para>
///         <c>gaps</c> is a JSON array of inclusive pairs, <c>[[first, last], ...]</c>: the numbers inside the
///         range with no committed event when it was skipped. It is <c>nvarchar(max)</c> rather than the native
///         <c>json</c> type on purpose, so nothing here ever goes near a <c>JSON_MODIFY</c> on a native column
///         (#907).
///     </para>
/// </remarks>
internal class HighWaterSkipsTable : Polecat.Storage.KeyedTable
{
    public const string TableName = "pc_high_water_skips";

    /// <summary>The tenant of a skip of the store-global mark, which covers every tenant.</summary>
    public const string GlobalTenant = "";

    public HighWaterSkipsTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("ending_sequence", "bigint").AsPrimaryKey().NotNull();
        AddColumn("tenant_id", eventGraph.KeyText()).AsPrimaryKey().NotNull();
        AddColumn("starting_sequence", "bigint").NotNull();
        AddColumn("timestamp", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        // Why the range was skipped (HighWaterSkipReason), the numbers in it that had no committed event, and
        // when the daemon stopped checking those numbers
        AddColumn("reason", "varchar(50)").AllowNulls();
        AddColumn("gaps", "nvarchar(max)").AllowNulls();
        AddColumn("closed_at", "datetimeoffset").AllowNulls();
    }
}

/// <summary>
///     #1054 (marten#6007): <c>pc_high_water_late_events</c>, one row for every event that committed inside a
///     sequence range the high water mark had already skipped, so async projections and subscriptions that were
///     past it never saw it.
/// </summary>
internal class HighWaterLateEventsTable : Polecat.Storage.KeyedTable
{
    public const string TableName = "pc_high_water_late_events";

    public HighWaterLateEventsTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("seq_id", "bigint").AsPrimaryKey().NotNull();
        AddColumn("tenant_id", eventGraph.KeyText()).AsPrimaryKey().NotNull();
        AddColumn("stream_id", eventGraph.KeyText()).NotNull();
        AddColumn("event_type", "varchar(500)").NotNull();
        AddColumn("skip_starting_sequence", "bigint").NotNull();
        AddColumn("skip_ending_sequence", "bigint").NotNull();
        AddColumn("skip_reason", "varchar(50)").AllowNulls();
        AddColumn("detected_at", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        // A JSON array of the async single-stream projections that have taken the event in, by rebuilding its
        // stream, or that do not handle it. nvarchar(max) for the reason on HighWaterSkipsTable.
        AddColumn("repaired_projections", "nvarchar(max)").AllowNulls();
    }
}

/// <summary>
///     #1054 (marten#6007): why the high water mark skipped a sequence range, as recorded in
///     <c>pc_high_water_skips.reason</c>.
/// </summary>
internal enum HighWaterSkipReason
{
    /// <summary>No append that could fill the range was in flight. An event committing inside it is a detector bug.</summary>
    Proven,

    /// <summary>An append that could fill the range was in flight, and <c>SkipStaleGapsDespiteLiveTransactionsAfter</c> expired.</summary>
    LiveTransaction,

    /// <summary>Skipped on the stale threshold alone. Polecat's detector always has transaction evidence, so it never records this; kept so the names match Marten's.</summary>
    WallClock,

    /// <summary>An operator advanced the mark to the committed ceiling.</summary>
    Manual
}
