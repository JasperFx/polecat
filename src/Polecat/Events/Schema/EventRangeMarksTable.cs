using Weasel.SqlServer;

namespace Polecat.Events.Schema;

/// <summary>
///     jasperfx#1215 / #1222: pc_event_range_marks, the event-range marks a stored-event rewrite and a projection
///     backfill keep their ceiling and their cursor in.
/// </summary>
/// <remarks>
///     <para>
///         A table of its own, like <see cref="EventProjectionFloorsTable" />, so a mark survives rebuild teardown
///         and every data-cleaning path (<c>Advanced.Clean</c>, <c>ResetAllData</c>). A mark says how far a job
///         got, which is not data derived from the events.
///     </para>
///     <para>
///         Keyed by the mark's key plus the tenant. <c>tenant_id</c> is part of the primary key and so cannot be
///         null: a store-global mark (a null tenant through <c>IEventDatabase</c>) is stored as the empty string,
///         which is never a tenant id. The key is <see cref="KeyLength" /> characters wide, which keeps the
///         primary key inside SQL Server's 900 byte limit with unicode keys.
///     </para>
/// </remarks>
internal class EventRangeMarksTable : Polecat.Storage.KeyedTable
{
    public const string TableName = "pc_event_range_marks";

    /// <summary>The widest key a mark can have, in characters.</summary>
    public const int KeyLength = 200;

    /// <summary>What a null tenant id is stored as. Not a valid tenant id anywhere in Polecat.</summary>
    public const string StoreGlobalTenant = "";

    public EventRangeMarksTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("mark_key", eventGraph.KeyText(KeyLength)).AsPrimaryKey().NotNull();
        AddColumn("tenant_id", eventGraph.KeyText()).AsPrimaryKey().NotNull();
        AddColumn("mark_seq_id", "bigint").NotNull();
        AddColumn("last_updated", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");
    }

    /// <summary>
    ///     Set the mark under <c>@key</c> and <c>@tenant</c> to <c>@mark</c> whether or not one is stored. Run on
    ///     its own, or inside the transaction of the page a cursor covers.
    /// </summary>
    public static string SetSql(string table) => $"""
        UPDATE {table} SET mark_seq_id = @mark, last_updated = SYSDATETIMEOFFSET()
        WHERE mark_key = @key AND tenant_id = @tenant;
        IF @@ROWCOUNT = 0
        BEGIN
            BEGIN TRY
                INSERT INTO {table} (mark_key, tenant_id, mark_seq_id) VALUES (@key, @tenant, @mark);
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                UPDATE {table} SET mark_seq_id = @mark, last_updated = SYSDATETIMEOFFSET()
                WHERE mark_key = @key AND tenant_id = @tenant;
            END CATCH
        END;
        """;

    public static string StoredTenant(string? tenantId)
        => string.IsNullOrEmpty(tenantId) ? StoreGlobalTenant : tenantId;

    public static void AssertKey(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (key.Length > KeyLength)
        {
            throw new ArgumentOutOfRangeException(nameof(key),
                $"An event-range mark key can be at most {KeyLength} characters, and '{key}' has {key.Length}.");
        }
    }
}
