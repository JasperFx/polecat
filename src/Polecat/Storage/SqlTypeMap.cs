namespace Polecat.Storage;

/// <summary>
///     Single source of truth mapping a CLR member type to the SQL Server type used both by the
///     LINQ translator's CAST/CONVERT locators and by computed-column index definitions. Keeping
///     one map is what lets an Index(...) computed column line up with the predicate the translator
///     emits, so SQL Server can actually seek the index (#223).
/// </summary>
internal static class SqlTypeMap
{
    /// <summary>
    ///     The SQL type for a CLR type, or null for types the translator leaves uncast
    ///     (string, bool — JSON_VALUE already returns nvarchar for these).
    /// </summary>
    public static string? For(Type type)
    {
        if (type == typeof(int)) return "int";
        if (type == typeof(long)) return "bigint";
        if (type == typeof(short)) return "smallint";
        if (type == typeof(double)) return "float";
        if (type == typeof(decimal)) return "decimal(18,6)";
        if (type == typeof(float)) return "real";
        if (type == typeof(Guid)) return "uniqueidentifier";
        if (type == typeof(DateTime)) return "datetime2";
        if (type == typeof(DateTimeOffset)) return "datetimeoffset";
        if (type == typeof(DateOnly)) return "date";
        if (type == typeof(TimeOnly)) return "time";
        return null; // string, bool, etc. — no CAST needed
    }

    /// <summary>
    ///     True when the SQL type is a date/time family that needs a deterministic CONVERT(..., 126)
    ///     (rather than CAST) to be usable in a PERSISTED computed column. CAST of a string to a
    ///     date/time type is non-deterministic and cannot be persisted.
    /// </summary>
    public static bool IsDateTimeFamily(string sqlType)
    {
        var t = sqlType.Trim().ToLowerInvariant();
        return t is "datetimeoffset" or "datetime2" or "datetime" or "smalldatetime"
            or "date" or "time";
    }
}
