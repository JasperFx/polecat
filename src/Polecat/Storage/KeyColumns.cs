using System.Data;

namespace Polecat.Storage;

/// <summary>
///     #935: the type of a column holding text the application supplies (a tenant id, a string id or stream
///     key, a tag value, correlation and causation ids), which is <c>nvarchar</c> under
///     <see cref="AdvancedOptions.UseUnicodeKeys" /> and <c>varchar</c> otherwise. Names Polecat generates
///     itself (progression names, event type names, tag table names) stay <c>varchar</c> either way.
/// </summary>
internal static class KeyColumns
{
    /// <summary>The width of a key column, in characters.</summary>
    public const int Width = 250;

    public static string Text(bool unicode, int length = Width) => unicode ? $"nvarchar({length})" : $"varchar({length})";

    public static SqlDbType DbType(bool unicode) => unicode ? SqlDbType.NVarChar : SqlDbType.VarChar;

    /// <summary>
    ///     The size to give a parameter of <see cref="DbType" />, the largest that isn't <c>max</c>, so a
    ///     statement has one plan rather than one per value length.
    /// </summary>
    public static int ParameterSize(bool unicode) => unicode ? 4000 : 8000;

    /// <summary>Whether a column of this SQL type is one of the unicode ones.</summary>
    public static bool IsUnicode(string? sqlType)
        => sqlType != null && (sqlType.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase)
                               || sqlType.StartsWith("nchar", StringComparison.OrdinalIgnoreCase));
}
