using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     #363: typed replacements for <c>AddWithValue</c> on values compared against Polecat's
///     <c>varchar</c> columns (ids, stream ids, tenant ids, names). <c>AddWithValue</c> binds .NET
///     strings as <c>nvarchar</c>, which forces CONVERT_IMPLICIT onto the varchar column side and
///     turns index seeks into full scans under SQL collations. The fixed 8000 size keeps one plan
///     per statement instead of one per distinct value length.
/// </summary>
internal static class SqlParameterCollectionExtensions
{
    /// <summary>Bind a string compared against a varchar column (id, tenant_id, name, type).</summary>
    public static SqlParameter AddVarChar(this SqlParameterCollection parameters, string name, string? value)
    {
        var parameter = new SqlParameter(name, SqlDbType.VarChar, 8000)
        {
            Value = (object?)value ?? DBNull.Value
        };
        parameters.Add(parameter);
        return parameter;
    }

    /// <summary>
    ///     How to bind an id or stream key that nothing has checked against the column: a batched read, which
    ///     is written synchronously, or a diagnostic read, which has no session to ask with. Outside ASCII it
    ///     stays <c>nvarchar</c> and the comparison is exact (#919). A <c>varchar</c> parameter would turn
    ///     <c>東京</c> into <c>??</c> and find whatever else did. It costs such an id the index seek under a
    ///     SQL collation, the trade #877 made on the LINQ path for the same reason.
    /// </summary>
    public static SqlDbType? BindingForKey(object? id)
        => id is string text && Ascii.IsValid(text) ? SqlDbType.VarChar : null;

    /// <inheritdoc cref="BindingForKey" />
    public static SqlParameter AddUncheckedKey(this SqlParameterCollection parameters, string name, object id)
        => id is string text && !Ascii.IsValid(text)
            ? parameters.AddWithValue(name, text)
            : parameters.AddIdParameter(name, id);

    /// <summary>
    ///     Bind a stream/document identity whose runtime type is Guid or string (the two stream
    ///     identity modes). Strings bind as varchar to match the varchar(250) id columns.
    /// </summary>
    public static SqlParameter AddIdParameter(this SqlParameterCollection parameters, string name, object id)
    {
        return id switch
        {
            string text => parameters.AddVarChar(name, text),
            _ => parameters.AddWithValue(name, id)
        };
    }
}
