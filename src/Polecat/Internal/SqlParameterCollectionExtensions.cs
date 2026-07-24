using System.Data;
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
