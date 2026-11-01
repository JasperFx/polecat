using System.Globalization;

namespace Polecat.Events.Dcb;

/// <summary>
///     Converts a tag value (the primitive returned by
///     <see cref="JasperFx.Events.Tags.ITagTypeRegistration.ExtractValue" />) to its canonical string
///     form for storage in the heterogeneous <c>pc_dcb_tag_version</c> side table. That table is keyed
///     across every registered tag type at once, so values are stringified rather than stored in the
///     native types their own <c>pc_event_tag_*</c> tables use.
/// </summary>
/// <remarks>
///     Stable formatting matters: the same tag value has to produce the same string across processes
///     and across .NET runtimes, or two concurrent appenders will lock different rows and neither will
///     see the other. Mirrors Marten's stringifier so the two stores agree on the encoding.
/// </remarks>
internal static class TagValueStringifier
{
    public static string Stringify(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return value switch
        {
            string s => s,
            Guid g => g.ToString("d"),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }

    /// <summary>
    ///     <see cref="Stringify" /> as a SQL expression over a <c>pc_event_tag_*.value</c> column, for a
    ///     statement that has to reach <c>pc_dcb_tag_version</c> rows from the tag rows themselves. The
    ///     two have to produce the same string or the statement lands on a row nobody reads.
    /// </summary>
    public static string ToSql(Type simpleType, string column)
    {
        if (simpleType == typeof(string)) return column;

        // CONVERT renders a uniqueidentifier in upper case, and Guid.ToString("d") in lower.
        if (simpleType == typeof(Guid)) return $"LOWER(CONVERT(varchar(36), {column}))";

        return $"CONVERT(varchar(20), {column})";
    }
}
