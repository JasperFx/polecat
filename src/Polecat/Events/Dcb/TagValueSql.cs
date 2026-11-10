namespace Polecat.Events.Dcb;

/// <summary>
///     The SQL Server half of tag value stringification: the expression that renders a native
///     <c>pc_event_tag_*</c> value exactly as <see cref="JasperFx.Events.Tags.TagValueStringifier" />
///     renders it in .NET, so a set-based statement keys the <c>pc_dcb_tag_version</c> side table the
///     same way a single append does. The .NET half moved to JasperFx.Events (jasperfx#1044, polecat#806).
/// </summary>
internal static class TagValueSql
{
    public static string ToSql(Type simpleType, string column)
    {
        if (simpleType == typeof(string)) return column;

        // CONVERT renders a uniqueidentifier in upper case, and Guid.ToString("d") in lower.
        if (simpleType == typeof(Guid)) return $"LOWER(CONVERT(varchar(36), {column}))";

        return $"CONVERT(varchar(20), {column})";
    }
}
