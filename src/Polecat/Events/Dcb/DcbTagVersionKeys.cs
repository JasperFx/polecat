namespace Polecat.Events.Dcb;

/// <summary>
///     #973: the <c>pc_dcb_tag_version.tag_table</c> discriminator for a per-event-type version.
/// </summary>
/// <remarks>
///     <para>
///         A tag value's versions share one table. The tag type's table suffix keys the version every
///         event carrying the value moves, and <c>{suffix}:{event type name}</c> keys the one only events of
///         that type move. A table suffix is a SQL identifier and never contains a colon, so the two can't
///         collide, and the second kind needs no schema change. This is the same key Marten uses
///         (marten#5887).
///     </para>
///     <para>
///         The event type name is the alias stored in <c>pc_events.type</c>, which is what a condition's
///         event type filters on when the boundary reads, so the version checked and the events read always
///         describe the same set. <see cref="StreamTagVersionBump" /> builds the same key in SQL from that
///         column, and the two must agree.
///     </para>
/// </remarks>
internal static class DcbTagVersionKeys
{
    /// <summary>The width of <c>pc_dcb_tag_version.tag_table</c>.</summary>
    public const int MaxLength = 250;

    public const char Separator = ':';

    public static string ForEventType(string tagTableSuffix, string eventTypeName)
    {
        var key = tagTableSuffix + Separator + eventTypeName;

        // Refused rather than truncated: two long event type names sharing a prefix would truncate to one
        // key, and an INSERT would fail on the column anyway. pc_events.type is wider than this column.
        if (key.Length > MaxLength)
        {
            throw new InvalidOperationException(
                $"The DCB version key '{key}' for event type '{eventTypeName}' is longer than the {MaxLength} " +
                $"characters pc_dcb_tag_version.tag_table holds. Give the event type a shorter alias, or set " +
                $"opts.Events.DcbConflictScope = DcbConflictScope.TagValue, which keeps no per-event-type versions.");
        }

        return key;
    }
}
