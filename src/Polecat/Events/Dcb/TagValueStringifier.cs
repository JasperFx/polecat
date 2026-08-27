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
}
