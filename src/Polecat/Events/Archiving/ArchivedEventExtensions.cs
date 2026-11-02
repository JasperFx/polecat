using JasperFx.Events;

namespace Polecat.Events.Archiving;

/// <summary>
///     The event predicate that opts in to archived events.
/// </summary>
/// <remarks>
///     Name and namespace shape match Marten's and Fisher's, so a query ports between the stores by
///     changing a <c>using</c>.
/// </remarks>
public static class ArchivedEventExtensions
{
    /// <summary>
    ///     Query for events whether or not they are archived, dropping the implicit
    ///     <c>is_archived = 0</c> filter. A marker for the predicate translator.
    /// </summary>
    public static bool MaybeArchived(this IEvent @event) => true;
}
