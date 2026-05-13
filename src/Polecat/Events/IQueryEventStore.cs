using JasperFx.Events;
using Polecat.Linq;

namespace Polecat.Events;

/// <summary>
///     Polecat's read-side event-store API.
/// </summary>
/// <remarks>
/// Polecat 4 dedupe pillar: the database-agnostic surface (FetchStreamAsync,
/// AggregateStreamAsync, AggregateStreamToLastKnownAsync, LoadAsync,
/// FetchStreamStateAsync) now lives in <see cref="JasperFx.Events.IQueryEventStore"/>.
/// This interface adds the Polecat-specific LINQ-returning methods that return
/// <see cref="IPolecatQueryable{T}"/>.
/// </remarks>
public interface IQueryEventStore : JasperFx.Events.IQueryEventStore
{
    /// <summary>
    ///     Query directly against ONLY the raw event data for a specific event type.
    ///     Warning: this searches the entire event table and is primarily intended
    ///     for diagnostics and troubleshooting.
    /// </summary>
    IPolecatQueryable<T> QueryRawEventDataOnly<T>() where T : class;

    /// <summary>
    ///     Query directly against the raw event data across all event types.
    ///     Returns IEvent wrappers with full metadata.
    /// </summary>
    IPolecatQueryable<IEvent> QueryAllRawEvents();
}
