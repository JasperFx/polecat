using JasperFx.Events.Aggregation;

namespace Polecat.Projections;

/// <summary>
///     Base class for multi-stream projections that aggregate events from
///     multiple event streams into a single document. Uses Identity/Identities
///     to route events to the correct aggregate by ID.
///
///     Usage:
///     <code>
///     public class CustomerSummaryProjection : MultiStreamProjection&lt;CustomerSummary, Guid&gt;
///     {
///         public CustomerSummaryProjection()
///         {
///             Identity&lt;OrderPlaced&gt;(e => e.CustomerId);
///             Identity&lt;PaymentReceived&gt;(e => e.CustomerId);
///         }
///         public static CustomerSummary Create(OrderPlaced e) => new() { ... };
///         public void Apply(PaymentReceived e, CustomerSummary doc) { ... }
///     }
///     </code>
/// </summary>
/// <typeparam name="TDoc">The aggregate document type.</typeparam>
/// <typeparam name="TId">The identity type used to route events to aggregates.</typeparam>
public abstract class MultiStreamProjection<TDoc, TId>
    : JasperFxMultiStreamProjectionBase<TDoc, TId, IDocumentSession, IQuerySession>
    where TDoc : notnull
    where TId : notnull
{
}
