using JasperFx.Events.Aggregation;

namespace Polecat.Projections;

/// <summary>
///     Base class for single-stream projections that build an aggregate document
///     from events in a single stream. Uses conventional Apply/Create/ShouldDelete
///     method discovery from JasperFx.Events.
///
///     Usage:
///     <code>
///     public class QuestPartyProjection : SingleStreamProjection&lt;QuestParty&gt;
///     {
///         public static QuestParty Create(QuestStarted e) => new() { Name = e.Name };
///         public void Apply(MembersJoined e, QuestParty party) => party.Members.AddRange(e.Members);
///         public bool ShouldDelete(QuestEnded e) => true;
///     }
///     </code>
/// </summary>
/// <typeparam name="TDoc">The aggregate document type. Must have a Guid Id property.</typeparam>
public class SingleStreamProjection<TDoc>
    : JasperFxSingleStreamProjectionBase<TDoc, Guid, IDocumentSession, IQuerySession>
    where TDoc : notnull
{
}
