using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public class QuestLog
{
    public Guid Id { get; set; }
    public string QuestName { get; set; } = string.Empty;
    public string? Location { get; set; }
    public int MemberCount { get; set; }
}

public partial class QuestLogProjection : EventProjection
{
    public void Project(QuestStarted e, IDocumentSession ops)
    {
        ops.Store(new QuestLog { Id = Guid.NewGuid(), QuestName = e.Name });
    }
}

public partial class MultiEventQuestLogProjection : EventProjection
{
    // Store a QuestLog keyed by stream ID via event data
    public void Project(QuestStarted e, IDocumentSession ops)
    {
        ops.Store(new QuestLog { Id = Guid.NewGuid(), QuestName = e.Name });
    }

    public void Project(MembersJoined e, IDocumentSession ops)
    {
        ops.Store(new QuestLog
        {
            Id = Guid.NewGuid(),
            QuestName = "join",
            Location = e.Location,
            MemberCount = e.Members.Length
        });
    }
}
