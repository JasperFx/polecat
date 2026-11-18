using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

/// <summary>
///     Self-aggregating document type for testing inline projections.
///     Uses conventional Apply/Create methods.
/// </summary>
public partial class QuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public string? Location { get; set; }
    public List<string> MonstersSlain { get; set; } = new();
    public bool IsFinished { get; set; }
    public int Version { get; set; }

    // Create: called for the first event in a new stream
    public static QuestParty Create(QuestStarted e)
    {
        return new QuestParty { Name = e.Name };
    }

    // Apply: called for subsequent events
    public void Apply(MembersJoined e)
    {
        Members.AddRange(e.Members);
        Location = e.Location;
    }

    public void Apply(MembersDeparted e)
    {
        foreach (var m in e.Members) Members.Remove(m);
    }

    public void Apply(ArrivedAtLocation e)
    {
        Location = e.Location;
    }

    public void Apply(MonsterSlain e)
    {
        MonstersSlain.Add(e.Name);
    }

    public bool ShouldDelete(QuestEnded e)
    {
        return true;
    }
}
