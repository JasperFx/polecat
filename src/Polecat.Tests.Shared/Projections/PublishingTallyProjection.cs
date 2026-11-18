using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events.Aggregation;
using Polecat.Projections;
using Polecat.Services;
using Polecat.TestUtils;

namespace Polecat.Tests.Projections;

public record RetriedTallied(Guid Id);
public record RetriedNotice(Guid Id);

/// <summary>
///     Changes itself in place, so an event applied twice shows up in the count.
/// </summary>
public partial class RetriedTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public void Apply(RetriedTallied e) => Count++;
}

public partial class PublishingTallyProjection : SingleStreamProjection<RetriedTally, Guid>
{
    public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<RetriedTally> slice)
    {
        if (slice.Snapshot is not null)
        {
            slice.PublishMessage(new RetriedNotice(slice.Snapshot.Id));
        }

        return ValueTask.CompletedTask;
    }
}
