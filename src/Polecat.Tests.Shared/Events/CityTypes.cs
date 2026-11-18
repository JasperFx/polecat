using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

public record CityFounded(string City);

public record CityVisited(string City);

public class CityLedger
{
    public string Id { get; set; } = string.Empty;
    public int Visits { get; set; }

    public static CityLedger Create(IEvent<CityFounded> e) => new() { Id = e.StreamKey! };
    public void Apply(CityVisited _) => Visits++;
}

public class CityVisits
{
    public string Id { get; set; } = string.Empty;
    public int Count { get; set; }
}

public class CityTrail
{
    public string Id { get; set; } = string.Empty;
    public int Visits { get; set; }
}

/// <summary>
///     Raises onto a stream named by the event, which nothing has looked at. Its own document is keyed by
///     the stream it's reading, so the raised key is the only one in this batch that can be lost.
/// </summary>
public partial class CityTrailProjection: SingleStreamProjection<CityTrail, string>
{
    public void Apply(CityTrail view, CityVisited _) => view.Visits++;

    public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<CityTrail> slice)
    {
        foreach (var visit in slice.Events().Select(x => x.Data).OfType<CityVisited>())
        {
            slice.AppendEvent(visit.City, new CityFounded(visit.City));
        }

        return ValueTask.CompletedTask;
    }
}

public partial class CityVisitsProjection: MultiStreamProjection<CityVisits, string>
{
    public CityVisitsProjection()
    {
        Identity<CityVisited>(x => x.City);
    }

    public void Apply(CityVisits view, CityVisited _) => view.Count++;
}
