using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Projections;

public record Teardown439Registered(string Name, string Category);

public class Teardown439Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
}

public class Teardown439Metric
{
    public Guid Id { get; set; }
    public double Price { get; set; }
}

public partial class Teardown439ProductProjection : SingleStreamProjection<Teardown439Product, Guid>
{
    public Teardown439ProductProjection()
    {
        Name = "Teardown439Product";
    }

    public void Apply(Teardown439Registered e, Teardown439Product view)
    {
        view.Name = e.Name;
        view.Category = e.Category;
    }
}

/// <summary>
///     A raw <see cref="IProjection" />: it declares neither its storage nor its teardown, which is
///     exactly the shape #439 is about.
/// </summary>
public class Teardown439MetricProjection : IProjection
{
    public Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        foreach (var e in events)
        {
            if (e.Data is Teardown439Registered)
            {
                operations.Store(new Teardown439Metric { Id = e.StreamId, Price = 1 });
            }
        }

        return Task.CompletedTask;
    }
}
