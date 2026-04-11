using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Projections;

public class event_projection_enrichment_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task enrichment_sets_data_before_apply_inline()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add(new SimpleEnrichmentProjection(), ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var taskId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(taskId,
                new EnrichmentTaskAssigned { TaskId = taskId, UserId = Guid.NewGuid() });
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var summary = await query.LoadAsync<EnrichmentTaskSummary>(taskId);
        summary.ShouldNotBeNull();
        summary.AssignedUserName.ShouldBe("Enriched User");
    }

    [Fact]
    public async Task enrichment_is_called_before_apply()
    {
        var callOrder = new List<string>();
        ConfigureStore(opts =>
        {
            opts.Projections.Add(
                new EnrichmentCallOrderProjection(callOrder),
                ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new EnrichmentTaskAssigned { TaskId = streamId, UserId = Guid.NewGuid() });
        await session.SaveChangesAsync();

        callOrder.ShouldBe(new[] { "EnrichEventsAsync", "Apply:EnrichmentTaskAssigned" });
    }

    [Fact]
    public async Task enrichment_with_database_lookup_inline()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add(new DbLookupEnrichmentProjection(), ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        // Pre-store a lookup document
        var userId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new EnrichmentUser { Id = userId, Name = "Alice Smith" });
            await session.SaveChangesAsync();
        }

        var taskId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(taskId,
                new EnrichmentTaskAssigned { TaskId = taskId, UserId = userId });
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var summary = await query.LoadAsync<EnrichmentTaskSummary>(taskId);
        summary.ShouldNotBeNull();
        summary.AssignedUserName.ShouldBe("Alice Smith");
    }
}

#region Test Types

public class EnrichmentTaskAssigned
{
    public Guid TaskId { get; set; }
    public Guid UserId { get; set; }
    public string? UserName { get; set; }
}

public class EnrichmentTaskSummary
{
    public Guid Id { get; set; }
    public string? AssignedUserName { get; set; }
}

public class EnrichmentUser
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

#endregion

#region Projections

public class SimpleEnrichmentProjection : EventProjection
{
    public SimpleEnrichmentProjection()
    {
        Project<EnrichmentTaskAssigned>((e, ops) =>
        {
            ops.Store(new EnrichmentTaskSummary
            {
                Id = e.TaskId,
                AssignedUserName = e.UserName
            });
        });
    }

    public override Task EnrichEventsAsync(IQuerySession querySession,
        IReadOnlyList<IEvent> events, CancellationToken cancellation)
    {
        foreach (var e in events.OfType<IEvent<EnrichmentTaskAssigned>>())
        {
            e.Data.UserName = "Enriched User";
        }
        return Task.CompletedTask;
    }
}

public class EnrichmentCallOrderProjection : EventProjection
{
    private readonly List<string> _callOrder;

    public EnrichmentCallOrderProjection(List<string> callOrder)
    {
        _callOrder = callOrder;

        Project<EnrichmentTaskAssigned>((e, ops) =>
        {
            _callOrder.Add($"Apply:{nameof(EnrichmentTaskAssigned)}");
        });
    }

    public override Task EnrichEventsAsync(IQuerySession querySession,
        IReadOnlyList<IEvent> events, CancellationToken cancellation)
    {
        _callOrder.Add(nameof(EnrichEventsAsync));
        return Task.CompletedTask;
    }
}

public class DbLookupEnrichmentProjection : EventProjection
{
    public DbLookupEnrichmentProjection()
    {
        Project<EnrichmentTaskAssigned>((e, ops) =>
        {
            ops.Store(new EnrichmentTaskSummary
            {
                Id = e.TaskId,
                AssignedUserName = e.UserName
            });
        });
    }

    public override async Task EnrichEventsAsync(IQuerySession querySession,
        IReadOnlyList<IEvent> events, CancellationToken cancellation)
    {
        var assigned = events.OfType<IEvent<EnrichmentTaskAssigned>>().ToArray();
        if (assigned.Length == 0) return;

        var userIds = assigned.Select(e => e.Data.UserId).Distinct().ToArray();

        foreach (var userId in userIds)
        {
            var user = await querySession.LoadAsync<EnrichmentUser>(userId, cancellation);
            if (user != null)
            {
                foreach (var e in assigned.Where(a => a.Data.UserId == userId))
                {
                    e.Data.UserName = user.Name;
                }
            }
        }
    }
}

#endregion
