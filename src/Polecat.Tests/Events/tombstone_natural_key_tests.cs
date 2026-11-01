using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #865: a natural key table has a cascading foreign key to <c>pc_streams</c> except under conjoined
///     tenancy, so a tombstone there has to take the stream's lookup rows itself. A row left behind
///     keeps naming a stream that no longer exists, reusing the key is refused as a live duplicate, and
///     the key's value outlives a delete someone may be relying on for erasure.
/// </summary>
public class tombstone_natural_key_tests : OneOffConfigurationsContext
{
    private const string Red = "Red";
    private const string Blue = "Blue";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync(StreamIdentity identity = StreamIdentity.AsGuid)
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.StreamIdentity = identity;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private IDocumentSession SessionFor(string tenant)
        => theStore.LightweightSession(new SessionOptions { TenantId = tenant });

    private async Task<int> KeyRowsAsync(OrderNumber number, string tenant)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*) FROM [{nameof(tombstone_natural_key_tests)}].[pc_natural_key_orderaggregate]
            WHERE natural_key_value = @value AND tenant_id = @tenant
            """;
        cmd.Parameters.AddWithValue("@value", number.Value);
        cmd.Parameters.AddWithValue("@tenant", tenant);
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task<string?> CustomerResolvedByAsync(OrderNumber number, string tenant)
    {
        await using var session = SessionFor(tenant);
        var order = await session.Events.FetchLatest<OrderAggregate, OrderNumber>(number, Ct);
        return order?.CustomerName;
    }

    [Fact]
    public async Task a_tombstoned_streams_natural_key_can_be_reused()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-TOMB-GUID");
        var tombstoned = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(tombstoned, new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream(tombstoned);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(number, "Second"));
            await session.SaveChangesAsync(Ct);
        }

        (await CustomerResolvedByAsync(number, Red)).ShouldBe("Second");
    }

    [Fact]
    public async Task a_tombstoned_string_streams_natural_key_can_be_reused()
    {
        await ConfigureAsync(StreamIdentity.AsString);
        var number = new OrderNumber("ORD-TOMB-STRING");

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream("tombstoned", new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream("tombstoned");
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream("replacement", new NkOrderCreated(number, "Second"));
            await session.SaveChangesAsync(Ct);
        }

        (await CustomerResolvedByAsync(number, Red)).ShouldBe("Second");
    }

    [Fact]
    public async Task the_key_can_move_to_a_new_stream_in_the_save_that_tombstones_the_old_one()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-TOMB-SAME-SAVE");
        var tombstoned = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(tombstoned, new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream(tombstoned);
            session.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(number, "Second"));
            await session.SaveChangesAsync(Ct);
        }

        (await CustomerResolvedByAsync(number, Red)).ShouldBe("Second");
    }

    [Fact]
    public async Task tombstoning_erases_the_natural_key_value()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-TOMB-ERASED");
        var tombstoned = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(tombstoned, new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync(number, Red)).ShouldBe(1);

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream(tombstoned);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync(number, Red)).ShouldBe(0);
    }

    [Fact]
    public async Task tombstoning_an_archived_stream_erases_its_natural_key_value_too()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-TOMB-ARCHIVED");
        var tombstoned = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(tombstoned, new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.ArchiveStream(tombstoned);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream(tombstoned);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync(number, Red)).ShouldBe(0);
    }

    [Fact]
    public async Task tombstoning_takes_only_the_natural_key_of_the_sessions_tenant()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-TOMB-TENANTS");

        // The same stream id AND the same key in both tenants, so only the tenant predicate separates them.
        var streamId = Guid.NewGuid();

        foreach (var tenant in new[] { Red, Blue })
        {
            await using var session = SessionFor(tenant);
            session.Events.StartStream(streamId, new NkOrderCreated(number, tenant));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync(number, Red)).ShouldBe(0);
        (await KeyRowsAsync(number, Blue)).ShouldBe(1);
        (await CustomerResolvedByAsync(number, Blue)).ShouldBe(Blue);
    }
}
