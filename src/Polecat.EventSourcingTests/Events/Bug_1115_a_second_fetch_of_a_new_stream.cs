using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #1115 (port of marten#6094): a fetch for writing that finds the stream missing reuses a Start action the session
///     already holds for that id and tenant. It used to add a second one, so <c>Events.Append(id, ...)</c> went to the
///     first, the second fetch's handle wrote to the second, and <c>SaveChangesAsync</c> started the stream twice.
/// </summary>
public class Bug_1115_a_second_fetch_of_a_new_stream : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task configureAsync(StreamIdentity identity = StreamIdentity.AsGuid, bool conjoined = false)
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = identity;
            if (conjoined) opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.AddEventType<SharedIdHappened>();
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<IReadOnlyList<IEvent>> readAsync(Guid id, string? tenantId = null)
    {
        await using var query = tenantId == null ? theStore.QuerySession() : theStore.QuerySession(tenantId);
        return await query.Events.FetchStreamAsync(id, token: Ct);
    }

    [Fact]
    public async Task fetching_a_new_stream_twice_starts_it_once()
    {
        await configureAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            var first = await session.Events.FetchForWriting<SharedIdAggregate>(id, Ct);
            var second = await session.Events.FetchForWriting<SharedIdAggregate>(id, Ct);

            first.CurrentVersion.ShouldBe(0);
            second.CurrentVersion.ShouldBe(0);

            session.Events.Append(id, new SharedIdHappened("by id"));
            second.AppendOne(new SharedIdHappened("by handle"));

            session.PendingChanges.Streams.Count(x => x.Id == id).ShouldBe(1);

            await session.SaveChangesAsync(Ct);
        }

        var events = await readAsync(id);
        events.Select(x => ((SharedIdHappened)x.Data).Note).ShouldBe(["by id", "by handle"]);
        events.Select(x => x.Version).ShouldBe([1L, 2L]);
    }

    [Fact]
    public async Task fetching_a_new_string_stream_twice_starts_it_once()
    {
        await configureAsync(StreamIdentity.AsString);
        var key = Guid.NewGuid().ToString();

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.FetchForWriting<SharedKeyAggregate>(key, Ct);
            var second = await session.Events.FetchForWriting<SharedKeyAggregate>(key, Ct);
            second.CurrentVersion.ShouldBe(0);

            session.Events.Append(key, new SharedIdHappened("by key"));
            second.AppendOne(new SharedIdHappened("by handle"));

            session.PendingChanges.Streams.Count(x => x.Key == key).ShouldBe(1);

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var aggregate = await query.Events.AggregateStreamAsync<SharedKeyAggregate>(key, token: Ct);
        aggregate!.Notes.ShouldBe(["by key", "by handle"]);
        (await query.Events.FetchStreamStateAsync(key, Ct))!.Version.ShouldBe(2);
    }

    [Fact]
    public async Task fetching_a_new_stream_twice_for_one_tenant_starts_it_once_and_leaves_other_tenants_alone()
    {
        await configureAsync(conjoined: true);
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession("root"))
        {
            await session.ForTenant("red").Events.FetchForWriting<SharedIdAggregate>(id, Ct);
            var red = await session.ForTenant("red").Events.FetchForWriting<SharedIdAggregate>(id, Ct);
            var blue = await session.ForTenant("blue").Events.FetchForWriting<SharedIdAggregate>(id, Ct);

            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red by id"));
            red.AppendOne(new SharedIdHappened("red by handle"));
            blue.AppendOne(new SharedIdHappened("blue by handle"));

            // One stream per tenant
            session.PendingChanges.Streams.Count(x => x.Id == id).ShouldBe(2);

            await session.SaveChangesAsync(Ct);
        }

        (await readAsync(id, "red")).Select(x => ((SharedIdHappened)x.Data).Note)
            .ShouldBe(["red by id", "red by handle"]);
        (await readAsync(id, "blue")).Select(x => ((SharedIdHappened)x.Data).Note)
            .ShouldBe(["blue by handle"]);
    }

    [Fact]
    public async Task fetching_a_stream_the_session_started_writes_to_that_start()
    {
        await configureAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new SharedIdHappened("started"));

            var stream = await session.Events.FetchForWriting<SharedIdAggregate>(id, Ct);
            stream.AppendOne(new SharedIdHappened("by handle"));

            session.PendingChanges.Streams.Count(x => x.Id == id).ShouldBe(1);

            await session.SaveChangesAsync(Ct);
        }

        (await readAsync(id)).Select(x => x.Version).ShouldBe([1L, 2L]);
    }

    [Fact]
    public async Task fetch_many_then_fetch_one_of_a_new_stream_starts_it_once()
    {
        await configureAsync();
        var existing = Guid.NewGuid();
        var newId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(existing, new SharedIdHappened("existing"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var many = await session.Events.FetchManyForWriting<SharedIdAggregate>([existing, newId], Ct);
            var again = await session.Events.FetchForWriting<SharedIdAggregate>(newId, Ct);

            many[1].AppendOne(new SharedIdHappened("by many"));
            again.AppendOne(new SharedIdHappened("by one"));
            session.Events.Append(newId, new SharedIdHappened("by id"));

            session.PendingChanges.Streams.Count(x => x.Id == newId).ShouldBe(1);

            await session.SaveChangesAsync(Ct);
        }

        (await readAsync(newId)).Select(x => ((SharedIdHappened)x.Data).Note)
            .ShouldBe(["by many", "by one", "by id"]);
    }
}
