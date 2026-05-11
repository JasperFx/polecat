using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

#region snapshot_test_aggregates

public record SnapshotPartyStarted(string Name);
public record SnapshotMemberJoined(string Name);

public class SnapshotParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Members { get; set; } = new();

    public static SnapshotParty Create(SnapshotPartyStarted e) => new() { Name = e.Name };
    public void Apply(SnapshotMemberJoined e) => Members.Add(e.Name);
}

public class SnapshotPartyByString
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";

    public static SnapshotPartyByString Create(SnapshotPartyStarted e) => new() { Name = e.Name };
}

#endregion

[Collection("integration")]
public class snapshot_registration_tests : IntegrationContext
{
    public snapshot_registration_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void snapshot_registers_a_single_stream_projection_for_guid_id()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.Projections.Snapshot<SnapshotParty>(SnapshotLifecycle.Inline);

        // Verify a SingleStreamProjection<SnapshotParty, Guid> was registered
        var registered = options.Projections.All
            .Single(x => x.GetType().IsGenericType
                && x.GetType().GetGenericTypeDefinition() == typeof(SingleStreamProjection<,>));

        registered.GetType().GenericTypeArguments[0].ShouldBe(typeof(SnapshotParty));
        registered.GetType().GenericTypeArguments[1].ShouldBe(typeof(Guid));
        registered.Lifecycle.ShouldBe(ProjectionLifecycle.Inline);
    }

    [Fact]
    public void snapshot_resolves_string_identity_from_doc()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.Projections.Snapshot<SnapshotPartyByString>(SnapshotLifecycle.Async);

        var registered = options.Projections.All
            .Single(x => x.GetType().IsGenericType
                && x.GetType().GetGenericTypeDefinition() == typeof(SingleStreamProjection<,>));

        registered.GetType().GenericTypeArguments[0].ShouldBe(typeof(SnapshotPartyByString));
        registered.GetType().GenericTypeArguments[1].ShouldBe(typeof(string));
        registered.Lifecycle.ShouldBe(ProjectionLifecycle.Async);
    }

    [Fact]
    public void snapshot_rejects_projection_base_subclasses()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };

        Should.Throw<InvalidOperationException>(() =>
        {
            options.Projections.Snapshot<SingleStreamProjection<SnapshotParty, Guid>>(SnapshotLifecycle.Inline);
        });
    }

    [Fact]
    public async Task inline_snapshot_persists_aggregate_to_document_table()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "snapshot_inline";
            opts.Projections.Snapshot<SnapshotParty>(SnapshotLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new SnapshotPartyStarted("Fellowship"),
            new SnapshotMemberJoined("Frodo"),
            new SnapshotMemberJoined("Sam"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<SnapshotParty>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Fellowship");
        party.Members.ShouldBe(["Frodo", "Sam"]);
    }

    [Fact]
    public async Task async_snapshot_persists_aggregate_via_daemon()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "snapshot_async";
            opts.Projections.Snapshot<SnapshotParty>(SnapshotLifecycle.Async);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new SnapshotPartyStarted("Async Party"),
            new SnapshotMemberJoined("Aragorn"));
        await session.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<SnapshotParty>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Async Party");
        party.Members.ShouldBe(["Aragorn"]);
    }

    [Fact]
    public async Task composite_snapshot_registers_single_stream_projection()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "snapshot_composite";
            opts.Projections.CompositeProjectionFor("SnapshotComposite", composite =>
            {
                composite.Snapshot<SnapshotParty>();
            });
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new SnapshotPartyStarted("Composite Party"),
            new SnapshotMemberJoined("Gandalf"));
        await session.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<SnapshotParty>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Composite Party");
        party.Members.ShouldBe(["Gandalf"]);
    }
}
