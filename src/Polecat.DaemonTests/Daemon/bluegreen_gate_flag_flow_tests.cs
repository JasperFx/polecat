using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events.Aggregation;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #326 (jasperfx#480), work item 2: verify the blue/green side-effect gate
///     (<c>GateSideEffectsBehindPriorVersion</c>) flows through Polecat's projection registration
///     surface all the way to the async shard the daemon reads. The daemon skips the gate unless
///     <c>shard.Options.GateSideEffectsBehindPriorVersion</c> is set AND <c>shard.Name.Version &gt; 1</c>
///     (see JasperFxAsyncDaemon warm-up), so both must survive registration + shard construction. Each
///     async shard is built as <c>new AsyncShard(projection.Options, ..., ShardName.Compose(Name, "All",
///     null, projection.Version))</c>, so this asserts the flag + version reach the shard for each
///     directly-registered Polecat projection kind. Note aggregation projections name their shard after
///     the aggregate type, event projections after the projection type.
/// </summary>
public partial class bluegreen_gate_flag_flow_tests : OneOffConfigurationsContext
{
    private AsyncShard<IDocumentSession, IQuerySession> V2ShardFor(string identityContains)
    {
        var all = theStore.Options.Projections.AllShards();
        var match = all.SingleOrDefault(s => s.Name.Identity.Contains(identityContains) && s.Name.Version == 2);
        if (match == null)
        {
            throw new Exception($"No V2 shard matching '{identityContains}'. Available: " +
                                string.Join(" | ", all.Select(s => $"{s.Name.Identity}(v{s.Name.Version})")));
        }
        return match;
    }

    [Fact]
    public void gate_flows_through_single_stream_projection()
    {
        ConfigureStore(opts =>
        {
            var projection = new GatedSingleStream { Version = 2 };
            projection.Options.GateSideEffectsBehindPriorVersion = true;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        var shard = V2ShardFor("GatedAggregate");
        shard.Options.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        shard.Name.Version.ShouldBe(2u);
    }

    [Fact]
    public void gate_flows_through_multi_stream_projection()
    {
        ConfigureStore(opts =>
        {
            var projection = new GatedMultiStream { Version = 2 };
            projection.Options.GateSideEffectsBehindPriorVersion = true;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        var shard = V2ShardFor("GatedMultiAggregate");
        shard.Options.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        shard.Name.Version.ShouldBe(2u);
    }

    [Fact]
    public void gate_flows_through_event_projection()
    {
        ConfigureStore(opts =>
        {
            var projection = new GatedEventProjection { Version = 2 };
            projection.Options.GateSideEffectsBehindPriorVersion = true;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        var shard = V2ShardFor("GatedEventProjection");
        shard.Options.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        shard.Name.Version.ShouldBe(2u);
    }

    // The composite wrapper re-hosts child projections in ordered stages. A composite produces a single
    // v0-versioned parent shard (its children are internal stages, not separately-versioned shards), so
    // the per-child gate is not a top-level shard concern — but the wrapper must not strip the child's
    // AsyncOptions during assembly. Assert the child instance keeps its gate + version after composite
    // registration.
    [Fact]
    public void composite_assembly_preserves_child_gate()
    {
        var child = new GatedSingleStream { Version = 2 };
        child.Options.GateSideEffectsBehindPriorVersion = true;

        ConfigureStore(opts =>
        {
            opts.Projections.CompositeProjectionFor("GatedComposite", composite => composite.Add(child));
        });

        child.Options.GateSideEffectsBehindPriorVersion.ShouldBeTrue();
        child.Version.ShouldBe(2u);
    }

    // The daemon skips the gate for Version <= 1, so a v1 projection with the flag set must still
    // produce a v1 shard (the gate is inert, by design — this documents the skip condition).
    [Fact]
    public void version_one_projection_is_not_gated_even_with_flag_set()
    {
        ConfigureStore(opts =>
        {
            var projection = new GatedSingleStream(); // Version defaults to 1
            projection.Options.GateSideEffectsBehindPriorVersion = true;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });

        var shard = theStore.Options.Projections.AllShards()
            .Single(s => s.Name.Identity.Contains("GatedAggregate"));
        shard.Name.Version.ShouldBe(1u);
    }

    public record GateStarted(string Name);

    public partial class GatedAggregate
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";

        public void Apply(GateStarted e) => Name = e.Name;
    }

    public partial class GatedMultiAggregate
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public partial class GatedSingleStream : SingleStreamProjection<GatedAggregate, Guid>
    {
    }

    public partial class GatedMultiStream : MultiStreamProjection<GatedMultiAggregate, Guid>
    {
        public GatedMultiStream()
        {
            Identity<GateStarted>(_ => Guid.NewGuid());
        }

        public void Apply(GateStarted e, GatedMultiAggregate agg) => agg.Name = e.Name;
    }

    public partial class GatedEventProjection : EventProjection
    {
        public void Project(GateStarted e, IDocumentSession ops)
        {
            // No-op projection body; this test only cares about registration/shard flow.
        }
    }
}
