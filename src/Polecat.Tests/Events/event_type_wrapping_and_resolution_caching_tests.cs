using System.Collections.Concurrent;
using System.Reflection;
using JasperFx.Events;
using Polecat.Events;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     gh-537: two per-event reflection hotspots replaced with per-type caches.
///     <para>
///     1. <c>PolecatEventType.Wrap</c> used <c>MakeGenericType</c> + <c>Activator.CreateInstance</c>
///     per event; registration now closes <c>PolecatEventType&lt;T&gt;</c> once per event type and its
///     override is a plain <c>new Event&lt;T&gt;((T)data)</c>.
///     </para>
///     <para>
///     2. <c>EventGraph.ResolveEventType</c> called <c>Type.GetType(string)</c> per event row; it is
///     now memoized, including misses, so unknown/foreign event type names don't re-probe assemblies
///     on every row.
///     </para>
///     Pure unit tests — no database.
/// </summary>
public class event_type_wrapping_and_resolution_caching_tests
{
    public record WtcSomethingHappened(string Name);

    public record WtcOtherThingHappened(int Number);

    private static EventGraph CreateEventGraph() => new StoreOptions().EventGraph;

    [Fact]
    public void event_mapping_is_the_reflection_free_generic_subclass()
    {
        var graph = CreateEventGraph();

        var mapping = graph.EventMappingFor(typeof(WtcSomethingHappened));

        mapping.ShouldBeOfType<PolecatEventType<WtcSomethingHappened>>();
        mapping.EventType.ShouldBe(typeof(WtcSomethingHappened));

        // and the mapping itself is cached per type
        graph.EventMappingFor(typeof(WtcSomethingHappened)).ShouldBeSameAs(mapping);
    }

    [Fact]
    public void wrap_returns_typed_event_with_metadata_set()
    {
        var graph = CreateEventGraph();
        var mapping = graph.EventMappingFor(typeof(WtcSomethingHappened));

        var data = new WtcSomethingHappened("one");
        var wrapped = mapping.Wrap(data);

        var typed = wrapped.ShouldBeOfType<Event<WtcSomethingHappened>>();
        typed.Data.ShouldBeSameAs(data);
        wrapped.EventTypeName.ShouldBe(mapping.EventTypeName);
        wrapped.DotNetTypeName.ShouldBe(mapping.DotNetTypeName);
    }

    [Fact]
    public void wrap_builds_a_fresh_envelope_per_event()
    {
        var mapping = CreateEventGraph().EventMappingFor(typeof(WtcOtherThingHappened));

        var first = mapping.Wrap(new WtcOtherThingHappened(1));
        var second = mapping.Wrap(new WtcOtherThingHappened(2));

        first.ShouldNotBeSameAs(second);
        first.ShouldBeOfType<Event<WtcOtherThingHappened>>().Data.Number.ShouldBe(1);
        second.ShouldBeOfType<Event<WtcOtherThingHappened>>().Data.Number.ShouldBe(2);
    }

    [Fact]
    public void build_event_goes_through_the_typed_wrap()
    {
        var graph = CreateEventGraph();

        var @event = graph.BuildEvent(new WtcSomethingHappened("two"));

        @event.ShouldBeOfType<Event<WtcSomethingHappened>>();
        @event.EventTypeName.ShouldBe(graph.EventMappingFor(typeof(WtcSomethingHappened)).EventTypeName);
        @event.DotNetTypeName.ShouldBe(graph.EventMappingFor(typeof(WtcSomethingHappened)).DotNetTypeName);
    }

    /// <summary>
    ///     The base (non-generic) reflection path is kept as a fallback for directly constructed
    ///     instances — behavior parity with the pre-gh-537 implementation.
    /// </summary>
    [Fact]
    public void base_reflection_fallback_still_wraps_correctly()
    {
        var mapping = new PolecatEventType(typeof(WtcSomethingHappened));

        var data = new WtcSomethingHappened("three");
        var wrapped = mapping.Wrap(data);

        var typed = wrapped.ShouldBeOfType<Event<WtcSomethingHappened>>();
        typed.Data.ShouldBeSameAs(data);
        wrapped.EventTypeName.ShouldBe(mapping.EventTypeName);
        wrapped.DotNetTypeName.ShouldBe(mapping.DotNetTypeName);
    }

    [Fact]
    public void resolve_event_type_resolves_a_known_dotnet_type_name()
    {
        var graph = CreateEventGraph();
        var mapping = graph.EventMappingFor(typeof(WtcSomethingHappened));

        graph.ResolveEventType(mapping.DotNetTypeName).ShouldBe(typeof(WtcSomethingHappened));

        // second call is a cache hit and resolves identically
        graph.ResolveEventType(mapping.DotNetTypeName).ShouldBe(typeof(WtcSomethingHappened));
    }

    [Fact]
    public void resolve_event_type_tolerates_null_and_empty()
    {
        var graph = CreateEventGraph();

        graph.ResolveEventType(null).ShouldBeNull();
        graph.ResolveEventType(string.Empty).ShouldBeNull();
    }

    /// <summary>
    ///     Unknown/foreign event type names (written by some other application) resolve to null —
    ///     and the miss itself is cached so hydrating many rows of an unknown type doesn't call
    ///     <c>Type.GetType</c> (an assembly probe) once per row.
    /// </summary>
    [Fact]
    public void resolve_event_type_returns_null_for_unknown_types_and_caches_the_miss()
    {
        var graph = CreateEventGraph();
        const string unknown = "Some.Foreign.EventType, Some.Foreign.Assembly";

        graph.ResolveEventType(unknown).ShouldBeNull();
        graph.ResolveEventType(unknown).ShouldBeNull();

        var cache = typeof(EventGraph)
            .GetField("_eventTypeByDotNetName", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(graph)
            .ShouldBeOfType<ConcurrentDictionary<string, Type?>>();

        cache.TryGetValue(unknown, out var cachedMiss).ShouldBeTrue();
        cachedMiss.ShouldBeNull();
    }
}
