using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

#region unrelated_aggregates

public record ServiceRegistered(string Name, string Uri);

public record AlertRaised(string Reason);

public record AlertCleared(string Reason);

/// <summary>
///     Cares about service registration, and nothing else.
/// </summary>
public partial class ServiceSummary
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Uri { get; set; } = "";

    public static ServiceSummary Create(ServiceRegistered e) => new() { Name = e.Name, Uri = e.Uri };
}

/// <summary>
///     A self-aggregating type whose only handler is a catch-all <c>Evolve(IEvent)</c> — the shape
///     that surfaced #463. A catch-all accepts every event type at the method level, so nothing in
///     the aggregation path filtered by event applicability; the aggregator default-constructed an
///     instance and the switch simply matched nothing.
///     <para>
///     <see cref="IsActive" /> defaults to <c>true</c> on purpose, as in the reported aggregate: a
///     default-constructed phantom does not read as "empty", it reads as an <em>active alert</em>.
///     </para>
/// </summary>
public partial class AlertRecord
{
    public string Id { get; set; } = "";
    public string Reason { get; set; } = "";
    public bool IsActive { get; set; } = true;

    public void Evolve(IEvent e)
    {
        switch (e.Data)
        {
            case AlertRaised raised:
                Reason = raised.Reason;
                IsActive = true;
                break;

            case AlertCleared cleared:
                Reason = cleared.Reason;
                IsActive = false;
                break;
        }
    }
}

#endregion

/// <summary>
///     #463: <c>FetchLatest&lt;T&gt;(key)</c> on a stream that exists but contains no event
///     <c>T</c> handles used to return a non-null, default-constructed aggregate; Marten returns
///     null. <c>FetchLatest&lt;T&gt;(key) is null</c> is the idiomatic "does this aggregate exist?"
///     probe, so under the old behaviour it was satisfied by any stream key that had events at all
///     and the answer depended on whether some other aggregate happened to share the key space.
/// </summary>
/// <remarks>
///     The fix is Marten's mechanism, not a filter bolted onto aggregation: an Inline-projected
///     aggregate is read from its projected document (Marten's <c>FetchInlinedPlan</c>) rather than
///     re-aggregated off the stream. That makes the read agree with what the write side already
///     believed — the inline projection screens out streams it does not own, which is why no
///     document was ever written for them.
/// </remarks>
[Collection("integration")]
public class fetch_latest_for_unhandled_stream_tests : OneOffConfigurationsContext
{
    public fetch_latest_for_unhandled_stream_tests()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<ServiceSummary>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<AlertRecord>(SnapshotLifecycle.Inline);
        });
    }

    private async Task startServiceStream(string key)
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<ServiceSummary>(key,
            new ServiceRegistered(key, "rabbitmq://queue/test_service"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    // ===== The reported case =====

    [Fact]
    public async Task fetch_latest_is_null_for_a_stream_the_aggregate_does_not_handle()
    {
        await startServiceStream("TestService");

        await using var session = theStore.LightweightSession();
        var alert = await session.Events.FetchLatest<AlertRecord>("TestService",
            TestContext.Current.CancellationToken);

        // Before the fix this was a default-valued AlertRecord -- and since IsActive defaults to
        // true, the phantom read as an active alert rather than as absence.
        alert.ShouldBeNull();
    }

    // The issue verified this passed even before the fix -- the inline projection was already
    // screening the stream out. Keeping it pins the two halves together: the read path now agrees
    // with the persistence path.
    [Fact]
    public async Task no_document_is_written_for_a_stream_the_aggregate_does_not_handle()
    {
        await startServiceStream("NoDocService");

        await using var query = theStore.QuerySession();
        (await query.Query<AlertRecord>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);
    }

    // ===== The aggregate that DOES own the stream is unaffected =====

    [Fact]
    public async Task fetch_latest_still_returns_the_aggregate_that_handles_the_stream()
    {
        await startServiceStream("OwnedService");

        await using var session = theStore.LightweightSession();
        var summary = await session.Events.FetchLatest<ServiceSummary>("OwnedService",
            TestContext.Current.CancellationToken);

        summary.ShouldNotBeNull();
        summary!.Name.ShouldBe("OwnedService");
        summary.Uri.ShouldBe("rabbitmq://queue/test_service");
    }

    [Fact]
    public async Task fetch_latest_returns_the_alert_when_the_stream_does_hold_its_events()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<AlertRecord>("RealAlert", new AlertRaised("disk full"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        var alert = await query.Events.FetchLatest<AlertRecord>("RealAlert",
            TestContext.Current.CancellationToken);

        alert.ShouldNotBeNull();
        alert!.Reason.ShouldBe("disk full");
        alert.IsActive.ShouldBeTrue();
    }

    // A later append is reflected -- the projected document is what FetchLatest now reads, and the
    // inline projection has already written it by the time SaveChangesAsync returns.
    [Fact]
    public async Task fetch_latest_reflects_events_appended_after_the_stream_started()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<AlertRecord>("EvolvingAlert", new AlertRaised("cpu pegged"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append("EvolvingAlert", new AlertCleared("operator cleared"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        var alert = await query.Events.FetchLatest<AlertRecord>("EvolvingAlert",
            TestContext.Current.CancellationToken);

        alert.ShouldNotBeNull();
        alert!.IsActive.ShouldBeFalse();
        alert.Reason.ShouldBe("operator cleared");
    }

    // A stream that mixes a foreign event with the aggregate's own still resolves -- the inline
    // projection owns that stream, so the document exists.
    [Fact]
    public async Task a_stream_mixing_handled_and_unhandled_events_still_resolves()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<AlertRecord>("MixedStream",
                new ServiceRegistered("MixedStream", "rabbitmq://queue/mixed"),
                new AlertRaised("mixed"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        var alert = await query.Events.FetchLatest<AlertRecord>("MixedStream",
            TestContext.Current.CancellationToken);

        alert.ShouldNotBeNull();
        alert!.Reason.ShouldBe("mixed");
        alert.IsActive.ShouldBeTrue();
    }

    // ===== Pre-existing guarantees =====

    [Fact]
    public async Task fetch_latest_is_still_null_for_a_stream_that_does_not_exist()
    {
        await using var session = theStore.LightweightSession();
        var alert = await session.Events.FetchLatest<AlertRecord>("NeverExisted",
            TestContext.Current.CancellationToken);

        alert.ShouldBeNull();
    }
}
