using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_project_latest_for_commit_aggregate

public record CommitShipmentStarted(string Origin);

public record CommitShipmentScanned(string Location);

// Copies the metadata of the last event it applied
public partial record CommitShipment
{
    public Guid Id { get; set; }
    public int Scans { get; set; }
    public Guid LastEventId { get; set; }
    public long LastEventVersion { get; set; }
    public DateTimeOffset LastModified { get; set; }
    public string? LastModifiedBy { get; set; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? TenantId { get; set; }

    public static CommitShipment Create(IEvent<CommitShipmentStarted> e) => new CommitShipment().track(e);

    public void Apply(IEvent<CommitShipmentScanned> e)
    {
        Scans++;
        track(e);
    }

    private CommitShipment track(IEvent e)
    {
        LastEventId = e.Id;
        LastEventVersion = e.Version;
        LastModified = e.Timestamp;
        LastModifiedBy = e.UserName;
        CorrelationId = e.CorrelationId;
        CausationId = e.CausationId;
        TenantId = e.TenantId;
        return this;
    }
}

#endregion

public partial record KeyedCommitShipment
{
    public string Id { get; set; } = "";
    public int Scans { get; set; }
    public long LastEventVersion { get; set; }
    public DateTimeOffset LastModified { get; set; }

    public static KeyedCommitShipment Create(IEvent<CommitShipmentStarted> e) =>
        new() { LastEventVersion = e.Version, LastModified = e.Timestamp };

    public void Apply(IEvent<CommitShipmentScanned> e)
    {
        Scans++;
        LastEventVersion = e.Version;
        LastModified = e.Timestamp;
    }
}

public enum PendingCommitShape
{
    FetchForWriting,
    StartStream,
    AppendWithExpectedVersion
}

/// <summary>
///     #1145 (marten#6149): <c>ProjectLatestForCommit</c> returns the aggregate as <c>SaveChangesAsync()</c> will write
///     it, with the versions, ids, timestamps and metadata the commit gives the session's pending events, so it's
///     exactly what <c>FetchLatest</c> and live aggregation return once the session commits.
/// </summary>
public class project_latest_for_commit_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(ProjectionLifecycle lifecycle, Action<StoreOptions>? also = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableUserName = true;

            switch (lifecycle)
            {
                case ProjectionLifecycle.Inline:
                    opts.Projections.Snapshot<CommitShipment>(SnapshotLifecycle.Inline);
                    break;
                case ProjectionLifecycle.Async:
                    opts.Projections.Snapshot<CommitShipment>(SnapshotLifecycle.Async);
                    break;
            }

            also?.Invoke(opts);
        });
    }

    private static void stampMetadata(IDocumentSession session, string suffix)
    {
        session.LastModifiedBy = "user-" + suffix;
        session.CorrelationId = "correlation-" + suffix;
        session.CausationId = "causation-" + suffix;
    }

    private async Task<Guid> startShipmentAsync()
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        stampMetadata(session, "seed");
        session.Events.StartStream<CommitShipment>(id, new CommitShipmentStarted("York"),
            new CommitShipmentScanned("Selby"));
        await session.SaveChangesAsync(Ct);
        return id;
    }

    // Long enough for the clock to move on, so a timestamp the commit stamped again would differ
    private static Task theClockMovesOn() => Task.Delay(50, Ct);

    private async Task shouldMatchTheCommitAsync(CommitShipment? returned, Guid id)
    {
        await using var query = theStore.LightweightSession();
        var fromStoredEvents = await query.Events.AggregateStreamAsync<CommitShipment>(id, token: Ct);
        var latest = await query.Events.FetchLatest<CommitShipment>(id, Ct);

        returned.ShouldNotBeNull();
        fromStoredEvents.ShouldNotBeNull();
        returned.ShouldBe(fromStoredEvents! with { Id = returned.Id });
        latest.ShouldBe(fromStoredEvents with { Id = latest!.Id });
        returned.Id.ShouldBe(id);
    }

    public static TheoryData<ProjectionLifecycle, PendingCommitShape, bool> AgreementCases()
    {
        var cases = new TheoryData<ProjectionLifecycle, PendingCommitShape, bool>();
        foreach (var lifecycle in new[] { ProjectionLifecycle.Live, ProjectionLifecycle.Inline, ProjectionLifecycle.Async })
        {
            foreach (var shape in Enum.GetValues<PendingCommitShape>())
            {
                cases.Add(lifecycle, shape, false);
            }

            // The identity map changes how a stream fetched for writing is read
            cases.Add(lifecycle, PendingCommitShape.FetchForWriting, true);
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(AgreementCases))]
    public async Task returns_the_aggregate_the_commit_writes(ProjectionLifecycle lifecycle, PendingCommitShape shape,
        bool useIdentityMap)
    {
        configure(lifecycle, opts => opts.Projections.UseIdentityMapForAggregates = useIdentityMap);

        var id = shape == PendingCommitShape.StartStream ? Guid.NewGuid() : await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        stampMetadata(session, "call");

        switch (shape)
        {
            case PendingCommitShape.FetchForWriting:
                var stream = await session.Events.FetchForWriting<CommitShipment>(id, Ct);
                stream.AppendOne(new CommitShipmentScanned("Leeds"));
                break;
            case PendingCommitShape.StartStream:
                session.Events.StartStream<CommitShipment>(id, new CommitShipmentStarted("York"),
                    new CommitShipmentScanned("Leeds"));
                break;
            case PendingCommitShape.AppendWithExpectedVersion:
                session.Events.Append(id, 3, new CommitShipmentScanned("Leeds"));
                break;
        }

        var returned = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        await theClockMovesOn();
        await session.SaveChangesAsync(Ct);

        await shouldMatchTheCommitAsync(returned, id);
    }

    [Fact]
    public async Task the_commit_keeps_the_values_the_call_assigned()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        stampMetadata(session, "call");

        #region sample_polecat_project_latest_for_commit

        var stream = await session.Events.FetchForWriting<CommitShipment>(id);
        stream.AppendOne(new CommitShipmentScanned("Leeds"));

        // The aggregate as SaveChangesAsync() will write it, version, timestamps and metadata included
        var shipment = await session.Events.ProjectLatestForCommit<CommitShipment>(id);

        // ...store a response that carries it, in the same transaction

        await session.SaveChangesAsync();

        #endregion

        var pending = session.PendingChanges.Streams;
        pending.ShouldBeEmpty();

        await using var query = theStore.LightweightSession();
        var stored = (await query.Events.FetchStreamAsync(id, token: Ct)).Last();

        shipment!.LastEventId.ShouldBe(stored.Id);
        shipment.LastEventVersion.ShouldBe(3);
        stored.Version.ShouldBe(3);
        shipment.LastModified.ShouldBe(stored.Timestamp);
        shipment.LastModifiedBy.ShouldBe("user-call");
        shipment.CorrelationId.ShouldBe("correlation-call");
        shipment.CausationId.ShouldBe("causation-call");
    }

    [Fact]
    public async Task the_commit_keeps_the_timestamp_even_when_the_clock_moved_on()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CommitShipment>(id, Ct);
        stream.AppendOne(new CommitShipmentScanned("Leeds"));

        var shipment = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);
        var called = DateTimeOffset.UtcNow;

        await Task.Delay(250, Ct);
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        var stored = (await query.Events.FetchStreamAsync(id, token: Ct)).Last();
        stored.Timestamp.ShouldBe(shipment!.LastModified);
        stored.Timestamp.ShouldBeLessThanOrEqualTo(called);
    }

    [Fact]
    public async Task session_metadata_unset_at_the_call_is_filled_at_commit()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CommitShipment>(id, Ct);
        stream.AppendOne(new CommitShipmentScanned("Leeds"));

        var returned = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);
        returned!.CorrelationId.ShouldBeNull();

        // Set after the call, so the call couldn't have given it to the event
        session.CorrelationId = "correlation-late";
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchStreamAsync(id, token: Ct)).Last().CorrelationId.ShouldBe("correlation-late");
    }

    [Theory]
    [InlineData(ProjectionLifecycle.Live)]
    [InlineData(ProjectionLifecycle.Inline)]
    [InlineData(ProjectionLifecycle.Async)]
    public async Task calling_again_after_another_append_covers_the_new_event(ProjectionLifecycle lifecycle)
    {
        configure(lifecycle);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CommitShipment>(id, Ct);
        stream.AppendOne(new CommitShipmentScanned("Leeds"));

        var first = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);
        await theClockMovesOn();

        stream.AppendOne(new CommitShipmentScanned("Hull"));
        var second = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        // The first call's event kept what it was given
        second!.LastEventVersion.ShouldBe(4);
        second.Scans.ShouldBe(first!.Scans + 1);

        await theClockMovesOn();
        await session.SaveChangesAsync(Ct);

        await shouldMatchTheCommitAsync(second, id);

        await using var query = theStore.LightweightSession();
        var events = await query.Events.FetchStreamAsync(id, token: Ct);
        events[2].Timestamp.ShouldBe(first.LastModified);
    }

    [Fact]
    public async Task string_keyed_stream()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<KeyedCommitShipment>(SnapshotLifecycle.Inline);
        });

        var key = "shipment-" + Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<KeyedCommitShipment>(key, new CommitShipmentStarted("York"),
            new CommitShipmentScanned("Leeds"));

        var returned = await session.Events.ProjectLatestForCommit<KeyedCommitShipment>(key, Ct);

        await theClockMovesOn();
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        var latest = await query.Events.FetchLatest<KeyedCommitShipment>(key, Ct);
        returned.ShouldNotBeNull();
        returned.ShouldBe(latest);
        returned.LastEventVersion.ShouldBe(2);
    }

    [Fact]
    public async Task without_pending_events_it_is_fetch_latest()
    {
        configure(ProjectionLifecycle.Inline);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        var returned = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        returned.ShouldBe(await session.Events.FetchLatest<CommitShipment>(id, Ct));
        returned!.Scans.ShouldBe(1);
    }

    [Fact]
    public async Task refuses_a_natural_key_without_touching_the_pending_events()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CommitShipment>(id, Ct);
        stream.AppendOne(new CommitShipmentScanned("Leeds"));

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await session.Events.ProjectLatestForCommit<CommitShipment>("SHIP-1", Ct));
        ex.Message.ShouldContain("natural key");

        session.PendingChanges.Streams.Single().Events.Single().Version.ShouldBe(0);
    }

    [Fact]
    public async Task refuses_a_stream_whose_version_is_not_known_without_touching_the_pending_events()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();

        // No expected version, so the commit is what learns the stream's version
        session.Events.Append(id, new CommitShipmentScanned("Leeds"));

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct));
        ex.Message.ShouldContain("FetchForWriting()");

        var pending = session.PendingChanges.Streams.Single().Events.Single();
        pending.Version.ShouldBe(0);
        pending.Timestamp.ShouldBe(default);

        // And the commit is unaffected
        await session.SaveChangesAsync(Ct);
    }

    // Another writer commits an event, and the session then appends again with the expected version that accounts
    // for it. Without the pin the commit would succeed, writing the call's events one version later than the
    // aggregate it returned says.
    private async Task<IDocumentSession> appendAfterAnotherWriterAsync(Guid id, bool callAgain)
    {
        var session = theStore.LightweightSession();
        session.Events.Append(id, 3, new CommitShipmentScanned("Leeds"));
        await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        await using (var other = theStore.LightweightSession())
        {
            other.Events.Append(id, new CommitShipmentScanned("Goole"));
            await other.SaveChangesAsync(Ct);
        }

        // Expecting the stream to be at 3 now: 5, less the two events this session has pending
        session.Events.Append(id, 5, new CommitShipmentScanned("Hull"));

        if (callAgain) await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        return session;
    }

    [Fact]
    public async Task the_commit_refuses_a_stream_whose_expected_version_changed_after_the_call()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = await appendAfterAnotherWriterAsync(id, callAgain: false);

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));
        ex.Message.ShouldContain("after ProjectLatestForCommit()");

        // Nothing of this session's was written
        await using var query = theStore.LightweightSession();
        (await query.Events.FetchStreamAsync(id, token: Ct)).Count.ShouldBe(3);
    }

    [Fact]
    public async Task calling_again_after_the_expected_version_changed_lets_the_commit_through()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = await appendAfterAnotherWriterAsync(id, callAgain: true);
        var returned = await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);
        returned!.LastEventVersion.ShouldBe(5);

        await session.SaveChangesAsync(Ct);
        await shouldMatchTheCommitAsync(returned, id);
    }

    [Fact]
    public async Task ejecting_the_pending_changes_forgets_the_call()
    {
        configure(ProjectionLifecycle.Live);
        var id = await startShipmentAsync();

        await using var session = theStore.LightweightSession();
        session.Events.Append(id, 3, new CommitShipmentScanned("Leeds"));
        await session.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);

        session.EjectAllPendingChanges();

        // Freely appended again, with no call this time, so the commit stamps it as it always does
        session.Events.Append(id, 3, new CommitShipmentScanned("Hull"));
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchStreamAsync(id, token: Ct)).Count.ShouldBe(3);
    }

    [Fact]
    public async Task for_tenant_session_on_a_conjoined_store()
    {
        configure(ProjectionLifecycle.Inline, opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "blue" });
        stampMetadata(session, "call");

        var red = session.ForTenant("red");
        red.Events.StartStream<CommitShipment>(id, new CommitShipmentStarted("York"),
            new CommitShipmentScanned("Leeds"));

        var returned = await red.Events.ProjectLatestForCommit<CommitShipment>(id, Ct);
        returned!.TenantId.ShouldBe("red");

        await theClockMovesOn();
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession(new SessionOptions { TenantId = "red" });
        var latest = await query.Events.FetchLatest<CommitShipment>(id, Ct);
        returned.ShouldBe(latest);
    }
}
