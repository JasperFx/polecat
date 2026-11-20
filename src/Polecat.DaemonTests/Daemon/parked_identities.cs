using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #1053 (marten#6003): what Polecat adds around the parked identity compliance suites. FetchForWriting, FetchLatest
///     and FetchManyForWriting over a parked identity with both stream identities, a multi-stream projection parking one
///     tenant's identity under conjoined tenancy, the cleanups that go with a rewind and an eject, and what happens with
///     the parked identities turned off.
/// </summary>
public class parked_identities : IAsyncLifetime
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private readonly List<IDisposable> _disposables = new();
    private readonly List<object> _poisoned = new();
    private readonly List<Guid> _poisonedTallies = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in _poisoned) ParkingPoison.Off(stream);
        foreach (var tally in _poisonedTallies) TallyPoison.Off(tally);

        foreach (var disposable in _disposables)
        {
            if (disposable is IAsyncDisposable async)
            {
                await async.DisposeAsync();
            }
            else
            {
                disposable.Dispose();
            }
        }
    }

    private async Task<DocumentStore> aStoreAsync(string schema, Action<StoreOptions> configure)
    {
        await TestSchema.DropSchemaTablesAsync(schema);

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            configure(opts);
        });

        _disposables.Add(store);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private async Task<IProjectionDaemon> startDaemonAsync(DocumentStore store)
    {
        var daemon = await store.BuildProjectionDaemonAsync();
        _disposables.Add(daemon);
        await daemon.StartAllAsync();
        return daemon;
    }

    private static async Task<IReadOnlyList<ParkedIdentity>> parkedAsync(DocumentStore store, string projection)
        => await ((IEventDatabase)store.Database).FetchParkedIdentitiesAsync(
            new ParkedIdentityQuery { ProjectionName = projection }, Ct);

    private static void addParkingEvents(StoreOptions opts)
    {
        opts.Events.AddEventType<ParkStarted>();
        opts.Events.AddEventType<ParkTicked>();
        opts.Events.AddEventType<ParkPoisoned>();
    }

    // ---- FetchForWriting, FetchLatest and FetchManyForWriting around a park ----

    public static TheoryData<string> StreamIdentities => new() { "guid", "string" };

    [Theory]
    [MemberData(nameof(StreamIdentities))]
    public async Task fetching_for_writing_sees_the_whole_stream_around_a_park(string kind)
    {
        var guids = kind == "guid";
        var store = await aStoreAsync($"parked_fetch_{kind}", opts =>
        {
            if (!guids) opts.Events.StreamIdentity = StreamIdentity.AsString;
            addParkingEvents(opts);

            if (guids)
            {
                opts.Projections.Snapshot<ParkedGuidCounter>(SnapshotLifecycle.Async);
            }
            else
            {
                opts.Projections.Snapshot<ParkedStringCounter>(SnapshotLifecycle.Async);
            }
        });

        object stream = guids ? Guid.NewGuid() : $"parked/{Guid.NewGuid():N}";
        var projection = guids ? nameof(ParkedGuidCounter) : nameof(ParkedStringCounter);

        async Task appendAsync(params object[] events)
        {
            await using var session = store.LightweightSession();
            if (stream is Guid id)
            {
                session.Events.Append(id, events);
            }
            else
            {
                session.Events.Append((string)stream, events);
            }

            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession())
        {
            if (stream is Guid id)
            {
                session.Events.StartStream<ParkedGuidCounter>(id, new ParkStarted(), new ParkTicked(), new ParkTicked());
            }
            else
            {
                session.Events.StartStream<ParkedStringCounter>((string)stream, new ParkStarted(), new ParkTicked(),
                    new ParkTicked());
            }

            await session.SaveChangesAsync(Ct);
        }

        var daemon = await startDaemonAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        async Task<(IParkedCounter? Writing, IParkedCounter? Latest, IParkedCounter? Many)> fetchAsync()
        {
            await using var session = store.LightweightSession();
            if (stream is Guid id)
            {
                var writing = await session.Events.FetchForWriting<ParkedGuidCounter>(id, Ct);
                var latest = await session.Events.FetchLatest<ParkedGuidCounter>(id, Ct);
                var many = await session.Events.FetchManyForWriting<ParkedGuidCounter>([id], Ct);
                return (writing.Aggregate, latest, many.Single().Aggregate);
            }
            else
            {
                var key = (string)stream;
                var writing = await session.Events.FetchForWriting<ParkedStringCounter>(key, Ct);
                var latest = await session.Events.FetchLatest<ParkedStringCounter>(key, Ct);
                var many = await session.Events.FetchManyForWriting<ParkedStringCounter>([key], Ct);
                return (writing.Aggregate, latest, many.Single().Aggregate);
            }
        }

        async Task<IParkedCounter?> storedAsync()
        {
            await using var query = store.QuerySession();
            return stream is Guid id
                ? await query.LoadAsync<ParkedGuidCounter>(id, Ct)
                : await query.LoadAsync<ParkedStringCounter>((string)stream, Ct);
        }

        var before = await fetchAsync();
        before.Writing.ShouldNotBeNull().Ticks.ShouldBe(2);
        before.Latest.ShouldNotBeNull().Ticks.ShouldBe(2);
        before.Many.ShouldNotBeNull().Ticks.ShouldBe(2);

        ParkingPoison.On(stream);
        _poisoned.Add(stream);
        await appendAsync(new ParkPoisoned(), new ParkTicked());
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var row = (await parkedAsync(store, projection)).ShouldHaveSingleItem();
        row.IdentityText.ShouldBe(ParkedIdentityText.Format(stream));
        row.IdentityType.ShouldBe(guids ? "guid" : "string");

        // While parked the stored document is frozen, and a fetch that folds the poisoned event fails the same way
        (await storedAsync())!.Ticks.ShouldBe(2);
        await Should.ThrowAsync<Exception>(fetchAsync);

        // Fixed: a fetch folds every event, though the stored document is still frozen
        ParkingPoison.Off(stream);
        var during = await fetchAsync();
        during.Writing.ShouldNotBeNull().Ticks.ShouldBe(4);
        during.Latest.ShouldNotBeNull().Ticks.ShouldBe(4);
        during.Many.ShouldNotBeNull().Ticks.ShouldBe(4);
        (await storedAsync())!.Ticks.ShouldBe(2);

        (await daemon.ReplayParkedIdentityAsync(projection, null, stream, Ct)).Outcome
            .ShouldBe(ParkedReplayOutcome.Replayed);

        (await parkedAsync(store, projection)).ShouldBeEmpty();
        var after = await fetchAsync();
        after.Writing.ShouldNotBeNull().Ticks.ShouldBe(4);
        after.Latest.ShouldNotBeNull().Ticks.ShouldBe(4);
        after.Many.ShouldNotBeNull().Ticks.ShouldBe(4);
        (await storedAsync())!.Sequences.ShouldBe(after.Writing!.Sequences);
    }

    // ---- a multi-stream projection under conjoined tenancy ----

    public static TheoryData<string> MultiStreamKinds => new() { "tag", "index" };

    [Theory]
    [MemberData(nameof(MultiStreamKinds))]
    public async Task a_multi_stream_projection_parks_one_tenants_identity_and_leaves_the_other_tenant_alone(string kind)
    {
        var tagged = kind == "tag";
        var projectionName = tagged ? ParkingTagTallyProjection.ProjectionName : ParkingIndexedTallyProjection.ProjectionName;

        var store = await aStoreAsync($"parked_conjoined_{kind}", opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.AddEventType<TallyTicked>();
            opts.Events.AddEventType<TallyPoisoned>();
            opts.Events.AddEventType<TallySideEffect>();

            if (tagged)
            {
                opts.Events.RegisterTagType<TallyTag>("tally");
                opts.Projections.Add(new ParkingTagTallyProjection(), ProjectionLifecycle.Async);
            }
            else
            {
                opts.Projections.Add(new ParkingIndexedTallyProjection(), ProjectionLifecycle.Async);
            }
        });

        var tally = Guid.NewGuid();

        async Task appendAsync(string tenant, object body)
        {
            await using var session = store.LightweightSession(tenant);
            if (tagged)
            {
                var @event = session.Events.BuildEvent(body);
                @event.WithTag(new TallyTag(tally));
                session.Events.Append(Guid.NewGuid(), @event);
            }
            else
            {
                session.Events.StartStream(Guid.NewGuid(), body);
            }

            await session.SaveChangesAsync(Ct);
        }

        async Task<int> ticksAsync(string tenant)
        {
            await using var query = store.QuerySession(tenant);
            return (await query.LoadAsync<ComplianceBalance>(tally, Ct))?.Amount ?? 0;
        }

        await appendAsync("red", new TallyTicked([tally]));
        await appendAsync("blue", new TallyTicked([tally]));

        var daemon = await startDaemonAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        TallyPoison.On(tally);
        _poisonedTallies.Add(tally);

        // Only red's poisoned event reaches the tally in red. Blue only ticks, which never throws.
        await appendAsync("red", new TallyPoisoned([tally]));
        await appendAsync("red", new TallyTicked([tally]));
        await appendAsync("blue", new TallyTicked([tally]));
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var row = (await parkedAsync(store, projectionName)).ShouldHaveSingleItem();
        row.TenantId.ShouldBe("red");
        row.IdentityText.ShouldBe(ParkedIdentityText.Format(tally));

        (await ticksAsync("red")).ShouldBe(1);
        (await ticksAsync("blue")).ShouldBe(2);

        TallyPoison.Off(tally);
        (await daemon.ReplayParkedIdentityAsync(projectionName, "red", tally, Ct)).Outcome
            .ShouldBe(ParkedReplayOutcome.Replayed);

        (await parkedAsync(store, projectionName)).ShouldBeEmpty();
        (await ticksAsync("red")).ShouldBe(3);
        (await ticksAsync("blue")).ShouldBe(2);

        // Red's identity reads back from its own source, and blue's events aren't in it
        var page = await ((IEventStore<IDocumentSession, IQuerySession>)store).LoadIdentityEventsAsync(store.Database,
            new IdentityEventsRequest { ProjectionName = projectionName, Identity = tally, TenantId = "red" }, Ct);
        page.Exact.ShouldBeTrue();
        page.Source.ShouldBe(tagged ? IdentityEventSource.Tag : IdentityEventSource.SliceIndex);
        page.Events.Count.ShouldBe(3);
        page.Events.ShouldAllBe(x => x.TenantId == "red");
    }

    // ---- the stepper ----

    [Fact]
    public async Task stepping_a_tag_grouped_projection_from_records_finds_its_identity_by_the_records_tags()
    {
        var store = await aStoreAsync("parked_step_tags", opts =>
        {
            opts.Events.AddEventType<TallyTicked>();
            opts.Events.AddEventType<TallyPoisoned>();
            opts.Events.AddEventType<TallySideEffect>();
            opts.Events.RegisterTagType<TallyTag>("tally");
            opts.Projections.Add(new ParkingTagTallyProjection(), ProjectionLifecycle.Async);
        });

        var tally = Guid.NewGuid();
        var other = Guid.NewGuid();
        var typeName = store.Events.EventMappingFor(typeof(TallyTicked)).EventTypeName;

        EventRecord record(long sequence, Guid tagged, IReadOnlyDictionary<string, string>? tags)
            => new(Guid.NewGuid(), sequence, 1, Guid.NewGuid().ToString(), typeName,
                System.Text.Json.JsonDocument.Parse(store.Options.Serializer.ToJson(new TallyTicked([tagged])))
                    .RootElement.Clone(),
                null, DateTimeOffset.UtcNow, StorageConstants.DefaultTenantId, tags);

        // Keyed by the tag type's name, as the explorer's tag queries are, or by its table suffix
        var records = new[]
        {
            record(1, tally, new Dictionary<string, string> { ["TallyTag"] = tally.ToString() }),
            record(2, other, new Dictionary<string, string> { ["TallyTag"] = other.ToString() }),
            record(3, tally, new Dictionary<string, string> { ["tally"] = tally.ToString() })
        };

        var timeline = await ((IEventStore)store).RunMultiStreamProjectionForIdentityAsync(
            ParkingTagTallyProjection.ProjectionName, tally, records, null, Ct);

        timeline.Steps.Count.ShouldBe(2);
        timeline.Steps.Select(x => x.Event.Sequence).ShouldBe([1L, 3L]);
        timeline.FinalState.ShouldNotBeNull().GetProperty("amount").GetInt32().ShouldBe(2);

        // The same records without their tags give the identity nothing
        var untagged = records.Select(x => x with { Tags = null }).ToArray();
        (await ((IEventStore)store).RunMultiStreamProjectionForIdentityAsync(
            ParkingTagTallyProjection.ProjectionName, tally, untagged, null, Ct)).Steps.ShouldBeEmpty();
    }

    // ---- rewinds and ejects ----

    [Fact]
    public async Task a_rewind_clears_the_parks_and_index_rows_above_its_floor_and_an_eject_clears_the_rest()
    {
        var store = await aStoreAsync("parked_rewind", opts =>
        {
            opts.Events.AddEventType<TallyTicked>();
            opts.Events.AddEventType<TallyPoisoned>();
            opts.Events.AddEventType<TallySideEffect>();
            opts.Projections.Add(new ParkingIndexedTallyProjection(), ProjectionLifecycle.Async);
        });

        var early = Guid.NewGuid();
        var late = Guid.NewGuid();

        async Task<long> appendAsync(object body)
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), body);
            await session.SaveChangesAsync(Ct);
            return await store.Database.FetchHighestEventSequenceNumber(Ct);
        }

        TallyPoison.On(early);
        TallyPoison.On(late);
        _poisonedTallies.Add(early);
        _poisonedTallies.Add(late);

        // Both tallies exist before anything fails, so the poison switch knows them by id
        await appendAsync(new TallyTicked([early]));
        await appendAsync(new TallyTicked([late]));

        var daemon = await startDaemonAsync(store);
        var started = DateTimeOffset.UtcNow.Add(Generous);
        while (daemon.CurrentAgents().Single().Position < 2)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(started, "the tallies were never projected");
            await Task.Delay(100, Ct);
        }

        await appendAsync(new TallyPoisoned([early]));
        var floor = await appendAsync(new TallyTicked([late]));
        await appendAsync(new TallyPoisoned([late]));

        // A fresh store has no progression row for the wait to read until the first batch commits
        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while ((await parkedAsync(store, ParkingIndexedTallyProjection.ProjectionName)).Count < 2)
        {
            var agent = daemon.CurrentAgents().Single();
            agent.Status.ShouldBe(AgentStatus.Running, agent.Failure?.ToString());
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline,
                $"the two tallies never parked: position {agent.Position}, high water {await store.Database.FetchHighestEventSequenceNumber(Ct)}");
            await Task.Delay(100, Ct);
        }

        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);
        await daemon.StopAllAsync();

        (await parkedAsync(store, ParkingIndexedTallyProjection.ProjectionName)).Count.ShouldBe(2);
        (await sliceIndexRowsAsync(store)).ShouldBe(5);

        var shard = ((IEventStore)store).RegisteredShardNames()
            .Single(x => x.Name == ParkingIndexedTallyProjection.ProjectionName);
        await ((IEventStore<IDocumentSession, IQuerySession>)store).RewindAgentProgressAsync(store.Database,
            shard.Identity, Ct, floor);

        // The park above the floor goes, with the index rows above it. The park below stays.
        var row = (await parkedAsync(store, ParkingIndexedTallyProjection.ProjectionName)).ShouldHaveSingleItem();
        row.IdentityText.ShouldBe(ParkedIdentityText.Format(early));
        (await sliceIndexRowsAsync(store)).ShouldBe(4);

        await ((IEventDatabase)store.Database).DeleteProjectionProgressByShardNameAsync(shard.Identity, Ct);

        (await parkedAsync(store, ParkingIndexedTallyProjection.ProjectionName)).ShouldBeEmpty();
        (await sliceIndexRowsAsync(store)).ShouldBe(0);
    }

    private static async Task<int> sliceIndexRowsAsync(DocumentStore store)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM [{store.Options.DatabaseSchemaName}].[{SliceIndexTable.TableNameFor(ParkingIndexedTallyProjection.ProjectionName, 1)}];";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    // ---- turned off ----

    [Fact]
    public async Task with_the_parked_identities_turned_off_a_failing_identity_pauses_the_shard()
    {
        var store = await aStoreAsync("parked_off", opts =>
        {
            addParkingEvents(opts);
            opts.Events.EnableParkedIdentities = false;
            opts.Projections.Snapshot<ParkedGuidCounter>(SnapshotLifecycle.Async);
        });

        (await tableExistsAsync(store, ParkedIdentitiesTable.TableName)).ShouldBeFalse();

        var stream = Guid.NewGuid();
        ParkingPoison.On(stream);
        _poisoned.Add(stream);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<ParkedGuidCounter>(stream, new ParkStarted(), new ParkPoisoned());
            await session.SaveChangesAsync(Ct);
        }

        var daemon = await startDaemonAsync(store);
        var shard = ((IEventStore)store).RegisteredShardNames().Single();

        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while (daemon.StatusFor(shard.Identity) != AgentStatus.Paused)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, "the shard never paused");
            await Task.Delay(100, Ct);
        }

        (await parkedAsync(store, nameof(ParkedGuidCounter))).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_parked_table_and_a_string_slice_index_take_unicode_keys()
    {
        var store = await aStoreAsync("parked_unicode", opts =>
        {
            opts.Advanced.UseUnicodeKeys = true;
            opts.Events.AddEventType<EmployeeHired>();
            opts.Events.AddEventType<DepartmentPoisoned>();
            opts.Projections.Add(new ParkingDepartmentProjection(), ProjectionLifecycle.Async,
                asyncConfiguration: o => o.UseSliceIndex());
        });

        (await tableExistsAsync(store, ParkedIdentitiesTable.TableName)).ShouldBeTrue();
        (await tableExistsAsync(store,
            SliceIndexTable.TableNameFor(ParkingDepartmentProjection.ProjectionName, 1))).ShouldBeTrue();

        // A department whose name is all of the key's 250 characters, outside ASCII, parks and is found again
        var department = new string('東', 250);
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new EmployeeHired(department, "Ann"));
            await session.SaveChangesAsync(Ct);
        }

        var stream = Guid.NewGuid();
        ParkingPoison.On(stream);
        _poisoned.Add(stream);
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(stream, new DepartmentPoisoned(department));
            await session.SaveChangesAsync(Ct);
        }

        await startDaemonAsync(store);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var row = (await parkedAsync(store, ParkingDepartmentProjection.ProjectionName)).ShouldHaveSingleItem();
        row.IdentityText.ShouldBe(department);

        var page = await ((IEventStore<IDocumentSession, IQuerySession>)store).LoadIdentityEventsAsync(store.Database,
            new IdentityEventsRequest { ProjectionName = ParkingDepartmentProjection.ProjectionName, Identity = department },
            Ct);
        page.Source.ShouldBe(IdentityEventSource.SliceIndex);
        page.Events.Count.ShouldBe(2);
    }

    private static async Task<bool> tableExistsAsync(DocumentStore store, string table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT OBJECT_ID(@name, 'U');";
        cmd.Parameters.AddWithValue("@name", $"[{store.Options.DatabaseSchemaName}].[{table}]");
        return await cmd.ExecuteScalarAsync(Ct) is not DBNull and not null;
    }
}
