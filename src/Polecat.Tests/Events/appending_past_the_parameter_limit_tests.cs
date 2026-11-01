using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Linq.SqlGeneration;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #834. SQL Server refuses a command carrying more than 2098 parameters, and an append binds at
///     least eight per event, so one save of a stream failed at 262 events, and sooner with metadata
///     columns or tags on. The append now becomes as many commands as it needs, in the batch it was
///     already in.
/// </summary>
/// <remarks>
///     What the limit was, measured on 5.47.0 by binary search of a single save: 261 events by default,
///     for a Guid or a string stream and under conjoined tenancy; 232 per-tenant partitioned; 209 with
///     one tag; 174 with the four metadata columns on, with two tags, or with one tag under conjoined
///     tenancy. Several streams in one save were never the problem, because each already had its own
///     command: 6000 streams of one event passed, and 10 streams failed exactly where one did.
/// </remarks>
public partial class appending_past_the_parameter_limit_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public record Ping(int N);

    public record PingTag(Guid Value);

    public record PingCategory(Guid Value);

    public partial class PingTally
    {
        public Guid Id { get; set; }
        public int Count { get; set; }
        public int LastSeen { get; set; }

        public void Apply(Ping ping)
        {
            Count++;
            LastSeen = ping.N;
        }
    }

    public class PingDoc
    {
        public Guid Id { get; set; }
    }

    private static object[] Pings(int count) => Enumerable.Range(0, count).Select(i => (object)new Ping(i)).ToArray();

    private string Schema => theStore.Options.DatabaseSchemaName;

    private async Task UseStoreAsync(Action<StoreOptions>? configure = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<Ping>();
            configure?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<int> ScalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private Task<int> EventCountAsync() => ScalarAsync($"select count(*) from [{Schema}].[pc_events]");

    private Task<int> StreamCountAsync() => ScalarAsync($"select count(*) from [{Schema}].[pc_streams]");

    /// <summary>
    ///     The events a save handed back, the rows it wrote and the stream row all have to agree,
    ///     across every command the append became.
    /// </summary>
    private async Task AssertStreamHoldsAsync(object streamId, IReadOnlyList<IEvent> saved, long firstVersion)
    {
        for (var i = 0; i < saved.Count; i++)
        {
            saved[i].Version.ShouldBe(firstVersion + i);
            saved[i].Sequence.ShouldBeGreaterThan(0);
            if (i > 0) saved[i].Sequence.ShouldBeGreaterThan(saved[i - 1].Sequence);
        }

        await using var query = theStore.QuerySession();

        var stored = streamId is Guid guid
            ? await query.Events.FetchStreamAsync(guid, token: Ct)
            : await query.Events.FetchStreamAsync((string)streamId, token: Ct);

        stored.Where(x => x.Version >= firstVersion).Select(x => (x.Id, x.Sequence, x.Version))
            .ShouldBe(saved.Select(x => (x.Id, x.Sequence, x.Version)));

        var state = streamId is Guid stateGuid
            ? await query.Events.FetchStreamStateAsync(stateGuid, Ct)
            : await query.Events.FetchStreamStateAsync((string)streamId, Ct);

        state.ShouldNotBeNull();
        state!.Version.ShouldBe(firstVersion + saved.Count - 1);
    }

    // ---- the append itself ----------------------------------------------------------------------

    [Theory]
    [InlineData(200)]
    [InlineData(300)]
    [InlineData(1000)]
    [InlineData(5000)]
    public async Task one_save_starts_a_stream_of_any_size(int count)
    {
        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var stream = session.Events.StartStream(id, Pings(count));
        await session.SaveChangesAsync(Ct);

        await AssertStreamHoldsAsync(id, stream.Events, 1);
    }

    [Fact]
    public async Task one_save_appends_to_an_existing_stream_and_continues_its_versions()
    {
        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, Pings(7));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stream = session.Events.Append(id, Pings(5000));
            await session.SaveChangesAsync(Ct);

            await AssertStreamHoldsAsync(id, stream.Events, 8);
        }
    }

    [Fact]
    public async Task a_string_stream_key_takes_the_same_path()
    {
        await UseStoreAsync(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        var key = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        var stream = session.Events.StartStream(key, Pings(1000));
        await session.SaveChangesAsync(Ct);

        await AssertStreamHoldsAsync(key, stream.Events, 1);
    }

    [Fact]
    public async Task several_streams_in_one_save_each_keep_their_own_numbering()
    {
        await UseStoreAsync();

        await using var session = theStore.LightweightSession();
        var streams = Enumerable.Range(0, 3)
            .Select(_ => Guid.NewGuid())
            .Select(id => (Id: id, Action: session.Events.StartStream(id, Pings(600))))
            .ToArray();

        await session.SaveChangesAsync(Ct);

        foreach (var (id, action) in streams)
        {
            await AssertStreamHoldsAsync(id, action.Events, 1);
        }

        streams.SelectMany(x => x.Action.Events).Select(x => x.Sequence).Distinct().Count().ShouldBe(1800);
    }

    [Fact]
    public async Task every_metadata_column_reaches_the_events_of_a_later_command()
    {
        await UseStoreAsync(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableHeaders = true;
            opts.Events.EnableUserName = true;
        });

        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.CorrelationId = "correlation";
        session.CausationId = "causation";
        session.LastModifiedBy = "someone";
        session.SetHeader("source", "gh834");

        var stream = session.Events.StartStream(id, Pings(1000));
        await session.SaveChangesAsync(Ct);

        await AssertStreamHoldsAsync(id, stream.Events, 1);

        await using var query = theStore.QuerySession();
        var last = (await query.Events.FetchStreamAsync(id, token: Ct)).Last();
        last.CorrelationId.ShouldBe("correlation");
        last.CausationId.ShouldBe("causation");
        last.UserName.ShouldBe("someone");
        last.Headers!["source"].ToString().ShouldBe("gh834");
    }

    /// <summary>
    ///     A tag row finds its event's <c>seq_id</c> by the event's position in its own command, so a
    ///     tag written from the wrong offset would point at a different event rather than fail.
    /// </summary>
    [Theory]
    [InlineData(TenancyStyle.Single)]
    [InlineData(TenancyStyle.Conjoined)]
    public async Task a_tag_in_a_later_command_lands_on_its_own_event(TenancyStyle tenancy)
    {
        await UseStoreAsync(opts =>
        {
            opts.Events.TenancyStyle = tenancy;
            opts.Events.RegisterTagType<PingTag>("ping_tag");
            opts.Events.RegisterTagType<PingCategory>("ping_category");
        });

        const int count = 1000;
        var id = Guid.NewGuid();
        var category = new PingCategory(Guid.NewGuid());
        var tags = Enumerable.Range(0, count).Select(_ => new PingTag(Guid.NewGuid())).ToArray();

        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = "tagged" });

        var events = Enumerable.Range(0, count).Select(i =>
        {
            var built = session.Events.BuildEvent(new Ping(i));
            built.AddTag(tags[i]);
            built.AddTag(category);
            return (object)built;
        }).ToArray();

        var stream = session.Events.StartStream(id, events);
        await session.SaveChangesAsync(Ct);

        foreach (var position in new[] { 0, 200, 700, count - 1 })
        {
            var matched = await session.Events.QueryByTagsAsync(new EventTagQuery().Or(tags[position]), Ct);
            matched.ShouldHaveSingleItem().Sequence.ShouldBe(stream.Events[position].Sequence);
            ((Ping)matched[0].Data).N.ShouldBe(position);
        }

        (await ScalarAsync($"select count(*) from [{Schema}].[pc_event_tag_ping_tag]")).ShouldBe(count);
        (await ScalarAsync($"select count(*) from [{Schema}].[pc_event_tag_ping_category]")).ShouldBe(count);
    }

    [Fact]
    public async Task an_inline_projection_folds_the_whole_append_in_the_same_transaction()
    {
        await UseStoreAsync(opts =>
            opts.Projections.Add<SingleStreamProjection<PingTally, Guid>>(ProjectionLifecycle.Inline));

        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<PingTally>(id, Pings(1000));
        await session.SaveChangesAsync(Ct);

        var tally = await session.LoadAsync<PingTally>(id, Ct);
        tally.ShouldNotBeNull();
        tally!.Count.ShouldBe(1000);
        tally.LastSeen.ShouldBe(999);
    }

    [Fact]
    public async Task a_natural_key_is_written_for_a_stream_that_needed_several_commands()
    {
        await UseStoreAsync(opts =>
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline));

        var key = new OrderNumber("ORD-834");
        var events = new List<object> { new NkOrderCreated(key, "Hilda") };
        events.AddRange(Enumerable.Range(0, 999).Select(i => (object)new NkOrderItemAdded($"item-{i}", 1m)));

        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<OrderAggregate>(id, events.ToArray());
        await session.SaveChangesAsync(Ct);

        await using var writer = theStore.LightweightSession();
        var stream = await writer.Events.FetchForWriting<OrderAggregate, OrderNumber>(key, Ct);
        stream.Id.ShouldBe(id);
        stream.CurrentVersion.ShouldBe(1000);
    }

    [Fact]
    public async Task a_session_can_save_a_second_large_append_after_the_first()
    {
        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using var session = theStore.LightweightSession();

        session.Events.StartStream(id, Pings(600));
        await session.SaveChangesAsync(Ct);

        var second = session.Events.Append(id, Pings(600));
        await session.SaveChangesAsync(Ct);

        await AssertStreamHoldsAsync(id, second.Events, 601);
    }

    // ---- nothing commits when any of it fails ---------------------------------------------------

    /// <summary>
    ///     A THROW from a trigger aborts the batch and rolls the transaction back, the same way a
    ///     deadlock does. The commands behind it must not run outside the dead transaction.
    /// </summary>
    [Theory]
    [InlineData(1500)] // a command in the middle
    [InlineData(3000)] // the last event of the last command
    public async Task a_batch_abort_in_a_later_command_commits_nothing(int failingVersion)
    {
        await UseStoreAsync();

        await ExecuteAsync($"""
            CREATE TRIGGER [{Schema}].[tr_gh834_refuse] ON [{Schema}].[pc_events] AFTER INSERT AS
            BEGIN
                IF EXISTS (SELECT 1 FROM inserted WHERE version = {failingVersion})
                    THROW 51000, 'refused by the test', 1;
            END
            """);

        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, Pings(3000));

            var refused = await Should.ThrowAsync<SqlException>(session.SaveChangesAsync(Ct));
            refused.Number.ShouldBe(51000);
        }

        (await EventCountAsync()).ShouldBe(0);
        (await StreamCountAsync()).ShouldBe(0);
    }

    /// <summary>
    ///     The other kind of failure. A duplicate key only ends its statement, so the commands behind
    ///     it do run, and all that stops the save committing is the error surfacing as the reader
    ///     walks the batch.
    /// </summary>
    [Theory]
    [InlineData(1500)] // a command in the middle
    [InlineData(3000)] // the last statement before the final SELECT, which only the reader's drain reaches
    public async Task a_statement_failure_in_a_later_command_commits_nothing(int collidingVersion)
    {
        // Conjoined, because that is the mode with no foreign key to pc_streams, which is what lets a
        // stray row sit in the way of a stream whose own row has not been written yet.
        await UseStoreAsync(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var id = Guid.NewGuid();
        await ExecuteAsync($$"""
            INSERT INTO [{{Schema}}].[pc_events] (id, stream_id, version, data, type, tenant_id)
            VALUES (NEWID(), '{{id}}', {{collidingVersion}}, '{}', 'stray', 'colliding');
            """);

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "colliding" }))
        {
            session.Events.StartStream(id, Pings(3000));

            var collided = await Should.ThrowAsync<SqlException>(session.SaveChangesAsync(Ct));
            collided.Number.ShouldBe(2601); // ix_pc_events_stream_and_version
        }

        (await EventCountAsync()).ShouldBe(1);
        (await StreamCountAsync()).ShouldBe(0);
    }

    /// <summary>
    ///     A real 1205, with the victim in the third command of the save. The transaction is gone by
    ///     the time the client hears about it, so this is the case where a command that ran on
    ///     regardless would have committed on its own.
    /// </summary>
    [Fact]
    public async Task a_deadlock_in_a_later_command_surfaces_as_itself_and_commits_nothing()
    {
        const int waitingVersion = 1500;

        await UseStoreAsync();

        await ExecuteAsync($"""
            CREATE TABLE [{Schema}].[gh834_contended] (id int PRIMARY KEY, n int NOT NULL);
            INSERT INTO [{Schema}].[gh834_contended] (id, n) VALUES (1, 0);
            """);
        await ExecuteAsync($"""
            CREATE TRIGGER [{Schema}].[tr_gh834_contend] ON [{Schema}].[pc_events] AFTER INSERT AS
            BEGIN
                IF EXISTS (SELECT 1 FROM inserted WHERE version = {waitingVersion})
                    UPDATE [{Schema}].[gh834_contended] SET n = n + 1 WHERE id = 1;
            END
            """);

        var id = Guid.NewGuid();

        await using var blocker = new SqlConnection(ConnectionSource.ConnectionString);
        await blocker.OpenAsync(Ct);
        await using (var priority = blocker.CreateCommand())
        {
            // Who loses is the point of the test, so it cannot be left to the deadlock monitor.
            priority.CommandText = "SET DEADLOCK_PRIORITY HIGH";
            await priority.ExecuteNonQueryAsync(Ct);
        }

        await using var blockerTx = (SqlTransaction)await blocker.BeginTransactionAsync(Ct);
        await using (var hold = blocker.CreateCommand())
        {
            hold.Transaction = blockerTx;
            hold.CommandText = $"UPDATE [{Schema}].[gh834_contended] SET n = n + 1 WHERE id = 1";
            await hold.ExecuteNonQueryAsync(Ct);
        }

        var save = Task.Run(async () =>
        {
            await using var session = theStore.LightweightSession();
            session.Events.StartStream(id, Pings(3000));
            await session.SaveChangesAsync(Ct);
        }, Ct);

        // The save now owns the pc_streams row and is parked on the row this connection holds.
        var spid = blocker.ServerProcessId;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (await ScalarAsync($"select count(*) from sys.dm_exec_requests where blocking_session_id = {spid}") == 0)
        {
            DateTime.UtcNow.ShouldBeLessThan(deadline, "the save never reached the contended row");
            await Task.Delay(50, Ct);
        }

        // Asking for the row the save owns closes the cycle.
        await using (var close = blocker.CreateCommand())
        {
            close.Transaction = blockerTx;
            close.CommandText = $"SELECT count(*) FROM [{Schema}].[pc_streams] WHERE id = '{id}'";
            await close.ExecuteScalarAsync(Ct);
        }

        var deadlocked = await Should.ThrowAsync<SqlException>(save);
        deadlocked.Number.ShouldBe(1205);

        await blockerTx.CommitAsync(Ct);

        (await EventCountAsync()).ShouldBe(0);
        (await StreamCountAsync()).ShouldBe(0);
    }

    /// <summary>
    ///     #781 made a failing command name its own operation, and it depends on commands and operations
    ///     lining up one to one. If an append's extra commands did not each have an operation, this
    ///     would fall back to asking them all, and the first stream would be blamed for the collision.
    /// </summary>
    [Fact]
    public async Task a_collision_behind_a_split_append_names_the_stream_that_collided()
    {
        await UseStoreAsync();

        var taken = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(taken, new Ping(0));
            await session.SaveChangesAsync(Ct);
        }

        var innocent = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(innocent, Pings(600));
            session.Events.StartStream(taken, Pings(600));
            session.Events.StartStream(Guid.NewGuid(), Pings(10));

            var collision = await Should.ThrowAsync<Polecat.Exceptions.ExistingStreamIdCollisionException>(
                session.SaveChangesAsync(Ct));
            collision.Id.ShouldBe(taken);
        }

        (await EventCountAsync()).ShouldBe(1);
        (await StreamCountAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task a_document_failure_after_a_large_append_rolls_the_events_back_too()
    {
        await UseStoreAsync();

        var existing = new PingDoc { Id = Guid.NewGuid() };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(existing);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), Pings(1000));
            session.Insert(new PingDoc { Id = existing.Id });

            await Should.ThrowAsync<DocumentAlreadyExistsException>(session.SaveChangesAsync(Ct));
        }

        (await EventCountAsync()).ShouldBe(0);
        (await StreamCountAsync()).ShouldBe(0);
    }

    // ---- concurrency still means what it meant --------------------------------------------------

    [Fact]
    public async Task a_wrong_expected_version_is_refused_before_anything_is_written()
    {
        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new Ping(0));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(id, 9999L, Pings(1000));
            await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(session.SaveChangesAsync(Ct));
        }

        (await EventCountAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task a_stale_fetch_for_writing_loses_to_a_concurrent_append()
    {
        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new Ping(0));
            await session.SaveChangesAsync(Ct);
        }

        await using var stale = theStore.LightweightSession();
        var stream = await stale.Events.FetchForWriting<PingTally>(id, Ct);

        await using (var other = theStore.LightweightSession())
        {
            other.Events.Append(id, new Ping(1));
            await other.SaveChangesAsync(Ct);
        }

        stream.AppendMany(Pings(1000));
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(stale.SaveChangesAsync(Ct));

        (await EventCountAsync()).ShouldBe(2);
    }

    /// <summary>The race itself: both saves are past their own checks only if the loser's read blocks.</summary>
    [Fact]
    public async Task two_large_appends_at_one_expected_version_leave_exactly_one_of_them()
    {
        const int count = 1000;

        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new Ping(0));
            await session.SaveChangesAsync(Ct);
        }

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
        {
            await using var session = theStore.LightweightSession();
            session.Events.Append(id, 1L + count, Pings(count));

            try
            {
                await session.SaveChangesAsync(Ct);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }, Ct)));

        outcomes.Count(x => x is null).ShouldBe(1);
        outcomes.Single(x => x is not null).ShouldBeOfType<EventStreamUnexpectedMaxEventIdException>();

        await using var query = theStore.QuerySession();
        var stored = await query.Events.FetchStreamAsync(id, token: Ct);
        stored.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 1 + count).Select(x => (long)x));
    }

    [Fact]
    public async Task two_large_appends_with_no_expectation_serialize_and_both_land()
    {
        const int count = 1000;

        await UseStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new Ping(0));
            await session.SaveChangesAsync(Ct);
        }

        var saved = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
        {
            await using var session = theStore.LightweightSession();
            var action = session.Events.Append(id, Pings(count));
            await session.SaveChangesAsync(Ct);
            return action;
        }, Ct)));

        await using var query = theStore.QuerySession();
        var stored = await query.Events.FetchStreamAsync(id, token: Ct);

        stored.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 1 + 2 * count).Select(x => (long)x));

        // Neither append's events interleave with the other's, because the first holds the stream
        // row's lock from its first command until it commits.
        foreach (var action in saved)
        {
            var versions = action.Events.Select(x => x.Version).ToArray();
            versions.ShouldBe(Enumerable.Range(0, count).Select(i => versions[0] + i));
        }
    }

    [Fact]
    public async Task two_large_starts_of_one_stream_leave_exactly_one_of_them()
    {
        const int count = 1000;

        await UseStoreAsync();
        var id = Guid.NewGuid();

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
        {
            await using var session = theStore.LightweightSession();
            session.Events.StartStream(id, Pings(count));

            try
            {
                await session.SaveChangesAsync(Ct);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }, Ct)));

        outcomes.Count(x => x is null).ShouldBe(1);
        outcomes.Single(x => x is not null).ShouldBeOfType<Polecat.Exceptions.ExistingStreamIdCollisionException>();

        (await EventCountAsync()).ShouldBe(count);
    }

    // ---- what it costs ----------------------------------------------------------------------------

    /// <summary>
    ///     The extra commands travel in the batch the append was already in, so no save pays another
    ///     round trip for its size, and the ordinary save below cannot have.
    /// </summary>
    [Fact]
    public async Task a_save_takes_the_same_round_trips_whatever_its_size()
    {
        await UseStoreAsync();

        await using var small = theStore.LightweightSession();
        small.Events.StartStream(Guid.NewGuid(), Pings(1));
        await small.SaveChangesAsync(Ct);

        await using var large = theStore.LightweightSession();
        large.Events.StartStream(Guid.NewGuid(), Pings(1000));
        await large.SaveChangesAsync(Ct);

        large.RequestCount.ShouldBe(small.RequestCount);
    }

    /// <summary>
    ///     249 events of 8 parameters plus the stream row's 4 is 1996. The 250th makes 2004, which is
    ///     past the budget, so that is where the second command starts. Measuring it on the operation
    ///     list rather than on timing is the proof that an ordinary save is untouched: below this line
    ///     the operation is the very instance the planner built, so its command is the same text it
    ///     always was.
    /// </summary>
    [Theory]
    [InlineData(1, 1)]
    [InlineData(249, 1)]
    [InlineData(250, 2)]
    [InlineData(501, 3)]
    public async Task an_append_only_splits_when_it_has_to(int events, int commands)
    {
        await UseStoreAsync();

        await using var session = (DocumentSessionBase)theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), Pings(events));

        var operations = await session.BuildRaisedEventAppendsAsync(session.WorkTracker.Streams.ToList(), Ct);
        operations.Count.ShouldBe(commands);
    }

    public static TheoryData<string> Shapes => new()
    {
        "default", "string stream", "metadata", "conjoined", "tagged", "conjoined and tagged"
    };

    /// <summary>
    ///     <c>SplitToFit</c> restates what <c>ConfigureCommand</c> binds, so this holds the two together
    ///     in both directions: over the budget would mean it undercounts, and a command that stopped
    ///     well short would mean it overcounts. Run against the real builder rather than the real
    ///     server, because the server's own limit sits 98 above the budget and would hide a small drift.
    /// </summary>
    [Theory]
    [MemberData(nameof(Shapes))]
    public async Task every_command_fills_the_budget_and_none_passes_it(string shape)
    {
        await UseStoreAsync(opts =>
        {
            if (shape == "string stream") opts.Events.StreamIdentity = StreamIdentity.AsString;
            if (shape.Contains("conjoined")) opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            if (shape.Contains("tagged"))
            {
                opts.Events.RegisterTagType<PingTag>("ping_tag");
                opts.Events.RegisterTagType<PingCategory>("ping_category");
            }

            if (shape == "metadata")
            {
                opts.Events.EnableCorrelationId = true;
                opts.Events.EnableCausationId = true;
                opts.Events.EnableHeaders = true;
                opts.Events.EnableUserName = true;
            }
        });

        object[] Build(IDocumentSession session) => Enumerable.Range(0, 1200).Select(i =>
        {
            var built = session.Events.BuildEvent(new Ping(i));
            if (shape.Contains("tagged"))
            {
                built.AddTag(new PingTag(Guid.NewGuid()));
                if (i % 2 == 0) built.AddTag(new PingCategory(Guid.NewGuid()));
            }

            return (object)built;
        }).ToArray();

        var streamId = Guid.NewGuid();
        var streamKey = Guid.NewGuid().ToString();

        // Once for the new stream's INSERT and once for the existing stream's UPDATE, which bind
        // different things.
        for (var pass = 0; pass < 2; pass++)
        {
            await using var session = (DocumentSessionBase)theStore.LightweightSession(
                new SessionOptions { TenantId = "budgeted" });

            if (shape == "string stream")
            {
                session.Events.Append(streamKey, Build(session));
            }
            else
            {
                session.Events.Append(streamId, Build(session));
            }

            var operations = await session.BuildRaisedEventAppendsAsync(session.WorkTracker.Streams.ToList(), Ct);
            operations.Count.ShouldBeGreaterThan(1);

            await using var batch = new SqlBatch();
            var builder = new BatchBuilder(batch);
            for (var i = 0; i < operations.Count; i++)
            {
                if (i > 0) builder.StartNewCommand();
                StorageOperationExecution.Configure(operations[i], builder, session);
            }

            builder.Compile();

            var counts = batch.BatchCommands.Cast<SqlBatchCommand>().Select(x => x.Parameters.Count).ToArray();

            counts.Length.ShouldBe(operations.Count);
            counts.ShouldAllBe(x => x <= JsonValueList.Budget);

            // The heaviest event here is a conjoined one with two tags, at 24 parameters, so a command
            // that stopped further short of the budget than that had room for the next event.
            counts.SkipLast(1).ShouldAllBe(x => x > JsonValueList.Budget - 24);

            if (pass == 0)
            {
                await using var starter = theStore.LightweightSession(new SessionOptions { TenantId = "budgeted" });
                if (shape == "string stream")
                {
                    starter.Events.StartStream(streamKey, new Ping(0));
                }
                else
                {
                    starter.Events.StartStream(streamId, new Ping(0));
                }

                await starter.SaveChangesAsync(Ct);
            }
        }
    }
}
