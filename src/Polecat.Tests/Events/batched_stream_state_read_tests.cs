using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Logging;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     gh-544: SaveChanges used to issue one UPDLOCK/HOLDLOCK stream-state round trip PER stream
///     being appended to. These tests pin the batched replacement — one locking read per save —
///     and its exact (currentVersion, exists, archived) per-stream semantics, for both Guid and
///     string stream identity.
/// </summary>
[Collection("integration")]
public class batched_stream_state_read_tests : IntegrationContext
{
    public batched_stream_state_read_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static bool IsStreamStateRead(string commandText)
        => commandText.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
           && commandText.Contains("pc_streams")
           && commandText.Contains("WITH (UPDLOCK, HOLDLOCK)");

    [Fact]
    public async Task multi_stream_append_issues_one_stream_state_query()
    {
        var streamIds = Enumerable.Range(0, 20).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var id in streamIds)
        {
            theSession.Events.StartStream(id, new QuestStarted($"Quest {id}"));
        }

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var logger = new CommandRecordingLogger();
        await using var session = theStore.LightweightSession();
        session.Logger = logger;

        foreach (var id in streamIds)
        {
            session.Events.Append(id, new MembersJoined(2, "Town", ["Hero"]));
        }

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var stateReads = logger.Commands.Where(IsStreamStateRead).ToArray();
        stateReads.Length.ShouldBe(1);
        stateReads[0].ShouldContain("OPENJSON(@ids)");

        // And the batched read fed the exact same version bookkeeping as the per-stream reads did
        await using var query = theStore.QuerySession();
        foreach (var id in streamIds)
        {
            var state = await query.Events.FetchStreamStateAsync(id, TestContext.Current.CancellationToken);
            state.ShouldNotBeNull();
            state!.Version.ShouldBe(2);
        }
    }

    [Fact]
    public async Task single_stream_append_keeps_the_point_lookup()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Solo"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var logger = new CommandRecordingLogger();
        await using var session = theStore.LightweightSession();
        session.Logger = logger;
        session.Events.Append(streamId, new MembersJoined(2, "Town", ["Hero"]));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var stateReads = logger.Commands.Where(IsStreamStateRead).ToArray();
        stateReads.Length.ShouldBe(1);
        stateReads[0].ShouldContain("WHERE id = @id");
        stateReads[0].ShouldNotContain("OPENJSON");
    }

    [Fact]
    public async Task batched_read_preserves_existing_and_missing_stream_semantics()
    {
        // One stream that exists with 2 events, one that exists with 1, and one the save creates
        var existingA = Guid.NewGuid();
        var existingB = Guid.NewGuid();
        var missing = Guid.NewGuid();

        theSession.Events.StartStream(existingA, new QuestStarted("A"), new MembersJoined(1, "Town", ["X"]));
        theSession.Events.StartStream(existingB, new QuestStarted("B"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session = theStore.LightweightSession();
        session.Events.Append(existingA, new MembersJoined(2, "Castle", ["Y"]));
        session.Events.Append(existingB, new MembersJoined(2, "Castle", ["Z"]));
        session.Events.Append(missing, new QuestStarted("Born by append"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(existingA, TestContext.Current.CancellationToken))!
            .Version.ShouldBe(3);
        (await query.Events.FetchStreamStateAsync(existingB, TestContext.Current.CancellationToken))!
            .Version.ShouldBe(2);
        (await query.Events.FetchStreamStateAsync(missing, TestContext.Current.CancellationToken))!
            .Version.ShouldBe(1);

        var born = await query.Events.FetchStreamAsync(missing, token: TestContext.Current.CancellationToken);
        born.Count.ShouldBe(1);
        born[0].Version.ShouldBe(1);
    }

    [Fact]
    public async Task archived_stream_in_a_multi_stream_save_still_throws()
    {
        var healthy = Guid.NewGuid();
        var archived = Guid.NewGuid();

        theSession.Events.StartStream(healthy, new QuestStarted("Healthy"));
        theSession.Events.StartStream(archived, new QuestStarted("Doomed"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var archiver = theStore.LightweightSession())
        {
            archiver.Events.ArchiveStream(archived);
            await archiver.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session = theStore.LightweightSession();
        session.Events.Append(healthy, new MembersJoined(2, "Town", ["A"]));
        session.Events.Append(archived, new MembersJoined(2, "Town", ["B"]));

        await Should.ThrowAsync<InvalidStreamException>(async () =>
        {
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task expected_version_mismatch_in_a_multi_stream_save_still_throws()
    {
        var fine = Guid.NewGuid();
        var contested = Guid.NewGuid();

        theSession.Events.StartStream(fine, new QuestStarted("Fine"));
        theSession.Events.StartStream(contested, new QuestStarted("Contested"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session = theStore.LightweightSession();
        session.Events.Append(fine, new MembersJoined(2, "Town", ["A"]));
        await session.Events.AppendOptimistic(contested, TestContext.Current.CancellationToken,
            new MembersJoined(2, "Town", ["B"]));

        // Bump the contested stream behind the session's back
        await using (var interloper = theStore.LightweightSession())
        {
            interloper.Events.Append(contested, new MembersJoined(2, "Keep", ["C"]));
            await interloper.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(async () =>
        {
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task string_identity_multi_stream_append_batches_and_preserves_state()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "batched_read_strings";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var keys = Enumerable.Range(0, 8).Select(i => $"stream-{Guid.NewGuid():N}-{i}").ToArray();

        await using (var seed = theStore.LightweightSession())
        {
            foreach (var key in keys)
            {
                seed.Events.StartStream(key, new QuestStarted(key));
            }

            await seed.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var logger = new CommandRecordingLogger();
        await using var session = theStore.LightweightSession();
        session.Logger = logger;

        foreach (var key in keys)
        {
            session.Events.Append(key, new MembersJoined(2, "Town", ["Hero"]));
        }

        var freshKey = $"fresh-{Guid.NewGuid():N}";
        session.Events.Append(freshKey, new QuestStarted("Fresh"));

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var stateReads = logger.Commands.Where(IsStreamStateRead).ToArray();
        stateReads.Length.ShouldBe(1);
        stateReads[0].ShouldContain("OPENJSON(@ids)");
        stateReads[0].ShouldContain("CAST(value AS varchar(250))");

        await using var query = theStore.QuerySession();
        foreach (var key in keys)
        {
            (await query.Events.FetchStreamStateAsync(key, TestContext.Current.CancellationToken))!
                .Version.ShouldBe(2);
        }

        (await query.Events.FetchStreamStateAsync(freshKey, TestContext.Current.CancellationToken))!
            .Version.ShouldBe(1);
    }

    [Fact]
    public async Task concurrent_saves_over_overlapping_stream_sets_do_not_deadlock()
    {
        // Two sessions appending to the same streams enqueued in OPPOSITE orders. The batched read
        // sorts the ids client-side into one locking statement, so the sessions serialize on the
        // pc_streams rows instead of interleaving N per-stream lock acquisitions in opposite order.
        var streamIds = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var id in streamIds)
        {
            theSession.Events.StartStream(id, new QuestStarted($"Quest {id}"));
        }

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var token = TestContext.Current.CancellationToken;

        async Task AppendAll(IEnumerable<Guid> ids)
        {
            await using var session = theStore.LightweightSession();
            foreach (var id in ids)
            {
                session.Events.Append(id, new MembersJoined(2, "Town", ["Racer"]));
            }

            await session.SaveChangesAsync(token);
        }

        await Task.WhenAll(
            Task.Run(() => AppendAll(streamIds), token),
            Task.Run(() => AppendAll(Enumerable.Reverse(streamIds)), token));

        await using var query = theStore.QuerySession();
        foreach (var id in streamIds)
        {
            (await query.Events.FetchStreamStateAsync(id, token))!.Version.ShouldBe(3);
        }
    }

    private class CommandRecordingLogger : IPolecatSessionLogger
    {
        public List<string> Commands { get; } = new();

        public void OnBeforeExecute(string commandText) => Commands.Add(commandText);

        public void LogSuccess(string commandText)
        {
        }

        public void LogFailure(string commandText, Exception ex)
        {
        }

        public void RecordSavedChanges(IDocumentSession session)
        {
        }
    }
}
