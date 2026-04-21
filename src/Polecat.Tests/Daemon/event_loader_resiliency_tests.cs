using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Tests for event loader resiliency: SkipUnknownEvents and SkipSerializationErrors.
///     These test the error handling paths in PolecatEventLoader by inserting
///     poison pill events directly via SQL.
/// </summary>
[Collection("integration")]
public class event_loader_resiliency_tests : IntegrationContext
{
    public event_loader_resiliency_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        // Clean slate for each test
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM [dbo].[pc_events];
            DELETE FROM [dbo].[pc_streams];
            DELETE FROM [dbo].[pc_event_progression];
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    // ===== SkipUnknownEvents =====

    [Fact]
    public async Task skip_unknown_events_skips_unresolvable_type()
    {
        // Insert a valid event
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid Quest"));
        await theSession.SaveChangesAsync();

        // Insert a poison pill with an unknown type directly via SQL
        await InsertPoisonPillEventAsync("unknown_event_type", "{\"data\": \"poison\"}",
            "Fake.Assembly.UnknownEvent, FakeAssembly");

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: true, skipSerialization: false);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // Only the valid event should be loaded — unknown event skipped
        page.Count.ShouldBe(1);
    }

    [Fact]
    public async Task unknown_events_throw_when_not_skipped()
    {
        // Insert a valid event
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid Quest"));
        await theSession.SaveChangesAsync();

        // Insert a poison pill
        await InsertPoisonPillEventAsync("unknown_event_type", "{\"data\": \"poison\"}",
            "Fake.Assembly.UnknownEvent, FakeAssembly");

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: false, skipSerialization: false);

        await Should.ThrowAsync<InvalidOperationException>(
            loader.LoadAsync(request, CancellationToken.None));
    }

    // ===== SkipSerializationErrors =====

    [RequiresNativeJsonFact(false)]
    public async Task skip_serialization_errors_skips_corrupted_json()
    {
        // Insert a valid event
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid Quest"));
        await theSession.SaveChangesAsync();

        // Insert an event with corrupted JSON but a valid, resolvable type name
        var questStartedTypeName = theStore.Database.Events.EventMappingFor(typeof(QuestStarted)).EventTypeName;
        var dotNetTypeName = typeof(QuestStarted).AssemblyQualifiedName!;
        await InsertPoisonPillEventAsync(questStartedTypeName, "NOT VALID JSON {{{",
            dotNetTypeName);

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: false, skipSerialization: true);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // Only the valid event should be loaded — corrupted event skipped
        page.Count.ShouldBe(1);
    }

    [RequiresNativeJsonFact(false)]
    public async Task serialization_errors_throw_when_not_skipped()
    {
        // Insert a valid event
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid Quest"));
        await theSession.SaveChangesAsync();

        // Insert corrupted JSON with valid type
        var questStartedTypeName = theStore.Database.Events.EventMappingFor(typeof(QuestStarted)).EventTypeName;
        var dotNetTypeName = typeof(QuestStarted).AssemblyQualifiedName!;
        await InsertPoisonPillEventAsync(questStartedTypeName, "CORRUPTED",
            dotNetTypeName);

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: false, skipSerialization: false);

        await Should.ThrowAsync<InvalidOperationException>(
            loader.LoadAsync(request, CancellationToken.None));
    }

    // ===== Both skip options enabled =====

    [RequiresNativeJsonFact(false)]
    public async Task skip_both_unknown_and_serialization_errors()
    {
        // Insert valid events
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid 1"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.StartStream(Guid.NewGuid(), new QuestStarted("Valid 2"));
        await session2.SaveChangesAsync();

        // Insert unknown type event
        await InsertPoisonPillEventAsync("totally_unknown", "{}", "Unknown.Type, UnknownAssembly");

        // Insert corrupted JSON event with valid type
        var questStartedTypeName = theStore.Database.Events.EventMappingFor(typeof(QuestStarted)).EventTypeName;
        var dotNetTypeName = typeof(QuestStarted).AssemblyQualifiedName!;
        await InsertPoisonPillEventAsync(questStartedTypeName, "{{BAD}}", dotNetTypeName);

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: true, skipSerialization: true);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // Only the 2 valid events should be loaded
        page.Count.ShouldBe(2);
    }

    [Fact]
    public async Task all_valid_events_loaded_when_no_errors()
    {
        // Insert only valid events
        for (var i = 0; i < 5; i++)
        {
            await using var session = theStore.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted($"Quest {i + 1}"));
            await session.SaveChangesAsync();
        }

        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100,
            skipUnknown: true, skipSerialization: true);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(5);
    }

    // ===== Helper methods =====

    private PolecatEventLoader CreateLoader()
    {
        return new PolecatEventLoader(theStore.Database.Events, theStore.Options, theStore.Options.ConnectionString);
    }

    private static EventRequest CreateRequest(long floor, long highWater, int batchSize,
        bool skipUnknown = false, bool skipSerialization = false)
    {
        return new EventRequest
        {
            Floor = floor,
            HighWater = highWater,
            BatchSize = batchSize,
            Name = new ShardName("TestLoader"),
            ErrorOptions = new ErrorHandlingOptions
            {
                SkipUnknownEvents = skipUnknown,
                SkipSerializationErrors = skipSerialization
            },
            Runtime = null!,
            Metrics = null!
        };
    }

    private async Task InsertPoisonPillEventAsync(string typeName, string json, string dotNetTypeName)
    {
        // Create a stream for the poison pill event
        var streamId = Guid.NewGuid();
        await using var conn = await OpenConnectionAsync();
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync();

        // Insert stream row
        await using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO [dbo].[pc_streams] (id, type, version, timestamp, created, tenant_id)
                VALUES (@id, NULL, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @tenant_id);
                """;
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@tenant_id", "*DEFAULT*");
            await cmd.ExecuteNonQueryAsync();
        }

        // Insert poison pill event
        await using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO [dbo].[pc_events]
                    (id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type)
                VALUES (@id, @stream_id, 1, @data, @type, SYSDATETIMEOFFSET(), @tenant_id, @dotnet_type);
                """;
            cmd.Parameters.AddWithValue("@id", Guid.NewGuid());
            cmd.Parameters.AddWithValue("@stream_id", streamId);
            cmd.Parameters.AddWithValue("@data", json);
            cmd.Parameters.AddWithValue("@type", typeName);
            cmd.Parameters.AddWithValue("@tenant_id", "*DEFAULT*");
            cmd.Parameters.AddWithValue("@dotnet_type", dotNetTypeName);
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }

    private async Task<long> GetHighestSeqIdAsync()
    {
        return await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
    }
}
