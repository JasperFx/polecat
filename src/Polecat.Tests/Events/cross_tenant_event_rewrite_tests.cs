using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Protected;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

// A mutable probe event so an Action masking rule can rewrite it in place.
public class MaskProbeEvent
{
    public string Secret { get; set; } = string.Empty;
}

/// <summary>
///     marten#5234's Polecat twin (fixed there in commit 5a44c7452): under
///     <c>Events.UseTenantPartitionedEvents</c> every tenant draws from its own
///     <c>pc_events_sequence_{ordinal}</c>, so <c>seq_id = 1</c> exists in EVERY tenant's
///     partition. Three operations that rewrite <c>pc_events</c> keyed their WHERE on
///     <c>seq_id</c> alone, so the write escaped the tenant the read had correctly scoped to:
///
///     <list type="bullet">
///         <item>masking (<c>OverwriteEventOperation</c>) destroyed another tenant's payload and
///         replaced it with the calling tenant's masked JSON — a cross-tenant write AND
///         disclosure, in the feature that exists to prevent exactly that;</item>
///         <item>compaction's delete (<c>DeleteEventsOperation</c>) permanently removed another
///         tenant's events;</item>
///         <item>compaction's snapshot write (<c>ReplaceEventOperation</c>) left the calling
///         tenant's whole aggregate state sitting in the other tenant's stream.</item>
///     </list>
///
///     Nothing threw in any of the three.
/// </summary>
[Collection("tenant-partitioning")]
public class cross_tenant_event_rewrite_tests : IAsyncLifetime
{
    private const string Schema = "xtenant_rewrite";

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;

            opts.Events.AddEventType(typeof(MaskProbeEvent));
            opts.Events.AddMaskingRuleForProtectedInformation<MaskProbeEvent>(x => x.Secret = "***");
        });
    }

    [Fact]
    public async Task masking_one_tenant_must_not_rewrite_another_tenants_events()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var alphaStream = Guid.NewGuid();
        var betaStream = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(alphaStream, new MaskProbeEvent { Secret = "alpha-secret" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "beta" }))
        {
            session.Events.StartStream(betaStream, new MaskProbeEvent { Secret = "beta-secret" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The collision this bug depends on: each tenant has its own sequence, so both events are
        // seq_id 1. If this ever stops being true the assertion below stops proving anything.
        (await sequenceOf(store, alphaStream, "alpha")).ShouldBe(await sequenceOf(store, betaStream, "beta"));

        await store.Advanced.ApplyEventDataMasking(x =>
        {
            x.ForTenant("alpha");
            x.IncludeStream(alphaStream);
        }, TestContext.Current.CancellationToken);

        await using var query = store.QuerySession(new SessionOptions { TenantId = "beta" });
        var betaEvent = (await query.Events.FetchStreamAsync(betaStream, token: TestContext.Current.CancellationToken)).Single();

        betaEvent.Data.ShouldBeOfType<MaskProbeEvent>().Secret.ShouldBe("beta-secret",
            "masking tenant alpha must not touch tenant beta's same-numbered event");
    }

    [Fact]
    public async Task masking_still_masks_the_tenant_it_was_asked_to()
    {
        // The guard must not be so tight that the feature stops working.
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var alphaStream = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(alphaStream, new MaskProbeEvent { Secret = "alpha-secret" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.Advanced.ApplyEventDataMasking(x =>
        {
            x.ForTenant("alpha");
            x.IncludeStream(alphaStream);
        }, TestContext.Current.CancellationToken);

        await using var query = store.QuerySession(new SessionOptions { TenantId = "alpha" });
        var alphaEvent = (await query.Events.FetchStreamAsync(alphaStream, token: TestContext.Current.CancellationToken)).Single();

        alphaEvent.Data.ShouldBeOfType<MaskProbeEvent>().Secret.ShouldBe("***");
    }

    [Fact]
    public async Task compacting_one_tenants_stream_must_not_delete_another_tenants_events()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var alphaStream = Guid.NewGuid();
        var betaStream = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(alphaStream,
                new QuestStarted("Alpha Quest"),
                new MembersJoined(1, "Town", ["AlphaHero"]),
                new MonsterSlain("Goblin", 5));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "beta" }))
        {
            session.Events.StartStream(betaStream,
                new QuestStarted("Beta Quest"),
                new MembersJoined(1, "Forest", ["BetaHero"]),
                new MonsterSlain("Dragon", 50));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            await session.Events.CompactStreamAsync<QuestParty>(alphaStream);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "beta" });
        var betaEvents = await query.Events.FetchStreamAsync(betaStream, token: TestContext.Current.CancellationToken);

        // Pre-fix this was 1 — beta's first two events deleted, and the survivor at seq_id 3 was
        // alpha's Compacted<QuestParty> snapshot sitting in beta's stream.
        betaEvents.Count.ShouldBe(3, "compacting alpha's stream must leave beta's events alone");
        betaEvents.ShouldAllBe(x => !(x.Data is Compacted<QuestParty>));
    }

    [Fact]
    public async Task compaction_still_compacts_the_stream_it_was_asked_to()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var alphaStream = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(alphaStream,
                new QuestStarted("Alpha Quest"),
                new MembersJoined(1, "Town", ["AlphaHero"]),
                new MonsterSlain("Goblin", 5));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            await session.Events.CompactStreamAsync<QuestParty>(alphaStream);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "alpha" });
        var alphaEvents = await query.Events.FetchStreamAsync(alphaStream, token: TestContext.Current.CancellationToken);

        var compacted = alphaEvents.Single().Data.ShouldBeOfType<Compacted<QuestParty>>();
        compacted.Snapshot.Name.ShouldBe("Alpha Quest");
    }

    private static async Task<long> sequenceOf(DocumentStore store, Guid streamId, string tenantId)
    {
        await using var query = store.QuerySession(new SessionOptions { TenantId = tenantId });
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        return events[0].Sequence;
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task DropSequencesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq
            JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
