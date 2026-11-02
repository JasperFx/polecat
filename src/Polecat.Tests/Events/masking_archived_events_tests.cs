using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

// A mutable event, so one rule can mask it whichever store shape a test configures
public class ArchivedSubjectRecorded
{
    public string Email { get; set; } = string.Empty;
}

/// <summary>
///     Archiving a stream takes it out of the normal reads and leaves it on disk, so masking has to reach it.
///     It read through the same paths everything else does, which left archived rows out: protected data
///     stayed where it was and the pass reported success. Every assertion here reads the stored row with SQL,
///     because the read under test is the thing that could not see it.
/// </summary>
[Collection("integration")]
public class masking_archived_events_tests : IntegrationContext
{
    private const string Secret = "alice@example.com";
    private const string Masked = "*****";

    public masking_archived_events_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<string> maskingEnabledAsync(string schema, Action<StoreOptions>? configure = null)
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.Events.AddEventType(typeof(ArchivedSubjectRecorded));
            opts.Events.AddMaskingRuleForProtectedInformation<ArchivedSubjectRecorded>(x => x.Email = Masked);
            configure?.Invoke(opts);
        });

        return schema;
    }

    private async Task<Guid> anArchivedStreamAsync()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new ArchivedSubjectRecorded { Email = Secret });
        await session.SaveChangesAsync(Ct);

        session.Events.ArchiveStream(streamId);
        await session.SaveChangesAsync(Ct);

        return streamId;
    }

    private async Task<string> anArchivedKeyedStreamAsync()
    {
        var streamKey = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamKey, new ArchivedSubjectRecorded { Email = Secret });
        await session.SaveChangesAsync(Ct);

        session.Events.ArchiveStream(streamKey);
        await session.SaveChangesAsync(Ct);

        return streamKey;
    }

    private async Task<Guid> aLiveStreamAsync()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new ArchivedSubjectRecorded { Email = Secret });
        await session.SaveChangesAsync(Ct);

        return streamId;
    }

    private async Task<string> storedRowAsync(string schema, object streamIdentity, string? tenantId = null)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = tenantId is null
            ? $"SELECT CAST(data AS nvarchar(max)) FROM [{schema}].[pc_events] WHERE stream_id = @id"
            : $"SELECT CAST(data AS nvarchar(max)) FROM [{schema}].[pc_events] WHERE stream_id = @id AND tenant_id = @tenant";

        cmd.Parameters.AddWithValue("@id", streamIdentity);
        if (tenantId is not null) cmd.Parameters.AddWithValue("@tenant", tenantId);

        var data = await cmd.ExecuteScalarAsync(Ct);
        return data.ShouldBeOfType<string>();
    }

    private async Task theRowWasMaskedAsync(string schema, object streamIdentity)
    {
        var stored = await storedRowAsync(schema, streamIdentity);
        stored.ShouldNotContain(Secret);
        stored.ShouldContain(Masked);
    }

    [Fact]
    public async Task include_stream_by_guid_reaches_an_archived_stream()
    {
        var schema = await maskingEnabledAsync("mask_arch_guid");
        var streamId = await anArchivedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId), Ct);

        await theRowWasMaskedAsync(schema, streamId);
    }

    [Fact]
    public async Task include_stream_by_guid_with_a_filter_reaches_an_archived_stream()
    {
        var schema = await maskingEnabledAsync("mask_arch_guid_filter");
        var streamId = await anArchivedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId, e => e.Version == 1), Ct);

        await theRowWasMaskedAsync(schema, streamId);
    }

    [Fact]
    public async Task include_stream_by_key_reaches_an_archived_stream()
    {
        var schema = await maskingEnabledAsync("mask_arch_key",
            opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        var streamKey = await anArchivedKeyedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamKey), Ct);

        await theRowWasMaskedAsync(schema, streamKey);
    }

    [Fact]
    public async Task include_stream_by_key_with_a_filter_reaches_an_archived_stream()
    {
        var schema = await maskingEnabledAsync("mask_arch_key_filter",
            opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        var streamKey = await anArchivedKeyedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamKey, e => e.Version == 1), Ct);

        await theRowWasMaskedAsync(schema, streamKey);
    }

    [Fact]
    public async Task include_events_reaches_an_archived_stream()
    {
        var schema = await maskingEnabledAsync("mask_arch_events");
        var streamId = await anArchivedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeEvents(e => e.StreamId == streamId), Ct);

        await theRowWasMaskedAsync(schema, streamId);
    }

    [Fact]
    public async Task include_events_can_name_the_archived_events_on_their_own()
    {
        var schema = await maskingEnabledAsync("mask_arch_only");
        var archived = await anArchivedStreamAsync();
        var live = await aLiveStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeEvents(e => e.IsArchived), Ct);

        await theRowWasMaskedAsync(schema, archived);
        (await storedRowAsync(schema, live)).ShouldContain(Secret);
    }

    [Fact]
    public async Task include_events_can_name_one_subjects_archived_events()
    {
        var schema = await maskingEnabledAsync("mask_arch_subject");
        var subject = await anArchivedStreamAsync();
        var other = await anArchivedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(
            x => x.IncludeEvents(e => e.StreamId == subject && e.IsArchived), Ct);

        await theRowWasMaskedAsync(schema, subject);
        (await storedRowAsync(schema, other)).ShouldContain(Secret);
    }

    [Fact]
    public async Task include_events_can_still_rule_the_archived_events_out()
    {
        var schema = await maskingEnabledAsync("mask_arch_ruled_out");
        var archived = await anArchivedStreamAsync();
        var live = await aLiveStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeEvents(e => !e.IsArchived), Ct);

        await theRowWasMaskedAsync(schema, live);
        (await storedRowAsync(schema, archived)).ShouldContain(Secret);
    }

    [Fact]
    public async Task a_live_stream_is_still_masked()
    {
        var schema = await maskingEnabledAsync("mask_arch_live");
        var streamId = await aLiveStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId), Ct);

        await theRowWasMaskedAsync(schema, streamId);
    }

    [Fact]
    public async Task the_mask_survives_unarchiving_the_stream()
    {
        await maskingEnabledAsync("mask_arch_unarchive");
        var streamId = await anArchivedStreamAsync();

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId), Ct);

        await using var session = theStore.LightweightSession();
        session.Events.UnArchiveStream(streamId);
        await session.SaveChangesAsync(Ct);

        var events = await session.Events.FetchStreamAsync(streamId, token: Ct);
        events.Single().Data.ShouldBeOfType<ArchivedSubjectRecorded>().Email.ShouldBe(Masked);
    }

    [Fact]
    public async Task only_the_named_tenants_archived_stream_is_masked()
    {
        var schema = await maskingEnabledAsync("mask_arch_tenants",
            opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var streamId = Guid.NewGuid();

        foreach (var tenantId in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(new SessionOptions { TenantId = tenantId });
            session.Events.StartStream(streamId, new ArchivedSubjectRecorded { Email = Secret });
            await session.SaveChangesAsync(Ct);

            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await theStore.Advanced.ApplyEventDataMasking(x => x.ForTenant("red").IncludeStream(streamId), Ct);

        (await storedRowAsync(schema, streamId, "red")).ShouldContain(Masked);
        (await storedRowAsync(schema, streamId, "blue")).ShouldContain(Secret);
    }
}

/// <summary>
///     The same reach under <c>UseArchivedStreamPartitioning</c>, where archiving moves the row to another
///     partition. On its own class because that partition scheme is shared across the catalog.
/// </summary>
[Collection("integration")]
public class masking_archived_events_under_partitioning_tests : OneOffConfigurationsContext
{
    private const string Schema = "masking_archived_events_under_partitioning_tests";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        // See PartitionTestCleanup: the partition function and scheme carry no schema
        await PartitionTestCleanup.DropArchivedPartitionObjectsAsync();
        await base.InitializeAsync();
    }

    [Fact]
    public async Task an_archived_stream_is_masked()
    {
        ConfigureStore(opts =>
        {
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Events.AddEventType(typeof(ArchivedSubjectRecorded));
            opts.Events.AddMaskingRuleForProtectedInformation<ArchivedSubjectRecorded>(x => x.Email = "*****");
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new ArchivedSubjectRecorded { Email = "alice@example.com" });
            await session.SaveChangesAsync(Ct);

            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId), Ct);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT CAST(data AS nvarchar(max)) FROM [{Schema}].[pc_events] WHERE stream_id = @id";
        cmd.Parameters.AddWithValue("@id", streamId);

        var stored = (await cmd.ExecuteScalarAsync(Ct)).ShouldBeOfType<string>();
        stored.ShouldNotContain("alice@example.com");
        stored.ShouldContain("*****");
    }
}
