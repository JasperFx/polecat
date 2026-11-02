using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record ForTenantStateHappened(int Number);

public class ForTenantStateAggregate
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public void Apply(ForTenantStateHappened _) => Count++;
}

/// <summary>
///     #892, "noticed by reading": the append planner's locking read of a stream's state was scoped to the
///     session's tenant, while <c>ForTenant</c> puts a stream of another tenant in the same save.
/// </summary>
public class for_tenant_stream_state_tests : IAsyncLifetime
{
    private const string Schema = "for_tenant_state";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);

        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.AddEventType<ForTenantStateHappened>();
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    private IDocumentSession SessionFor(string tenant)
        => _store.LightweightSession(new SessionOptions { TenantId = tenant });

    private async Task<Guid> StartStreamAsync(string tenant, int events = 1)
    {
        var streamId = Guid.NewGuid();
        await using var session = SessionFor(tenant);
        session.Events.StartStream(streamId,
            Enumerable.Range(1, events).Select(x => (object)new ForTenantStateHappened(x)).ToArray());
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task<IReadOnlyList<long>> VersionsOfAsync(string tenant, Guid streamId)
    {
        await using var query = _store.QuerySession(new SessionOptions { TenantId = tenant });
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        return events.Select(x => x.Version).ToList();
    }

    [Fact]
    public async Task append_through_for_tenant_to_a_stream_of_that_tenant()
    {
        var streamId = await StartStreamAsync("blue", 2);

        await using (var session = SessionFor("red"))
        {
            session.ForTenant("blue").Events.Append(streamId, new ForTenantStateHappened(3));
            await session.SaveChangesAsync(Ct);
        }

        (await VersionsOfAsync("blue", streamId)).ShouldBe([1L, 2L, 3L]);
        (await VersionsOfAsync("red", streamId)).ShouldBeEmpty();
    }

    /// <summary>Two streams go through the OPENJSON batch rather than the point lookup.</summary>
    [Fact]
    public async Task append_through_for_tenant_to_streams_of_two_tenants_in_one_save()
    {
        var red = await StartStreamAsync("red", 2);
        var blue = await StartStreamAsync("blue", 3);

        await using (var session = SessionFor("red"))
        {
            session.Events.Append(red, new ForTenantStateHappened(3));
            session.ForTenant("blue").Events.Append(blue, new ForTenantStateHappened(4));
            await session.SaveChangesAsync(Ct);
        }

        (await VersionsOfAsync("red", red)).ShouldBe([1L, 2L, 3L]);
        (await VersionsOfAsync("blue", blue)).ShouldBe([1L, 2L, 3L, 4L]);
    }

    [Fact]
    public async Task an_expected_version_is_checked_against_the_streams_tenant()
    {
        var streamId = await StartStreamAsync("blue", 2);

        await using var session = SessionFor("red");
        session.ForTenant("blue").Events.Append(streamId, 5L, new ForTenantStateHappened(3));

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task archival_is_checked_against_the_streams_tenant()
    {
        var streamId = await StartStreamAsync("blue");

        await using (var session = SessionFor("blue"))
        {
            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await using var writer = SessionFor("red");
        writer.ForTenant("blue").Events.Append(streamId, new ForTenantStateHappened(2));

        await Should.ThrowAsync<Polecat.Exceptions.ArchivedStreamException>(() => writer.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     A stream with nothing to append goes through a read of its own, with the same scope.
    /// </summary>
    [Fact]
    public async Task a_consistency_check_with_no_events_reads_the_streams_tenant()
    {
        var streamId = await StartStreamAsync("blue", 2);

        await using var session = SessionFor("red");
        var stream = await session.ForTenant("blue").Events.FetchForWriting<ForTenantStateAggregate>(streamId, Ct);
        stream.AlwaysEnforceConsistency = true;

        await session.SaveChangesAsync(Ct);
    }

    /// <summary>
    ///     A stream id is only unique within a tenant under conjoined tenancy, so the read has to be keyed
    ///     by both or one tenant's state answers for the other's.
    /// </summary>
    [Fact]
    public async Task one_stream_id_in_two_tenants()
    {
        var streamId = Guid.NewGuid();

        foreach (var (tenant, events) in new[] { ("red", 2), ("blue", 3) })
        {
            await using var seed = SessionFor(tenant);
            seed.Events.StartStream(streamId,
                Enumerable.Range(1, events).Select(x => (object)new ForTenantStateHappened(x)).ToArray());
            await seed.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor("red"))
        {
            session.ForTenant("blue").Events.Append(streamId, new ForTenantStateHappened(4));
            await session.SaveChangesAsync(Ct);
        }

        (await VersionsOfAsync("red", streamId)).ShouldBe([1L, 2L]);
        (await VersionsOfAsync("blue", streamId)).ShouldBe([1L, 2L, 3L, 4L]);
    }
}
