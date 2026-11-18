using JasperFx.Events;
using JasperFx.Events.Tags;
using JasperFx.MultiTenancy;
using Polecat.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     The Polecat side of marten#6087. In Marten the stream and its events carry <c>*DEFAULT*</c> in a
///     database-per-tenant store while the <c>mt_dcb_tag_version</c> rows carry the tenant that appended, so
///     <c>DeleteSingleEventStreamAsync(id)</c>, which runs without a tenant, left those rows behind. Polecat writes
///     the session's tenant on the stream, the events and the <c>pc_dcb_tag_version</c> rows alike, its only way to
///     delete a stream is <c>TombstoneStream</c> through a session, and a session without a tenant is refused in
///     such a store. So the tombstone always matches the rows by the tenant they carry.
/// </summary>
public class tombstone_in_a_database_per_tenant_store : OneOffConfigurationsContext
{
    private const string Tenant = "per_tenant_tombstone";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private string Schema => theStore.Options.DatabaseSchemaName;

    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.MultiTenantedDatabases(x => x.AddTenant(Tenant, ConnectionSource.ConnectionString));
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("pt_course").ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<StudentId>("pt_student").ForAggregate<StudentCourseEnrollment>();
            opts.Events.DcbConflictScope = DcbConflictScope.TagValueAndEventType;
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private async Task<Guid> StartAsync(params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession(Tenant);
        var e = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        e.WithTag(tags);
        session.Events.StartStream(streamId, e);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    // Every row holding the value, the per-event-type versions included
    private Task<long> RowsAsync(Guid tagValue, string? tenant = null)
        => ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].pc_dcb_tag_version WHERE tag_value = '{tagValue}'"
                       + (tenant == null ? "" : $" AND tenant_id = '{tenant}'"));

    [Fact]
    public async Task a_tombstone_takes_the_rows_no_other_stream_carries()
    {
        await ConfigureAsync();
        var mine = new StudentId(Guid.NewGuid());
        var shared = new CourseId(Guid.NewGuid());
        var doomed = await StartAsync(mine, shared);
        var survivor = await StartAsync(shared);

        // The stream, its events and the rows all carry the tenant that appended
        (await ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].pc_streams WHERE id = '{doomed}' AND tenant_id = '{Tenant}'"))
            .ShouldBe(1);
        (await RowsAsync(mine.Value, Tenant)).ShouldBe(2);

        var sharedVersion = await ScalarAsync(
            $"SELECT MAX(version) FROM [{Schema}].pc_dcb_tag_version WHERE tag_value = '{shared.Value}'");

        await using (var session = theStore.LightweightSession(Tenant))
        {
            session.Events.TombstoneStream(doomed);
            await session.SaveChangesAsync(Ct);
        }

        (await RowsAsync(mine.Value)).ShouldBe(0);

        // Another stream still carries this one, so its rows stay, moved
        (await RowsAsync(shared.Value)).ShouldBe(2);
        (await ScalarAsync(
                $"SELECT MIN(version) FROM [{Schema}].pc_dcb_tag_version WHERE tag_value = '{shared.Value}'"))
            .ShouldBeGreaterThan(sharedVersion);

        await using var query = theStore.QuerySession(Tenant);
        (await query.Events.FetchStreamStateAsync(doomed, Ct)).ShouldBeNull();
        (await query.Events.FetchStreamAsync(survivor, token: Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task there_is_no_tombstone_without_a_tenant()
    {
        await ConfigureAsync();
        var mine = new StudentId(Guid.NewGuid());
        var streamId = await StartAsync(mine);

        await Should.ThrowAsync<DefaultTenantUsageDisabledException>(async () =>
        {
            await using var session = theStore.LightweightSession();
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        });

        (await RowsAsync(mine.Value, Tenant)).ShouldBe(2);
    }
}
