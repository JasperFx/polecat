using JasperFx.Events;
using JasperFx.Events.Daemon.HighWater;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     #786 / jasperfx#1031: the store-agnostic preview and advance of a high water mark, which is what lets
///     CritterWatch jump-start a daemon held behind a gap on a SQL Server store the same way it does on Marten
///     and Fisher.
/// </summary>
public partial class DocumentStore
{
    Task<IReadOnlyList<HighWaterMarkPreview>> IEventStore.PreviewHighWaterAdvanceAsync(IEventDatabase database,
        string? tenantId, CancellationToken token)
    {
        var db = RequirePolecatDatabase(database);
        var tenants = highWaterTenantsFor(db, tenantId);

        return previewAsync();

        async Task<IReadOnlyList<HighWaterMarkPreview>> previewAsync()
        {
            await db.EnsureStorageExistsAsync(typeof(IEvent), token).ConfigureAwait(false);
            return await highWaterDetectorFor(db).PreviewAdvanceAsync(db.Identifier, tenants, token)
                .ConfigureAwait(false);
        }
    }

    Task<IReadOnlyList<HighWaterMarkAdvance>> IEventStore.AdvanceHighWaterMarkToLatestAsync(IEventDatabase database,
        string? tenantId, long? expectedCurrentMark, CancellationToken token)
    {
        var db = RequirePolecatDatabase(database);
        var tenants = highWaterTenantsFor(db, tenantId);

        // A compare-and-set guards ONE mark. A null tenant on a per-tenant database selects every tenant's,
        // and one expected value cannot describe them all.
        if (expectedCurrentMark.HasValue && tenants is { Count: 0 })
        {
            throw new ArgumentException(
                "An expected current mark guards a single high water mark, but this database keeps one mark per " +
                "tenant (per-tenant partitioned events) and no tenant was named, which selects every tenant's. " +
                "Name the tenant.", nameof(expectedCurrentMark));
        }

        return AdvanceHighWaterMarksAsync(db, tenants, expectedCurrentMark, token);
    }

    /// <summary>
    ///     Shared by the <see cref="IEventStore" /> members and by <c>Advanced.AdvanceHighWaterMarkToLatestAsync</c>.
    ///     <paramref name="tenants" /> is null for the store-global mark, or the tenants whose own marks to advance
    ///     (empty: every tenant).
    /// </summary>
    internal async Task<IReadOnlyList<HighWaterMarkAdvance>> AdvanceHighWaterMarksAsync(PolecatDatabase database,
        IReadOnlyCollection<string>? tenants, long? expectedCurrentMark, CancellationToken token)
    {
        await database.EnsureStorageExistsAsync(typeof(IEvent), token).ConfigureAwait(false);
        return await highWaterDetectorFor(database)
            .AdvanceToCommittedCeilingAsync(database.Identifier, tenants, expectedCurrentMark, token)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     What a tenant id selects, per jasperfx#1031. Without per-tenant partitioned events the database keeps
    ///     ONE mark that is every tenant's, so a named tenant is refused rather than quietly advancing all of
    ///     them. With it, the store-global mark means nothing (tenants draw overlapping sequences), so a null
    ///     tenant selects every tenant's own mark.
    /// </summary>
    private static IReadOnlyCollection<string>? highWaterTenantsFor(PolecatDatabase database, string? tenantId)
    {
        if (database.Events.UseTenantPartitionedEvents)
        {
            return tenantId == null ? [] : [tenantId];
        }

        if (tenantId != null)
        {
            throw new ArgumentException(
                $"Database '{database.Identifier}' keeps one high water mark for every tenant, because this store " +
                "does not use per-tenant partitioned events, so tenant '" + tenantId + "' has no mark of its own. " +
                "Pass a null tenant to preview or advance the database's mark.", nameof(tenantId));
        }

        return null;
    }

    private PolecatHighWaterDetector highWaterDetectorFor(PolecatDatabase database)
        => new(database.Events, database.ConnectionString, Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance, Options.ResiliencePipeline);
}
