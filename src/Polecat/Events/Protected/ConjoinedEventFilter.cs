using JasperFx.Events;
using Weasel.SqlServer;

namespace Polecat.Events.Protected;

/// <summary>
///     The tenant predicate every operation that keys a <c>pc_events</c> rewrite on
///     <c>seq_id</c> has to carry (marten#5234 has the identical defect and fix).
///
///     <para>
///         Under <see cref="EventGraph.UseTenantPartitionedEvents" /> each tenant draws from its
///         own <c>pc_events_sequence_{ordinal}</c>, so <c>seq_id</c> is NOT unique across tenants —
///         <c>seq_id = 1</c> exists in every tenant's partition. A <c>WHERE seq_id = @p</c> with no
///         tenant predicate therefore rewrites, or deletes, the same-numbered event in every other
///         tenant too. The read side of masking and compaction is correctly tenant-scoped, so the
///         sequences collected belong to the calling tenant; only the write escaped.
///     </para>
///
///     <para>
///         Gated on <see cref="TenancyStyle.Conjoined" /> rather than on
///         <c>UseTenantPartitionedEvents</c>, mirroring Marten's ConjoinedEventFilter.
///         Conjoined-without-partitioning keeps a single global IDENTITY, so there the predicate
///         filters nothing — but one shape across every call site covers any future mode that makes
///         <c>seq_id</c> ambiguous by construction.
///     </para>
/// </summary>
internal static class ConjoinedEventFilter
{
    /// <summary>
    ///     Appends <c> AND tenant_id = @p</c> when the event store is conjoined. Call immediately
    ///     after writing the <c>seq_id</c> predicate, before the statement terminator.
    /// </summary>
    public static void AppendConjoinedTenantFilter(this ICommandBuilder builder, EventGraph events, string tenantId)
    {
        if (events.TenancyStyle != TenancyStyle.Conjoined)
        {
            return;
        }

        // VarChar-typed like SetCompactedVersionOperation: tenant_id is varchar(250), and an
        // nvarchar-typed parameter would force an implicit conversion over the column.
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(tenantId, System.Data.SqlDbType.VarChar);
    }
}
