using System.Data;
using System.Data.Common;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     The fetch half of the DCB boundary check: read the current <c>pc_dcb_tag_version</c> row for
///     every tag a boundary query names, so the save can assert the rows have not moved since (gh-515).
/// </summary>
/// <remarks>
///     <para>
///         <b>The capture must be read BEFORE the events.</b> The two are separate statements, and
///         neither SQL Server's READ COMMITTED nor RCSI gives them a common point in time. Capturing
///         after the events pairs a FRESH version with a STALE aggregate, and the save's
///         <c>where version = @captured</c> then matches a check the caller never actually made — the
///         conflict is missed and both appends commit. Capturing first can only pair a STALE version
///         with a fresh aggregate, which fails the assertion: a spurious conflict the caller retries,
///         never a lost one. This is marten#5300, found in Marten and inherited by this design; do not
///         "tidy" the two reads back into the other order.
///     </para>
///     <para>
///         A missing row means no save has touched this boundary yet, and captures as version 0. Row
///         creation is deferred to the save, where the INSERT runs under the same lock as the assertion —
///         creating it here would either hold a lock for the caller's whole think-time or commit a write
///         on behalf of a read.
///     </para>
/// </remarks>
internal static class DcbTagVersionCapture
{
    /// <summary>
    ///     The distinct (tag table, stringified value) pairs a boundary query names. Distinct by the pair
    ///     alone: the side table is keyed by tag, not by the condition's optional event-type filter, so
    ///     two conditions over one tag are one row.
    /// </summary>
    public static List<(string TagTable, string TagValue)> TargetsFor(EventGraph events, EventTagQuery query)
    {
        var targets = new List<(string, string)>(query.Conditions.Count);
        var seen = new HashSet<(string, string)>();

        foreach (var condition in query.Conditions)
        {
            var registration = events.FindTagType(condition.TagType)
                               ?? throw new InvalidOperationException(
                                   $"Tag type '{condition.TagType.Name}' is not registered. " +
                                   $"Call RegisterTagType<{condition.TagType.Name}>() first.");

            var value = TagValueStringifier.Stringify(registration.ExtractValue(condition.TagValue));
            if (seen.Add((registration.TableSuffix, value)))
            {
                targets.Add((registration.TableSuffix, value));
            }
        }

        return targets;
    }

    public static SqlCommand BuildCommand(EventGraph events,
        IReadOnlyList<(string TagTable, string TagValue)> targets, string tenantId)
    {
        var cmd = new SqlCommand();
        var sql = new System.Text.StringBuilder();

        sql.Append("select tag_table, tag_value, version from ");
        sql.Append(events.DcbTagVersionTableName);
        sql.Append(" where tenant_id = @tenant and (");

        cmd.Parameters.AddVarChar("@tenant", tenantId);

        for (var i = 0; i < targets.Count; i++)
        {
            if (i > 0) sql.Append(" or ");
            sql.Append("(tag_table = @t").Append(i).Append(" and tag_value = @v").Append(i).Append(')');
            cmd.Parameters.AddVarChar($"@t{i}", targets[i].TagTable);
            cmd.Parameters.AddVarChar($"@v{i}", targets[i].TagValue);
        }

        sql.Append(')');
        cmd.CommandText = sql.ToString();

        return cmd;
    }

    /// <summary>The batched-query flavour of <see cref="BuildCommand" />, writing into a shared builder.</summary>
    public static void WriteSql(ICommandBuilder builder, EventGraph events,
        IReadOnlyList<(string TagTable, string TagValue)> targets, string tenantId)
    {
        builder.Append("select tag_table, tag_value, version from ");
        builder.Append(events.DcbTagVersionTableName);
        builder.Append(" where tenant_id = ");
        builder.AppendParameter(tenantId, SqlDbType.VarChar);
        builder.Append(" and (");

        for (var i = 0; i < targets.Count; i++)
        {
            if (i > 0) builder.Append(" or ");
            builder.Append("(tag_table = ");
            builder.AppendParameter(targets[i].TagTable, SqlDbType.VarChar);
            builder.Append(" and tag_value = ");
            builder.AppendParameter(targets[i].TagValue, SqlDbType.VarChar);
            builder.Append(")");
        }

        builder.Append(")");
    }

    public static async Task<Dictionary<(string, string), long>> ReadAsync(DbDataReader reader,
        CancellationToken token)
    {
        var byKey = new Dictionary<(string, string), long>();

        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            byKey[(reader.GetString(0), reader.GetString(1))] = reader.GetInt64(2);
        }

        return byKey;
    }

    public static IReadOnlyList<DcbTagVersionEntry> BuildEntries(
        IReadOnlyList<(string TagTable, string TagValue)> targets,
        Dictionary<(string, string), long> byKey,
        string tenantId,
        EventTagQuery query,
        long lastSeenSequence)
    {
        var entries = new DcbTagVersionEntry[targets.Count];

        for (var i = 0; i < targets.Count; i++)
        {
            // Absent from the SELECT means no row, which captures as 0 -- the save's INSERT branch then
            // creates it, with concurrent first-time creators serializing on the range lock.
            byKey.TryGetValue(targets[i], out var version);

            // Each row carries the query that captured it, so a save spanning several boundaries can
            // still name the one that actually lost.
            entries[i] = new DcbTagVersionEntry(targets[i].TagTable, targets[i].TagValue, tenantId,
                version, query, lastSeenSequence);
        }

        return entries;
    }
}
