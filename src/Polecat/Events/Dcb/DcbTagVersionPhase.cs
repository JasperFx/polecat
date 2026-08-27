using System.Data;
using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Schema;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     One <c>pc_dcb_tag_version</c> row this save has to touch.
/// </summary>
/// <param name="TagTable">
///     The tag type's <see cref="ITagTypeRegistration.TableSuffix" /> — the discriminator that lets one
///     side table serve every registered tag type.
/// </param>
/// <param name="TagValue">Canonical string form of the tag value, see <see cref="TagValueStringifier" />.</param>
/// <param name="TenantId">Owning tenant; the default tenant under single tenancy.</param>
/// <param name="CapturedVersion">
///     The version observed when a boundary read this row, or <c>null</c> when the save merely appends
///     under the tag and has no boundary to assert. A captured version turns the row into an optimistic
///     check; a null one is a bump only.
/// </param>
/// <param name="Query">The boundary query this row was captured for, so a failure can name it. Null for bumps.</param>
/// <param name="LastSeenSequence">How far <paramref name="Query" /> had read, for the exception message.</param>
internal readonly record struct DcbTagVersionEntry(
    string TagTable,
    string TagValue,
    string TenantId,
    long? CapturedVersion,
    EventTagQuery? Query,
    long LastSeenSequence);

/// <summary>
///     The DCB boundary check, run as one batch at the top of the save transaction: assert-and-bump the
///     rows this session's boundaries captured, and bump the rows it merely appends under.
/// </summary>
/// <remarks>
///     <para>
///         Assertions and bumps are deliberately merged into a single sorted phase rather than run
///         separately. Every session then takes its <c>pc_dcb_tag_version</c> row locks in the same
///         (tag_table, tag_value, tenant_id) order, so two sessions touching an overlapping set cannot
///         deadlock on the ORDER they take them in. Split into two phases — assert the boundary rows,
///         then bump the append rows — two sessions whose sets are mirror images of each other would take
///         the same two locks in opposite orders. Merging also stops a boundary save from bumping its own
///         row twice.
///     </para>
///     <para>
///         Ordering is only half of deadlock safety, and it is the half that only covers keys that already
///         exist. Creating a row cannot use a range lock to make check-then-insert atomic, because a range
///         lock on a missing key lands on a neighbouring key belonging to some other session's set — so
///         sorted acquisition does not order the locks actually taken. The create path therefore inserts
///         and catches the duplicate instead. See the comment on that branch.
///     </para>
///     <para>
///         Bumps are what make a plain <c>session.Events.Append(streamId, taggedEvent)</c> invalidate an
///         in-flight boundary. Without them the side table would only reflect boundary saves, and an
///         ordinary tagged append would commit straight through a boundary that had captured the prior
///         version.
///     </para>
///     <para>
///         Every command emits exactly one row, so the reader walks the batch one result set per entry
///         whether or not the entry is an assertion. See gh-515.
///     </para>
/// </remarks>
internal sealed class DcbTagVersionPhase
{
    private readonly EventGraph _events;
    private readonly DcbTagVersionEntry[] _ordered;

    private DcbTagVersionPhase(EventGraph events, DcbTagVersionEntry[] ordered)
    {
        _events = events;
        _ordered = ordered;
    }

    public int Count => _ordered.Length;

    /// <summary>
    ///     Merge the captured boundary rows and the appended-under rows into one ordered set. A row named
    ///     by both — the common case, a boundary that appends under its own tag — is asserted once.
    /// </summary>
    public static DcbTagVersionPhase? Build(EventGraph events,
        IReadOnlyList<DcbTagVersionEntry> captured,
        IReadOnlyList<DcbTagVersionEntry> appended)
    {
        if (captured.Count == 0 && appended.Count == 0) return null;

        var merged = new Dictionary<(string, string, string), DcbTagVersionEntry>();

        foreach (var entry in captured.Concat(appended))
        {
            var key = (entry.TagTable, entry.TagValue, entry.TenantId);
            if (!merged.TryGetValue(key, out var existing))
            {
                merged[key] = entry;
                continue;
            }

            // An assertion outranks a bump, and among assertions the OLDEST capture wins: every read the
            // session made has to still be valid, not just the most recent one.
            if (entry.CapturedVersion is null) continue;
            if (existing.CapturedVersion is null || entry.CapturedVersion < existing.CapturedVersion)
            {
                merged[key] = entry;
            }
        }

        var ordered = merged.Values.ToArray();
        Array.Sort(ordered, static (a, b) =>
        {
            var byTable = string.CompareOrdinal(a.TagTable, b.TagTable);
            if (byTable != 0) return byTable;

            var byValue = string.CompareOrdinal(a.TagValue, b.TagValue);
            if (byValue != 0) return byValue;

            return string.CompareOrdinal(a.TenantId, b.TenantId);
        });

        return new DcbTagVersionPhase(events, ordered);
    }

    public void WriteCommands(BatchBuilder builder)
    {
        var table = _events.DcbTagVersionTableName;

        for (var i = 0; i < _ordered.Length; i++)
        {
            if (i > 0) builder.StartNewCommand();

            var entry = _ordered[i];

            // The win/lose answer is a flag set where the decision is actually made, NOT a re-read of
            // the row afterwards. Re-reading cannot tell "I bumped it to captured + 1" apart from
            // "someone else bumped it to captured + 1 and I did nothing", which is precisely the race
            // this whole phase exists to catch. Each entry is its own SqlBatchCommand, hence its own
            // T-SQL batch, so the variable name cannot collide with a sibling entry's.
            builder.Append("declare @won int = 0; ");

            // Parameters are appended once and then referenced by name -- AppendParameter adds a NEW
            // parameter every call, and the key appears several times in each statement. Numbering
            // restarts per command, which is why the names are read back rather than assumed.
            // No table hint on the UPDATE, deliberately. Against an EXISTING row it takes the row's own
            // X lock and holds it to commit, which is all the serialization the steady state needs: a
            // concurrent saver blocks, and when it is let through SQL Server re-evaluates the predicate
            // against the now-committed version, so a stale `version = @captured` correctly matches
            // nothing. Adding SERIALIZABLE here was what deadlocked — see the INSERT branch below.
            builder.Append("update ");
            builder.Append(table);
            builder.Append(" set version = version + 1 where tag_table = ");
            var tagTableParam = builder.AppendParameter(entry.TagTable, SqlDbType.VarChar).ParameterName;
            builder.Append(" and tag_value = ");
            var tagValueParam = builder.AppendParameter(entry.TagValue, SqlDbType.VarChar).ParameterName;
            builder.Append(" and tenant_id = ");
            var tenantParam = builder.AppendParameter(entry.TenantId, SqlDbType.VarChar).ParameterName;

            if (entry.CapturedVersion is { } captured)
            {
                builder.Append(" and version = ");
                builder.AppendParameter(captured, SqlDbType.BigInt);
            }

            builder.Append("; if @@rowcount > 0 set @won = 1;");

            // Only a save that saw NO row may create one: either a boundary whose captured version was 0
            // or a plain append under a tag nothing has touched yet. A boundary that captured a version
            // and then failed to match it has lost outright -- the row moved under it.
            //
            // Insert-and-catch rather than probe-then-insert. Every read-then-write form of this needs a
            // range lock (HOLDLOCK/SERIALIZABLE) to be atomic, and a range lock on a key that does not
            // exist yet lands on the NEIGHBOURING key -- which belongs to some other session's set. That
            // is why sorting the keys is not enough on its own: concurrent savers inserting distinct new
            // rows into one index end up holding range locks over each other's keys and deadlock. Both
            // (UPDLOCK, SERIALIZABLE) and (XLOCK, HOLDLOCK) deadlocked here under the concurrent-append
            // tests, the second one on exactly this cross-key cycle.
            //
            // A bare INSERT takes an X lock on its OWN key and nothing else, so sessions creating
            // different rows never meet. Two sessions creating the SAME row do meet, on that one key: the
            // loser blocks, then gets a duplicate-key error once the winner commits, and re-runs the
            // versioned UPDATE -- which now finds the row at a version its captured value cannot match,
            // so it loses cleanly. A duplicate-key violation is a statement-level error, so catching it
            // leaves the transaction committable (Polecat never sets XACT_ABORT ON).
            if (entry.CapturedVersion is null or 0)
            {
                builder.Append(" if @won = 0 begin begin try insert into ");
                builder.Append(table);
                builder.Append(" (tag_table, tag_value, tenant_id, version) values (@");
                builder.Append(tagTableParam);
                builder.Append(", @");
                builder.Append(tagValueParam);
                builder.Append(", @");
                builder.Append(tenantParam);
                builder.Append(", 1); set @won = 1; end try begin catch");

                // 2627 = PK violation, 2601 = unique index violation. Anything else is not ours to
                // swallow.
                builder.Append(" if error_number() in (2627, 2601) begin update ");
                builder.Append(table);
                builder.Append(" set version = version + 1 where tag_table = @");
                builder.Append(tagTableParam);
                builder.Append(" and tag_value = @");
                builder.Append(tagValueParam);
                builder.Append(" and tenant_id = @");
                builder.Append(tenantParam);

                if (entry.CapturedVersion is { } capturedRetry)
                {
                    builder.Append(" and version = ");
                    builder.AppendParameter(capturedRetry, SqlDbType.BigInt);
                }

                builder.Append("; if @@rowcount > 0 set @won = 1; end else throw; end catch end");
            }

            // One row per command either way, so the caller's result-set walk does not have to know
            // which entries were assertions. The value only means anything for the assertions.
            builder.Append(" select @won;");
        }
    }

    /// <summary>
    ///     Walk the batch and collect the boundaries that lost. The reader is left on the last result set,
    ///     matching how the caller advances between phases.
    /// </summary>
    public async Task<IReadOnlyList<DcbTagVersionEntry>> ReadLosersAsync(DbDataReader reader,
        CancellationToken token)
    {
        var losers = new List<DcbTagVersionEntry>();

        for (var i = 0; i < _ordered.Length; i++)
        {
            if (i > 0) await reader.NextResultAsync(token).ConfigureAwait(false);

            var won = await reader.ReadAsync(token).ConfigureAwait(false) && reader.GetInt32(0) == 1;

            // Bumps cannot lose -- they carry no captured version to disagree with.
            if (!won && _ordered[i].CapturedVersion is not null)
            {
                losers.Add(_ordered[i]);
            }
        }

        return losers;
    }
}
