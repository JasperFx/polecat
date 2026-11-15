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
/// <param name="TagValue">Canonical string form of the tag value, see <see cref="JasperFx.Events.Tags.TagValueStringifier" />.</param>
/// <param name="TenantId">Owning tenant; the default tenant under single tenancy.</param>
/// <param name="CapturedVersion">
///     The version observed when a boundary read this row, or <c>null</c> when the save merely appends
///     under the tag and has no boundary to check. A captured version turns the row into an optimistic
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
///     What a boundary read when at least one of its tags had no <c>pc_dcb_tag_version</c> row, which the
///     save has to look at again. See <see cref="DcbTagVersionCapture.ReadFor" />.
/// </summary>
/// <param name="Query">The boundary's query.</param>
/// <param name="TenantId">The tenant the boundary read.</param>
/// <param name="LastSeenSequence">How far the query had read.</param>
/// <param name="EventCount">How many events it read.</param>
internal readonly record struct DcbBoundaryRead(
    EventTagQuery Query,
    string TenantId,
    long LastSeenSequence,
    int EventCount);

/// <summary>
///     The DCB boundary check, run as one batch at the top of the save transaction: lock and compare the
///     rows this session's boundaries captured, and bump the rows its appended events carry.
/// </summary>
/// <remarks>
///     <para>
///         <b>A check does not bump what it read (#972, marten#5887).</b> The DCB append condition fails only
///         when an event matching the boundary's query was appended after the read, so only a tag carried by
///         an appended event moves a version. A captured row the save does not also append under is read
///         under an update lock and compared, and left as it was. Bumping it made two boundaries over one tag
///         value conflict even when neither appended anything the other reads. A row the save both checked
///         and appends under — a boundary appending under its own tag, the common case — is still compared
///         and bumped by one <c>UPDATE</c>.
///     </para>
///     <para>
///         The check takes an update lock (<c>UPDLOCK</c>), not the exclusive lock an <c>UPDATE</c> would.
///         Two checks of one row still queue on it, and a bump queues behind a check, so a producer cannot
///         commit through a boundary that is mid-save. But a <c>FetchForWritingByTags</c> capture is a plain
///         read and is let through, which a no-op <c>UPDATE</c> would not allow. <c>UPDLOCK</c> also makes the
///         read a locking read, so under READ COMMITTED SNAPSHOT it sees the latest commit rather than the
///         row as of the statement's start.
///     </para>
///     <para>
///         Checks and bumps are deliberately one sorted phase rather than two. Every session then takes its
///         <c>pc_dcb_tag_version</c> row locks in the same (tag_table, tag_value, tenant_id) order, so two
///         sessions touching an overlapping set cannot deadlock on the ORDER they take them in. Split into
///         two phases — check the boundary rows, then bump the append rows — a save that checks row X and
///         bumps row Y deadlocks against one that checks Y and bumps X. With per-event-type versions (#973)
///         that crossing is routine on a single tag value: one decision selects returns and appends a
///         reminder, the other the reverse.
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
///         Every command emits exactly one row, so the reader walks the batch one result set per row
///         whether or not the row is checked. See gh-515.
///     </para>
///     <para>
///         Every value a BUMP gives a row comes from <c>pc_dcb_tag_version_sequence</c>, so a bumped row never
///         holds the same value twice, even a row deleted and created again. A tombstone relies on that (#890):
///         it deletes the rows of tags nothing carries any more, and with a counter that restarted, a stale
///         capture could match the row the next appender creates. The one other value a row can hold is 0,
///         written by a check that found no row (see <see cref="WriteCheck" />). 0 is what a missing row
///         captures as, and both mean the same thing: nothing carrying this key was appended since the row
///         last moved, so a capture of 0 matching it is a correct match.
///     </para>
/// </remarks>
internal sealed class DcbTagVersionPhase
{
    /// <summary>
    ///     Asserted when one session read a row twice and saw it move. A row's values only ever go up, so
    ///     no row can hold this, and the entry cannot create a row either: it loses outright.
    /// </summary>
    private const long NeverMatches = -1;

    /// <summary>One row the save locks: checked, bumped, or both.</summary>
    /// <param name="Captured">The boundary capture to compare against, or null for a bump only.</param>
    /// <param name="Bump">Whether an appended event carries this key, so its version moves.</param>
    private readonly record struct Row(
        string TagTable,
        string TagValue,
        string TenantId,
        DcbTagVersionEntry? Captured,
        bool Bump);

    private readonly EventGraph _events;
    private readonly Row[] _ordered;
    private readonly IReadOnlyList<DcbBoundaryRead> _reads;

    private DcbTagVersionPhase(EventGraph events, Row[] ordered, IReadOnlyList<DcbBoundaryRead> reads)
    {
        _events = events;
        _ordered = ordered;
        _reads = reads;
    }

    public int Count => _ordered.Length;

    /// <summary>
    ///     Merge the captured boundary rows and the appended-under rows into one ordered set. A row named
    ///     by both — the common case, a boundary that appends under its own tag — is checked and bumped by
    ///     one statement.
    /// </summary>
    public static DcbTagVersionPhase? Build(EventGraph events,
        IReadOnlyList<DcbTagVersionEntry> captured,
        IReadOnlyList<DcbTagVersionEntry> appended,
        IReadOnlyList<DcbBoundaryRead> reads)
    {
        if (captured.Count == 0 && appended.Count == 0) return null;

        var merged = new Dictionary<(string, string, string), Row>(captured.Count + appended.Count);

        foreach (var entry in captured)
        {
            var key = (entry.TagTable, entry.TagValue, entry.TenantId);
            if (!merged.TryGetValue(key, out var existing))
            {
                merged[key] = new Row(entry.TagTable, entry.TagValue, entry.TenantId, entry, false);
                continue;
            }

            // Two reads that saw different values mean the row moved between them, so the older read is
            // already stale, and every read the session made has to still be valid. Asserting the lower value
            // used to do that, until #890 let a row disappear: a read of 7 and a later read of "no row" would
            // have asserted the 0, created the row and won.
            if (existing.Captured!.Value.CapturedVersion != entry.CapturedVersion)
            {
                merged[key] = existing with
                {
                    Captured = existing.Captured.Value with { CapturedVersion = NeverMatches }
                };
            }
        }

        foreach (var entry in appended)
        {
            var key = (entry.TagTable, entry.TagValue, entry.TenantId);
            merged[key] = merged.TryGetValue(key, out var existing)
                ? existing with { Bump = true }
                : new Row(entry.TagTable, entry.TagValue, entry.TenantId, null, true);
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

        return new DcbTagVersionPhase(events, ordered, reads);
    }

    public void WriteCommands(BatchBuilder builder)
    {
        for (var i = 0; i < _ordered.Length; i++)
        {
            if (i > 0) builder.StartNewCommand();

            var row = _ordered[i];

            // The win/lose answer is a flag set where the decision is actually made, NOT a re-read of
            // the row afterwards. Re-reading cannot tell "I bumped it to the next value" apart from
            // "someone else bumped it and I did nothing", which is precisely the race this whole phase
            // exists to catch. Each row is its own SqlBatchCommand, hence its own T-SQL batch, so the
            // variable names cannot collide with a sibling row's.
            builder.Append("declare @won int = 0; ");

            // The key is bound once, into varchar locals, and referenced by name from then on: it appears
            // several times in each statement, and AppendParameter adds a NEW parameter every call. A fixed
            // type keeps one plan per statement shape rather than one per value length, and varchar compares
            // to the key columns without converting them. 8000 rather than the columns' 250 so a value too
            // long for the column fails the INSERT, as it always has, instead of being truncated into a match.
            DeclareKey(builder, "@k_table", row.TagTable);
            DeclareKey(builder, "@k_value", row.TagValue);
            DeclareKey(builder, "@k_tenant", row.TenantId);

            if (row.Bump)
            {
                WriteBump(builder, row.Captured?.CapturedVersion);
            }
            else
            {
                WriteCheck(builder, row.Captured!.Value.CapturedVersion ?? 0);
            }

            // One row per command either way, so the caller's result-set walk does not have to know which
            // rows were checked. The value only means anything for those.
            builder.Append(" select @won;");
        }

        foreach (var read in _reads)
        {
            builder.StartNewCommand();
            EventOperations.WriteTagQueryCountSql(builder, _events, read.Query, read.TenantId, read.LastSeenSequence);
        }
    }

    private const string KeyPredicate = "tag_table = @k_table and tag_value = @k_value and tenant_id = @k_tenant";

    private const string KeyValues = "@k_table, @k_value, @k_tenant";

    private static void DeclareKey(BatchBuilder builder, string name, string value)
    {
        builder.Append($"declare {name} varchar(8000) = ");
        builder.AppendParameter(value, SqlDbType.VarChar).Size = 8000;
        builder.Append("; ");
    }

    /// <summary>
    ///     A row an appended event carries: move it to the next sequence value, and when the session's
    ///     boundary also read it, only if it is still at the captured version.
    /// </summary>
    private void WriteBump(BatchBuilder builder, long? captured)
    {
        var table = _events.DcbTagVersionTableName;
        var sequence = _events.DcbTagVersionSequenceName;

        // No table hint on the UPDATE, deliberately. Against an EXISTING row it takes the row's own X lock and
        // holds it to commit, which is all the serialization the steady state needs: a concurrent saver
        // blocks, and when it is let through SQL Server re-evaluates the predicate against the now-committed
        // version, so a stale `version = @captured` correctly matches nothing. Adding SERIALIZABLE here was
        // what deadlocked -- see the INSERT branch below.
        builder.Append($"update {table} set version = next value for {sequence} where {KeyPredicate}");
        if (captured is { } version)
        {
            builder.Append(" and version = ");
            builder.AppendParameter(version, SqlDbType.BigInt);
        }

        builder.Append("; if @@rowcount > 0 set @won = 1;");

        // Only a save that saw NO row may create one: either a boundary whose captured version was 0 or a
        // plain append under a key nothing has touched yet. A boundary that captured a version and then failed
        // to match it has lost outright -- the row moved under it.
        if (captured is not (null or 0)) return;

        // Insert-and-catch rather than probe-then-insert. Every read-then-write form of this needs a range
        // lock (HOLDLOCK/SERIALIZABLE) to be atomic, and a range lock on a key that does not exist yet lands
        // on the NEIGHBOURING key -- which belongs to some other session's set. That is why sorting the keys
        // is not enough on its own: concurrent savers inserting distinct new rows into one index end up
        // holding range locks over each other's keys and deadlock. Both (UPDLOCK, SERIALIZABLE) and (XLOCK,
        // HOLDLOCK) deadlocked here under the concurrent-append tests, the second one on exactly this
        // cross-key cycle.
        //
        // A bare INSERT takes an X lock on its OWN key and nothing else, so sessions creating different rows
        // never meet. Two sessions creating the SAME row do meet, on that one key: the loser blocks, then gets
        // a duplicate-key error once the winner commits, and re-runs the UPDATE -- which, for a boundary, now
        // finds the row at a version its captured 0 cannot match, so it loses cleanly. A duplicate-key
        // violation is a statement-level error, so catching it leaves the transaction committable (Polecat
        // never sets XACT_ABORT ON).
        builder.Append(
            $" if @won = 0 begin begin try insert into {table} (tag_table, tag_value, tenant_id, version)" +
            $" values ({KeyValues}, next value for {sequence});" +
            " set @won = 1; end try begin catch");

        // 2627 = PK violation, 2601 = unique index violation. Anything else is not ours to swallow.
        builder.Append($" if error_number() in (2627, 2601) begin update {table} set version = next value for {sequence} where {KeyPredicate}");
        if (captured is not null)
        {
            builder.Append(" and version = 0");
        }

        builder.Append("; if @@rowcount > 0 set @won = 1; end else throw; end catch end");
    }

    /// <summary>
    ///     #972: a row a boundary read and the save does not append under. Lock it and compare it, and leave
    ///     its version alone.
    /// </summary>
    private void WriteCheck(BatchBuilder builder, long captured)
    {
        var table = _events.DcbTagVersionTableName;

        // ROWLOCK keeps it to the one key: this is a seek on the primary key. No HOLDLOCK, for the reason
        // given on the INSERT branch of WriteBump -- a range lock on a missing key lands on a neighbour.
        builder.Append("declare @v bigint; ");
        builder.Append($"select @v = version from {table} with (updlock, rowlock) where {KeyPredicate}; ");
        builder.Append("if @v = ");
        builder.AppendParameter(captured, SqlDbType.BigInt);
        builder.Append(" set @won = 1;");

        if (captured != 0) return;

        // A boundary that captured "no row" and still finds none has nothing to lose against -- but it also
        // has nothing to LOCK, and that is #4591's hazard: a producer could create the row and commit its
        // event between this check and this save's commit, and neither would wait for the other. So the check
        // creates the row, at 0, and holds it. 0 rather than a sequence value so that a second check-only
        // boundary over the same key, which also captured 0, still matches it: only an appended event moves a
        // version. A producer that arrives first creates the row from the sequence instead, and this check
        // then finds a value it cannot match. The duplicate is caught for the same reason it is in WriteBump.
        builder.Append(
            $" if @v is null begin begin try insert into {table} (tag_table, tag_value, tenant_id, version)" +
            $" values ({KeyValues}, 0); set @won = 1; end try begin catch" +
            $" if error_number() in (2627, 2601) begin select @v = version from {table} with (updlock, rowlock)" +
            $" where {KeyPredicate}; if @v = 0 set @won = 1; end else throw; end catch end");
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
            if (!won && _ordered[i].Captured is { } captured)
            {
                losers.Add(captured);
            }
        }

        foreach (var read in _reads)
        {
            await reader.NextResultAsync(token).ConfigureAwait(false);
            var remaining = await reader.ReadAsync(token).ConfigureAwait(false) ? reader.GetInt32(0) : 0;

            // Fewer, not different. The read can legitimately have kept fewer events than the query matches,
            // because an event of a type this application no longer knows is dropped as it is read.
            if (remaining < read.EventCount && !losers.Any(x => ReferenceEquals(x.Query, read.Query)))
            {
                losers.Add(new DcbTagVersionEntry(string.Empty, string.Empty, read.TenantId, 0, read.Query,
                    read.LastSeenSequence));
            }
        }

        return losers;
    }
}
