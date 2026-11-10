using System.Globalization;
using System.Text.Json;
using JasperFx.Pagination;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.CursorPaging;

/// <summary>
///     Keyset (seek) pagination mechanics — cursor encode/decode and the composite seek-predicate
///     builder. Mirrors Marten's <c>CursorPagination</c> (JasperFx/marten#5016). The cursor is an
///     opaque, versioned (<c>v1:</c>) base64-JSON value carrying the last row's sort-key values.
///     Values are typed on <em>decode</em> by the server-side ordering key type (the cursor never
///     dictates types) and enter the query as bound parameters — no injection.
/// </summary>
/// <remarks>
///     <para>
///         <b>NULL ordering: SQL Server's own, pinned.</b> A NULL key sorts first ascending and last
///         descending, so paging goes through the same plain <c>ORDER BY</c> every other query uses and
///         keeps its index. Marten sorts the other way, as PostgreSQL does, and a cursor from one store
///         never reaches the other. The rows a page hands back are the ones the seek predicate finds
///         after the previous page, so the predicate has to follow that order exactly (#838):
///     </para>
///     <list type="table">
///         <listheader><term>Last key</term><description>What comes after it</description></listheader>
///         <item><term>ascending, value</term><description><c>k &gt; v</c></description></item>
///         <item><term>ascending, NULL</term><description><c>k IS NOT NULL</c></description></item>
///         <item><term>descending, value</term><description><c>k &lt; v OR k IS NULL</c></description></item>
///         <item><term>descending, NULL</term><description>nothing on this key</description></item>
///     </list>
/// </remarks>
internal static class CursorPagination
{
    /// <summary>
    ///     The HTTP response header carrying the continuation cursor (mirrors Marten's
    ///     <c>Marten-Continuation</c>).
    /// </summary>
    public const string ContinuationHeader = "Polecat-Continuation";

    /// <summary>
    ///     Validates that the query's ordering can support keyset pagination: at least one
    ///     ORDER BY, and the terminal ordering key must be the unique document identity member so
    ///     the ordering is a total order (no skips/duplicates across ties). Throws
    ///     <see cref="InvalidOperationException"/> otherwise.
    /// </summary>
    public static void ValidateOrdering(IReadOnlyList<(IQueryableMember Member, bool Descending)> orderBy)
    {
        if (orderBy.Count == 0)
        {
            throw new InvalidOperationException(
                "Keyset (cursor) pagination requires an OrderBy clause. Add an OrderBy whose terminal key is the document identity (e.g. OrderBy(x => x.Something).ThenBy(x => x.Id)).");
        }

        if (orderBy[^1].Member is not IdMember)
        {
            throw new InvalidOperationException(
                "Keyset (cursor) pagination requires the terminal ordering key to be the unique document identity member (Id) so the ordering is a total order. End your ordering with ThenBy(x => x.Id) (or OrderBy(x => x.Id)).");
        }

        // Asked here, on page 1, rather than left to the next request's decode. Otherwise a client gets
        // a first page and a cursor that can never be redeemed (#838).
        foreach (var (member, _) in orderBy)
        {
            CursorKeyType.For(member);
        }
    }

    /// <summary>
    ///     Encodes the sort-key values of the last row on a page into an opaque continuation cursor.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #807: the codec is JasperFx's <see cref="CursorCodec" /> (jasperfx#1054), lifted from this
    ///         class and Fisher's. Its bytes are the ones written here before, so a cursor a client is
    ///         already holding stays valid across the upgrade; <c>cursor_codec_tests</c> pins them.
    ///         It writes with <see cref="Utf8JsonWriter" />, never <see cref="JsonSerializer" />, which
    ///         Native AOT disables (polecat#743).
    ///     </para>
    ///     <para>
    ///         A <c>time</c> key travels as a <see cref="TimeSpan" /> in its constant format, which is what
    ///         <c>ToString()</c> wrote. ⚠️ NOT as null: a null slot means a NULL key, so the next page
    ///         would seek from the wrong place.
    ///     </para>
    /// </remarks>
    public static string Encode(IReadOnlyList<object?> keyValues) => Codec.Encode(keyValues);

    /// <summary>
    ///     Decodes a cursor into typed sort-key values, one per ordering key. Each value is typed by the
    ///     SQL type its ordering key's locator is cast to (<see cref="CursorKeyType" />), which is what it
    ///     is bound as — the cursor payload itself carries no type information.
    /// </summary>
    public static object?[] Decode(string cursor,
        IReadOnlyList<(IQueryableMember Member, bool Descending)> orderBy)
    {
        var values = Codec.Decode(cursor, orderBy.Select(x => CursorKeyType.For(x.Member).ReadType).ToArray());

        // Every other key can legitimately be NULL. The identity cannot, so this was not issued here.
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is null && orderBy[i].Member is IdMember) throw MalformedSlot(null);
        }

        return values;
    }

    /// <summary>
    ///     Builds the composite keyset seek predicate for the values of the previous page's last row:
    ///     <c>(k0 after v0) OR (k0 = v0 AND k1 after v1) OR …</c>, where "after" follows the sort order
    ///     in the remarks on this class and a NULL value compares with <c>IS NULL</c>. Every value enters
    ///     as a bound parameter of the key's exact type, and each locator is the exact
    ///     <see cref="IQueryableMember.TypedLocator"/> used in the ORDER BY so the seek boundary lines
    ///     up with the sort.
    /// </summary>
    public static ISqlFragment BuildSeekPredicate(
        IReadOnlyList<(IQueryableMember Member, bool Descending)> orderBy, object?[] values)
    {
        ISqlFragment? predicate = null;

        for (var i = 0; i < orderBy.Count; i++)
        {
            // Nothing sorts after a NULL on a descending key, so this key cannot advance the seek.
            var clause = After(orderBy[i].Member, orderBy[i].Descending, values[i]);
            if (clause is null) continue;

            // Clause i: (k0 = v0 AND … AND k[i-1] = v[i-1] AND k[i] after v[i])
            for (var j = i - 1; j >= 0; j--)
            {
                clause = new CompoundWhereFragment("AND", Tied(orderBy[j].Member, values[j]), clause);
            }

            predicate = predicate is null ? clause : new CompoundWhereFragment("OR", predicate, clause);
        }

        return predicate!;
    }

    private static ISqlFragment Tied(IQueryableMember member, object? value)
    {
        return value is null
            ? new WhereFragment($"{member.TypedLocator} IS NULL")
            : new SeekComparison(member, "=", value);
    }

    private static ISqlFragment? After(IQueryableMember member, bool descending, object? value)
    {
        if (value is null)
        {
            return descending ? null : new WhereFragment($"{member.TypedLocator} IS NOT NULL");
        }

        if (!descending) return new SeekComparison(member, ">", value);

        return new CompoundWhereFragment("OR", new SeekComparison(member, "<", value),
            new WhereFragment($"{member.TypedLocator} IS NULL"));
    }

    /// <summary>
    ///     <c>locator op @p</c> with the parameter typed exactly as the locator. See
    ///     <see cref="CursorKeyType" />.
    /// </summary>
    private sealed class SeekComparison : ISqlFragment
    {
        private readonly string _locator;
        private readonly CursorKeyType _type;
        private readonly string _op;
        private readonly object _value;

        public SeekComparison(IQueryableMember member, string op, object value)
        {
            _locator = member.TypedLocator;
            _type = CursorKeyType.For(member);
            _op = op;
            _value = value;
        }

        public void Apply(ICommandBuilder builder)
        {
            builder.Append(_locator);
            builder.Append($" {_op} ");
            _type.ApplyTo(builder.AppendParameter(_value));
        }
    }

    private static readonly PolecatCursorCodec Codec = new();

    /// <summary>
    ///     The shared codec, typed the way Polecat binds: a slot is read as the CLR type of its key's SQL
    ///     type rather than of its member, so an enum stored by name reads back as the string it is bound as.
    /// </summary>
    private sealed class PolecatCursorCodec : CursorCodec
    {
        // ⚠️ A cursor is client input. A slot of the wrong shape is a bad request rather than a fault,
        // and StreamPagedByCursor turns only an ArgumentException into a 400. Thrown from here rather
        // than left to the base, so it keeps the message Polecat has always given it.
        protected override object? ConvertSlot(JsonElement slot, Type keyType)
        {
            try
            {
                return keyType == typeof(TimeSpan)
                    ? TimeSpan.Parse(slot.GetString()!, CultureInfo.InvariantCulture)
                    : base.ConvertSlot(slot, keyType);
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException or OverflowException)
            {
                throw MalformedSlot(ex);
            }
        }
    }

    private static ArgumentException MalformedSlot(Exception? inner)
        => new("Malformed cursor payload.", "cursor", inner);
}
