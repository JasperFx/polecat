using System.Text.Json;
using Polecat.Linq.Members;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Linq.CursorPaging;

/// <summary>
///     Keyset (seek) pagination mechanics — cursor encode/decode and the composite seek-predicate
///     builder. Mirrors Marten's <c>CursorPagination</c> (JasperFx/marten#5016). The cursor is an
///     opaque, versioned (<c>v1:</c>) base64-JSON value carrying the last row's sort-key values.
///     Values are typed on <em>decode</em> by the server-side ordering key type (the cursor never
///     dictates types) and enter the query as bound parameters — no injection.
/// </summary>
internal static class CursorPagination
{
    private const string Version = "v1:";

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
    }

    /// <summary>
    ///     Encodes the sort-key values of the last row on a page into an opaque continuation cursor.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Written with <see cref="Utf8JsonWriter" />, not <see cref="JsonSerializer" /></b>
    ///         (polecat#743). This used to be <c>JsonSerializer.Serialize(object?[])</c> — the
    ///         reflection-default overload, which takes no options, so it never sees a consumer's
    ///         source-generated <c>JsonSerializerContext</c>. Under Native AOT reflection-based STJ is
    ///         disabled, so cursor paging threw on every page that had a successor, and there was nothing a
    ///         consumer could register to stop it. Found by <c>Polecat.AotRuntimeSmoke</c>.
    ///     </para>
    ///     <para>
    ///         The bytes are unchanged, which is what keeps a cursor a client is already holding valid across
    ///         an upgrade: for every value type below, STJ's own converter is a single call to the matching
    ///         <see cref="Utf8JsonWriter" /> method with the same default encoder, so this writes what it wrote.
    ///         <c>cursor_codec_tests</c> pins that per type against <see cref="JsonSerializer" /> itself.
    ///     </para>
    ///     <para>
    ///         The set is exactly what <see cref="ConvertSlot" /> can decode. A key of any other type was
    ///         always refused — on the NEXT request, by decode — and that boundary stays where it was: such
    ///         a value is written as a string, so the next request still fails with the same message from the
    ///         same place. ⚠️ NOT as null, which reads as the obvious placeholder and is wrong:
    ///         <see cref="ConvertSlot" /> answers a null slot before it checks the type, so a null would
    ///         decode quietly instead of being refused.
    ///     </para>
    /// </remarks>
    public static string Encode(IReadOnlyList<object?> keyValues)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var value in keyValues)
            {
                WriteSlot(writer, NormalizeForCursor(value));
            }

            writer.WriteEndArray();
        }

        return Version + Convert.ToBase64String(buffer.WrittenSpan);
    }

    private static void WriteSlot(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string x: writer.WriteStringValue(x); break;
            case Guid x: writer.WriteStringValue(x); break;
            case bool x: writer.WriteBooleanValue(x); break;
            case int x: writer.WriteNumberValue(x); break;
            case long x: writer.WriteNumberValue(x); break;
            case short x: writer.WriteNumberValue(x); break;
            case byte x: writer.WriteNumberValue(x); break;
            case decimal x: writer.WriteNumberValue(x); break;
            case double x: writer.WriteNumberValue(x); break;
            case float x: writer.WriteNumberValue(x); break;
            case DateTimeOffset x: writer.WriteStringValue(x); break;
            case DateTime x: writer.WriteStringValue(x); break;

            // Not decodable, so decode refuses it on the next request, as it always has. A string, not a
            // null: see the remarks on Encode.
            default: writer.WriteStringValue(value.ToString()); break;
        }
    }

    /// <summary>
    ///     Decodes a cursor into typed sort-key values, one per ordering key. The values are typed by
    ///     each ordering member's CLR type — the cursor payload itself carries no type information.
    /// </summary>
    public static object?[] Decode(string cursor,
        IReadOnlyList<(IQueryableMember Member, bool Descending)> orderBy)
    {
        if (!cursor.StartsWith(Version, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Unrecognized or unversioned cursor. Expected a '{Version}' prefixed value.", nameof(cursor));
        }

        // Parsed with JsonDocument rather than JsonSerializer.Deserialize<JsonElement[]>, for the reason
        // Encode's remarks give: the reflection-default overload cannot run under Native AOT (#743).
        JsonElement[] slots;
        try
        {
            using var document = JsonDocument.Parse(Convert.FromBase64String(cursor[Version.Length..]));
            slots = document.RootElement.ValueKind switch
            {
                JsonValueKind.Array => document.RootElement.EnumerateArray().Select(x => x.Clone()).ToArray(),
                JsonValueKind.Null => [],
                _ => throw new JsonException("A cursor payload is a JSON array.")
            };
        }
        catch (Exception ex) when (ex is FormatException or JsonException)
        {
            throw new ArgumentException("Malformed cursor payload.", nameof(cursor), ex);
        }

        if (slots.Length != orderBy.Count)
        {
            throw new ArgumentException(
                $"Cursor carries {slots.Length} key(s) but the query orders by {orderBy.Count}. The cursor does not match this query's ordering.",
                nameof(cursor));
        }

        var values = new object?[slots.Length];
        for (var i = 0; i < slots.Length; i++)
        {
            values[i] = ConvertSlot(slots[i], orderBy[i].Member.MemberType);
        }

        return values;
    }

    /// <summary>
    ///     Builds the composite keyset seek predicate for the values of the previous page's last row:
    ///     <c>(k0 op0 v0) OR (k0 = v0 AND k1 op1 v1) OR …</c> where <c>opN</c> is <c>&gt;</c> for an
    ///     ascending key and <c>&lt;</c> for a descending one. Every value enters as a bound
    ///     parameter, and each locator is the exact <see cref="IQueryableMember.TypedLocator"/> used
    ///     in the ORDER BY so the seek boundary lines up with the sort.
    /// </summary>
    public static ISqlFragment BuildSeekPredicate(
        IReadOnlyList<(IQueryableMember Member, bool Descending)> orderBy, object?[] values)
    {
        ISqlFragment? predicate = null;

        for (var i = 0; i < orderBy.Count; i++)
        {
            // Clause i: (k0 = v0 AND … AND k[i-1] = v[i-1] AND k[i] op v[i])
            ISqlFragment? clause = null;
            for (var j = 0; j <= i; j++)
            {
                var locator = orderBy[j].Member.TypedLocator;
                var op = j < i ? "=" : (orderBy[j].Descending ? "<" : ">");
                ISqlFragment comparison = new ComparisonFilter(locator, op, values[j]!);
                clause = clause is null ? comparison : new CompoundWhereFragment("AND", clause, comparison);
            }

            predicate = predicate is null ? clause! : new CompoundWhereFragment("OR", predicate, clause!);
        }

        return predicate!;
    }

    // Read-side values arrive already normalized to CLR primitives / Guid / DateTimeOffset, which STJ
    // serializes losslessly. Guid/DateTimeOffset become strings; numbers/bools stay native.
    private static object? NormalizeForCursor(object? value) =>
        value is DBNull ? null : value;

    private static object? ConvertSlot(JsonElement slot, Type memberType)
    {
        var target = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (slot.ValueKind == JsonValueKind.Null) return null;

        if (target == typeof(Guid)) return slot.GetGuid();
        if (target == typeof(string)) return slot.GetString();
        if (target == typeof(int)) return slot.GetInt32();
        if (target == typeof(long)) return slot.GetInt64();
        if (target == typeof(short)) return slot.GetInt16();
        if (target == typeof(byte)) return slot.GetByte();
        if (target == typeof(bool)) return slot.GetBoolean();
        if (target == typeof(decimal)) return slot.GetDecimal();
        if (target == typeof(double)) return slot.GetDouble();
        if (target == typeof(float)) return slot.GetSingle();
        if (target == typeof(DateTimeOffset)) return slot.GetDateTimeOffset();
        if (target == typeof(DateTime)) return slot.GetDateTime();

        throw new BadLinqExpressionException(
            $"Keyset (cursor) pagination does not support an ordering key of type '{memberType.Name}'. " +
            "Order by primitive/Guid/DateTime(Offset) keys with the document identity as the terminal key.");
    }
}
