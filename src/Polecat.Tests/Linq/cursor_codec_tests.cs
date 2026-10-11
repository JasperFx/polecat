using System.Text;
using System.Text.Json;
using Polecat.Linq.CursorPaging;
using Shouldly;

namespace Polecat.Tests.Linq;

/// <summary>
///     polecat#743: the keyset cursor codec writes with <see cref="Utf8JsonWriter" /> and reads with
///     <see cref="JsonDocument" /> instead of the reflection-default <see cref="JsonSerializer" />
///     overloads, which cannot run under Native AOT. These pin that nothing a client can observe moved.
///     The end-to-end paging behaviour stays covered by <see cref="cursor_paging_tests" />.
/// </summary>
public class cursor_codec_tests
{
    public static IEnumerable<object[]> DecodableValues() =>
    [
        [null!],
        ["plain"],
        // the default encoder's escaping is part of the bytes: HTML-sensitive, quote, non-ASCII, '+'
        ["<tag> & \"quoted\" é + \u2028"],
        [Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff")],
        [true],
        [int.MinValue],
        [long.MaxValue],
        [(short)-7],
        [(byte)255],
        [123456789.012345678m],
        [0.1d],
        [double.MaxValue],
        [1.5f],
        [new DateTimeOffset(2026, 10, 11, 9, 30, 15, 123, TimeSpan.FromHours(-5))],
        [new DateTime(2026, 10, 11, 9, 30, 15, 123, DateTimeKind.Utc)],
        [new DateTime(2026, 10, 11, 9, 30, 15, DateTimeKind.Unspecified)]
    ];

    // ⚠️ The one that matters. A cursor is handed to a client and comes back later, possibly across an
    // upgrade, so the codec change is only safe if every byte it writes is what JsonSerializer wrote.
    [Theory]
    [MemberData(nameof(DecodableValues))]
    public void encodes_the_same_bytes_the_serializer_did(object? value)
    {
        var expected = "v1:" + Convert.ToBase64String(
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new[] { value })));

        CursorPagination.Encode([value]).ShouldBe(expected);
    }

    [Fact]
    public void encodes_several_keys_the_same_way_too()
    {
        object?[] keys = ["b", 42, Guid.Parse("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), null];
        var expected = "v1:" + Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(keys)));

        CursorPagination.Encode(keys).ShouldBe(expected);
    }

    [Fact]
    public void a_database_null_is_written_as_null()
    {
        Payload(CursorPagination.Encode([DBNull.Value])).ShouldBe("[null]");
    }

    // ⚠️ NOT null, which is the obvious placeholder: ConvertSlot answers a null slot before it checks the
    // member type, so a null here would decode quietly instead of being refused on the next request.
    [Fact]
    public void an_undecodable_key_is_written_as_a_string_so_decode_still_refuses_it()
    {
        Payload(CursorPagination.Encode([TimeSpan.FromSeconds(90)])).ShouldBe("[\"00:01:30\"]");
    }

    [Fact]
    public void a_null_payload_decodes_as_no_keys()
    {
        CursorPagination.Decode(Cursor("null"), []).ShouldBeEmpty();
    }

    [Theory]
    [InlineData("{\"a\":1}")]   // well-formed JSON, wrong shape
    [InlineData("[1,")]         // not JSON at all
    [InlineData("\"text\"")]
    public void a_malformed_payload_is_refused_as_before(string payload)
    {
        var ex = Should.Throw<ArgumentException>(() => CursorPagination.Decode(Cursor(payload), []));
        ex.Message.ShouldStartWith("Malformed cursor payload.");
    }

    [Fact]
    public void bad_base64_is_refused_as_before()
    {
        Should.Throw<ArgumentException>(() => CursorPagination.Decode("v1:%%%not-base64", []))
            .Message.ShouldStartWith("Malformed cursor payload.");
    }

    [Fact]
    public void a_key_count_mismatch_still_names_the_counts()
    {
        Should.Throw<ArgumentException>(() => CursorPagination.Decode(Cursor("[1]"), []))
            .Message.ShouldStartWith("Cursor carries 1 key(s) but the query orders by 0.");
    }

    private static string Cursor(string json) => "v1:" + Convert.ToBase64String(Encoding.UTF8.GetBytes(json));

    private static string Payload(string cursor) =>
        Encoding.UTF8.GetString(Convert.FromBase64String(cursor["v1:".Length..]));
}
