using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #839, and the twin of marten#5759: what a string predicate means by "whitespace" and by "empty"
///     has to be what .NET means, not what <c>LTRIM</c>, <c>RTRIM</c> and <c>=</c> happen to do.
/// </summary>
/// <remarks>
///     <para>
///         Two independent traps. <c>LTRIM(x)</c> with no second argument strips only U+0020. And
///         <c>x = ''</c> is true for spaces, because <c>=</c> pads with trailing spaces, and for anything
///         the collation gives no weight at all, so U+FEFF equals the empty string.
///     </para>
///     <para>
///         ⚠️ <b>Fixing the first with <c>TRIM(chars FROM x)</c> is not enough on its own</b>, and the
///         per-character theory below is what shows it. <c>TRIM</c> matches through the collation too,
///         so under the default <c>SQL_Latin1_General_CP1_CI_AS</c> it cannot remove U+1680, U+202F or
///         U+205F, which that collation treats as nothing. Only a binary collation matches the
///         characters themselves.
///     </para>
/// </remarks>
public class whitespace_string_predicate_tests: OneOffConfigurationsContext
{
    public class TextDoc
    {
        public Guid Id { get; set; }
        public string? Text { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);
    }

    private async Task<Guid> storeAsync(string? text, string code = "")
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new TextDoc { Id = id, Text = text, Code = code });
        await session.SaveChangesAsync(Ct);
        return id;
    }

    private async Task<Guid[]> idsAsync(Func<IQueryable<TextDoc>, IQueryable<TextDoc>> where)
    {
        await using var session = theStore.QuerySession();
        return (await where(session.Query<TextDoc>()).ToListAsync(Ct)).Select(x => x.Id).ToArray();
    }

    // Every value string.IsNullOrWhiteSpace answers true for one character at a time, spelled as code
    // points so that an invisible character in this file cannot quietly drop out of the list.
    public static TheoryData<bool, int> EveryWhitespaceCharacter()
    {
        var data = new TheoryData<bool, int>();

        for (var c = 0; c <= char.MaxValue; c++)
        {
            if (!char.IsWhiteSpace((char)c)) continue;

            data.Add(true, c);
            data.Add(false, c);
        }

        return data;
    }

    // ---- the issue's table ----------------------------------------------------------------------

    [Theory]
    [InlineData(true, "\t")]
    [InlineData(true, "\r\n")]
    [InlineData(false, "\t")]
    [InlineData(false, "\r\n")]
    public async Task is_null_or_white_space_matches_tabs_and_line_breaks(bool nativeJson, string value)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync(value);

        (await idsAsync(q => q.Where(x => string.IsNullOrWhiteSpace(x.Text)))).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task is_null_or_empty_does_not_match_spaces(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var spaces = await storeAsync("   ");
        var empty = await storeAsync("");
        var nothing = await storeAsync(null);

        (await idsAsync(q => q.Where(x => string.IsNullOrEmpty(x.Text))))
            .OrderBy(x => x).ShouldBe(new[] { empty, nothing }.OrderBy(x => x));

        (await idsAsync(q => q.Where(x => !string.IsNullOrEmpty(x.Text)))).ShouldBe([spaces]);
    }

    // ---- what .NET says, character by character ---------------------------------------------------

    [Theory]
    [MemberData(nameof(EveryWhitespaceCharacter))]
    public async Task every_dotnet_whitespace_character_is_whitespace(bool nativeJson, int codePoint)
    {
        ConfigureFor(nativeJson);
        var value = new string((char)codePoint, 2);
        var id = await storeAsync(value);

        (await idsAsync(q => q.Where(x => string.IsNullOrWhiteSpace(x.Text)))).ShouldBe([id]);
        (await idsAsync(q => q.Where(x => !string.IsNullOrWhiteSpace(x.Text)))).ShouldBeEmpty();
    }

    [Theory]
    [MemberData(nameof(EveryWhitespaceCharacter))]
    public async Task every_dotnet_whitespace_character_is_trimmed(bool nativeJson, int codePoint)
    {
        ConfigureFor(nativeJson);
        var padding = ((char)codePoint).ToString();
        var id = await storeAsync(null, padding + "ABC" + padding);

        (await idsAsync(q => q.Where(x => x.Code.Trim() == "ABC"))).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true, 0x200B)] // zero width space, which .NET does not call whitespace
    [InlineData(true, 0xFEFF)] // BOM. `U+FEFF = ''` is TRUE under the default collation
    [InlineData(true, 0x00AD)] // soft hyphen
    [InlineData(true, 0x180E)] // whitespace before Unicode 6.3, and not since
    [InlineData(true, 0x2060)] // word joiner
    [InlineData(false, 0x200B)]
    [InlineData(false, 0xFEFF)]
    [InlineData(false, 0x00AD)]
    [InlineData(false, 0x180E)]
    [InlineData(false, 0x2060)]
    public async Task an_invisible_character_is_not_whitespace_or_empty(bool nativeJson, int codePoint)
    {
        ConfigureFor(nativeJson);
        string.IsNullOrWhiteSpace(((char)codePoint).ToString()).ShouldBeFalse();

        await storeAsync(((char)codePoint).ToString());

        (await idsAsync(q => q.Where(x => string.IsNullOrWhiteSpace(x.Text)))).ShouldBeEmpty();
        (await idsAsync(q => q.Where(x => string.IsNullOrEmpty(x.Text)))).ShouldBeEmpty();
    }

    // ---- Trim, TrimStart, TrimEnd ---------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task trim_removes_tabs_and_line_breaks(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync(null, "\tABC\n");
        await storeAsync(null, "XYZ");

        (await idsAsync(q => q.Where(x => x.Code.Trim() == "ABC"))).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task trim_start_and_trim_end_remove_only_their_own_side(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var left = await storeAsync(null, "\t ABC");
        var right = await storeAsync(null, "ABC\r\n");

        (await idsAsync(q => q.Where(x => x.Code.TrimStart() == "ABC"))).ShouldBe([left]);
        (await idsAsync(q => q.Where(x => x.Code.TrimEnd() == "ABC"))).ShouldBe([right]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task trim_does_not_change_how_the_comparison_collates(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync(null, "\tABC\n");

        // TRIM has to match under a binary collation, and an expression keeps the collation it was given.
        // Left there, this comparison would turn case-sensitive on a case-insensitive database, a quiet
        // change to every Trim() predicate in an application. The expected answer is read from the server
        // so this states "unchanged" rather than a default the suite's own database happens to have.
        var insensitive = await serverComparesCaseInsensitivelyAsync();

        var hits = await idsAsync(q => q.Where(x => x.Code.Trim() == "abc"));
        hits.ShouldBe(insensitive ? [id] : []);
    }

    private async Task<bool> serverComparesCaseInsensitivelyAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CASE WHEN N'abc' = N'ABC' THEN 1 ELSE 0 END";
        return (int)(await cmd.ExecuteScalarAsync(Ct))! == 1;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task trim_reads_past_the_json_value_limit(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var body = new string('q', 5000);
        var id = await storeAsync(null, "\t" + body + "\n");

        (await idsAsync(q => q.Where(x => x.Code.Trim() == body))).ShouldBe([id]);
    }
}
