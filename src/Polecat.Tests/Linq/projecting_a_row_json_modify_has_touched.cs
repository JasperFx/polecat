using System.Text.Json;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using static Polecat.Tests.Patching.patching_values_and_parents;

namespace Polecat.Tests.Linq;

/// <summary>
///     #907: on native <c>json</c>, a <c>JSON_MODIFY</c> that adds a key can make the next one through an
///     object write to the wrong member. <c>JsonObjectFragment</c> nests <c>JSON_MODIFY</c> for a projection
///     streamed as JSON, so this reads rows in that state, using a shape that shows the bug.
/// </summary>
/// <remarks>
///     The fragment isn't exposed, and these are what pins that. The bug takes a <c>JSON_MODIFY</c> on a
///     <c>json</c> value, and the fragment's start from <c>JSON_OBJECT</c>, which yields
///     <c>nvarchar(max)</c>. So every write it nests works on text whatever the column is, and the column
///     itself only meets <c>JSON_VALUE</c> and <c>JSON_QUERY</c>, which read correctly.
/// </remarks>
public class projecting_a_row_json_modify_has_touched : OneOffConfigurationsContext
{
    private const string Schema = nameof(projecting_a_row_json_modify_has_touched);
    private const string AddsAKey = "JSON_MODIFY(data, N'$.text', N'added')";
    private const string WritesThroughAnObject = "JSON_MODIFY(data, N'$.map.child', 2)";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Shaped, with the non-nullable members that make the fragment nest more than one write.</summary>
    public class Valued
    {
        public Guid Id { get; set; }
        public string Alpha { get; set; } = "a";
        public List<Guid> Guids { get; set; } = [];
        public List<long> Longs { get; set; } = [1];
        public Dictionary<string, int> Map { get; set; } = new() { ["x"] = 1 };
        public Dictionary<string, int> Other { get; set; } = new();
        public List<string> Tags { get; set; } = ["t"];
        public string Zulu { get; set; } = "z";
        public int Count { get; set; } = 7;
        public bool Flag { get; set; } = true;
        public DateOnly Day { get; set; } = new(2026, 1, 2);
    }

    private sealed record ValuedDto(Guid Id, string Alpha, string Zulu, int Count, bool Flag, DateOnly Day);

    private sealed record ShapedDto(Guid Id, string Alpha, string Zulu, string? Text, int? Number);

    private async Task<SqlConnection> StoredAsync<T>(T document) where T : notnull
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "the fragment only runs on the native json type");
        ConfigureStore(opts => opts.UseNativeJsonType = true);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(document);
            await session.SaveChangesAsync(Ct);
        }

        return await OpenConnectionAsync();
    }

    private static async Task WriteAsync(SqlConnection conn, string table, string expression)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"UPDATE [{Schema}].[{table}] SET data = {expression};";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    /// <summary>A key that isn't there, which is what the first write needs. These are null members otherwise.</summary>
    private static Task RemoveNullMembersAsync(SqlConnection conn)
        => WriteAsync(conn, "pc_doc_shaped",
            "JSON_MODIFY(JSON_MODIFY(CAST(data AS nvarchar(max)), '$.text', NULL), '$.added', NULL)");

    [Fact]
    public async Task non_nullable_members_nest_their_writes_over_a_row_that_gained_a_key()
    {
        var id = Guid.NewGuid();
        var conn = await StoredAsync(new Valued { Id = id });
        await WriteAsync(conn, "pc_doc_valued", AddsAKey);

        await using var session = theStore.QuerySession();
        var json = await session.Query<Valued>().Where(x => x.Id == id)
            .Select(x => new { x.Id, x.Alpha, x.Zulu, x.Count, x.Flag, x.Day })
            .ToJsonArrayAsync(Ct);

        var projected = JsonSerializer.Deserialize<ValuedDto[]>(json, Web)!.ShouldHaveSingleItem();
        projected.ShouldBe(new ValuedDto(id, "a", "z", 7, true, new DateOnly(2026, 1, 2)));
    }

    [Fact]
    public async Task the_shape_that_shows_the_bug_projects_correctly_over_a_row_that_gained_a_key()
    {
        var id = Guid.NewGuid();
        var conn = await StoredAsync(new Shaped { Id = id, Number = 5 });
        await RemoveNullMembersAsync(conn);
        await WriteAsync(conn, "pc_doc_shaped", AddsAKey);

        await using var session = theStore.QuerySession();
        var json = await session.Query<Shaped>().Where(x => x.Id == id)
            .Select(x => new { x.Id, x.Alpha, x.Zulu, x.Text, x.Number })
            .ToJsonArrayAsync(Ct);

        JsonSerializer.Deserialize<ShapedDto[]>(json, Web)!.ShouldHaveSingleItem()
            .ShouldBe(new ShapedDto(id, "a", "z", "added", 5));
    }

    /// <summary>
    ///     This row's id is left unreadable by the second write, and the rest of it reads. A projection
    ///     has nothing wrong to report: every member it asks for is intact, and the id comes from the
    ///     column rather than the body.
    /// </summary>
    [Fact]
    public async Task a_row_the_bug_finished_with_projects_what_it_holds()
    {
        var id = Guid.NewGuid();
        var conn = await StoredAsync(new Shaped { Id = id, Number = 5 });
        await RemoveNullMembersAsync(conn);
        await WriteAsync(conn, "pc_doc_shaped", AddsAKey);
        await WriteAsync(conn, "pc_doc_shaped", WritesThroughAnObject);

        await using var session = theStore.QuerySession();
        var json = await session.Query<Shaped>().Where(x => x.Id == id)
            .Select(x => new { x.Id, x.Alpha, x.Zulu, x.Text, x.Number })
            .ToJsonArrayAsync(Ct);

        JsonSerializer.Deserialize<ShapedDto[]>(json, Web)!.ShouldHaveSingleItem()
            .ShouldBe(new ShapedDto(id, "a", "z", "added", 5));
    }
}
