using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.TestUtils;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     Coverage for translating simple <c>Select()</c> projections to a server-side JSON object plus
///     the two correctness guards (marten#5017 parity, polecat#358). The JSON_OBJECT optimization
///     requires SQL Server 2025 native JSON, so those facts skip on non-native (edge) stores; the
///     guards (throw on non-streamable projection; client-side fallback for ToList) are universal.
/// </summary>
[Collection("integration")]
public class select_projection_json_tests : IntegrationContext
{
    public select_projection_json_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public class NameYears
    {
        public string? PersonName { get; set; }
        public int Years { get; set; }

        [JsonPropertyName("custom_city")]
        public string? City { get; set; }
    }

    private async Task<string> SeedAsync(int count)
    {
        var name = $"Proj_{Guid.NewGuid():N}";
        for (var i = 0; i < count; i++)
        {
            theSession.Store(new LinqTarget
            {
                Id = Guid.NewGuid(),
                Name = name,
                Age = i,
                BigNumber = i,
                Address = new Address { City = $"City{i}", State = "TX" }
            });
        }

        await theSession.SaveChangesAsync();
        return name;
    }

    [Fact]
    public async Task simple_anonymous_projection_streams_json_object()
    {
        if (!ConnectionSource.SupportsNativeJson) return;

        var name = await SeedAsync(3);

        await using var query = theStore.QuerySession();
        var json = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .Select(x => new { x.Name, x.Age })
            .ToJsonArrayAsync();

        var array = JsonDocument.Parse(json).RootElement;
        array.GetArrayLength().ShouldBe(3);

        foreach (var element in array.EnumerateArray())
        {
            element.GetProperty("name").GetString().ShouldBe(name);
            element.TryGetProperty("age", out _).ShouldBeTrue();
            // The projection must NOT leak un-projected members.
            element.TryGetProperty("id", out _).ShouldBeFalse();
            element.TryGetProperty("address", out _).ShouldBeFalse();
        }
    }

    [Fact]
    public async Task nested_member_projection_streams_json_object()
    {
        if (!ConnectionSource.SupportsNativeJson) return;

        var name = await SeedAsync(2);

        await using var query = theStore.QuerySession();
        var json = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .OrderBy(x => x.Age)
            .Select(x => new { x.Age, City = x.Address!.City })
            .ToJsonArrayAsync();

        var array = JsonDocument.Parse(json).RootElement;
        array[0].GetProperty("city").GetString().ShouldBe("City0");
        array[0].GetProperty("age").GetInt32().ShouldBe(0);
        array[1].GetProperty("city").GetString().ShouldBe("City1");
    }

    [Fact]
    public async Task dto_projection_honors_json_property_name_and_naming_policy()
    {
        if (!ConnectionSource.SupportsNativeJson) return;

        var name = await SeedAsync(1);

        await using var query = theStore.QuerySession();
        var json = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .Select(x => new NameYears { PersonName = x.Name, Years = x.Age, City = x.Address!.City })
            .ToJsonArrayAsync();

        var obj = JsonDocument.Parse(json).RootElement[0];
        obj.GetProperty("personName").GetString().ShouldBe(name);   // naming policy (camelCase)
        obj.GetProperty("years").GetInt32().ShouldBe(0);
        obj.GetProperty("custom_city").GetString().ShouldBe("City0"); // [JsonPropertyName] wins verbatim
    }

    [Fact]
    public async Task safe_widening_conversion_stays_simple()
    {
        if (!ConnectionSource.SupportsNativeJson) return;

        var name = await SeedAsync(2);

        await using var query = theStore.QuerySession();
        var json = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .OrderBy(x => x.Age)
            .Select(x => new { Big = (long)x.Age })
            .ToJsonArrayAsync();

        var array = JsonDocument.Parse(json).RootElement;
        array[0].GetProperty("big").GetInt64().ShouldBe(0L);
        array[1].GetProperty("big").GetInt64().ShouldBe(1L);
    }

    // ---------- Correctness guards (universal — run on native and non-native alike) ----------

    [Fact]
    public async Task streaming_a_method_call_projection_throws()
    {
        var name = await SeedAsync(1);

        await using var query = theStore.QuerySession();
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<LinqTarget>()
                .Where(x => x.Name == name)
                .Select(x => new { Upper = x.Name!.ToUpper() })
                .ToJsonArrayAsync());
    }

    [Fact]
    public async Task streaming_an_arithmetic_projection_throws()
    {
        var name = await SeedAsync(1);

        await using var query = theStore.QuerySession();
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<LinqTarget>()
                .Where(x => x.Name == name)
                .Select(x => new { Doubled = x.Age * 2 })
                .ToJsonArrayAsync());
    }

    [Fact]
    public async Task streaming_a_scalar_projection_throws()
    {
        var name = await SeedAsync(1);

        await using var query = theStore.QuerySession();
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<LinqTarget>()
                .Where(x => x.Name == name)
                .Select(x => x.Name)
                .ToJsonArrayAsync());
    }

    [Fact]
    public async Task non_simple_projection_falls_back_to_client_side_for_to_list()
    {
        // Guard 1: a non-translatable Select() must NOT silently drop the unsupported operation — it
        // falls back to a client-side transform when the results are materialized (not streamed).
        var name = await SeedAsync(1);

        await using var query = theStore.QuerySession();
        var results = await query.Query<LinqTarget>()
            .Where(x => x.Name == name)
            .Select(x => new { Upper = x.Name!.ToUpper() })
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Upper.ShouldBe(name.ToUpper());
    }
}
