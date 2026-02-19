using System.Text.Json;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

[Collection("integration")]
public class json_streaming_tests : IntegrationContext
{
    public json_streaming_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task load_json_by_guid_id()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "JsonAlice", LastName = "Smith" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var json = await query.LoadJsonAsync<User>(user.Id);

        json.ShouldNotBeNull();
        json.ShouldContain("JsonAlice");
        json.ShouldContain("Smith");

        // Should be valid JSON
        var parsed = JsonDocument.Parse(json);
        parsed.RootElement.GetProperty("firstName").GetString().ShouldBe("JsonAlice");
    }

    [Fact]
    public async Task load_json_by_string_id()
    {
        var doc = new StringDoc { Id = "json-test-1", Name = "JsonTest" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var json = await query.LoadJsonAsync<StringDoc>("json-test-1");

        json.ShouldNotBeNull();
        json.ShouldContain("JsonTest");
    }

    [Fact]
    public async Task load_json_returns_null_for_missing()
    {
        await using var query = theStore.QuerySession();
        var json = await query.LoadJsonAsync<User>(Guid.NewGuid());

        json.ShouldBeNull();
    }

    [Fact]
    public async Task to_json_array_returns_valid_json_array()
    {
        var uniqueColor = $"JsonColor_{Guid.NewGuid():N}";
        var t1 = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 1 };
        var t2 = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 2 };

        theSession.Store(t1, t2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var json = await query.Query<Target>()
            .Where(t => t.Color == uniqueColor)
            .ToJsonArrayAsync();

        json.ShouldNotBeNull();
        json.ShouldStartWith("[");
        json.ShouldEndWith("]");

        var array = JsonDocument.Parse(json).RootElement;
        array.ValueKind.ShouldBe(JsonValueKind.Array);
        array.GetArrayLength().ShouldBe(2);
    }

    [Fact]
    public async Task to_json_array_empty_result()
    {
        await using var query = theStore.QuerySession();
        var json = await query.Query<Target>()
            .Where(t => t.Color == "NeverExistsColor999")
            .ToJsonArrayAsync();

        json.ShouldBe("[]");
    }

    [Fact]
    public async Task to_json_array_with_single_result()
    {
        var uniqueColor = $"SingleJson_{Guid.NewGuid():N}";
        var t = new Target { Id = Guid.NewGuid(), Color = uniqueColor, Number = 42 };

        theSession.Store(t);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var json = await query.Query<Target>()
            .Where(t => t.Color == uniqueColor)
            .ToJsonArrayAsync();

        var array = JsonDocument.Parse(json).RootElement;
        array.GetArrayLength().ShouldBe(1);
        array[0].GetProperty("number").GetInt32().ShouldBe(42);
    }
}
