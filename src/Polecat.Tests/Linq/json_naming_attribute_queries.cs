using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Linq;

/// <summary>
/// #270: System.Text.Json member-name override attributes — [JsonPropertyName] on a property and
/// [JsonStringEnumMemberName] on an enum member — are honored when documents are written, so the
/// stored JSON uses "cityName" and "enabled"/"disabled". But the LINQ translator ignored them: it
/// applied only the naming *policy*, emitting JSON_VALUE(data, '$.city') = 'active' instead of
/// JSON_VALUE(data, '$.cityName') = 'enabled', so predicates over these members matched nothing.
/// </summary>
public class json_naming_attribute_queries : OneOffConfigurationsContext
{
    public enum ActiveStatus
    {
        [JsonStringEnumMemberName("enabled")]
        Active,

        [JsonStringEnumMemberName("disabled")]
        Inactive
    }

    public class User
    {
        public Guid Id { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;

        [JsonPropertyName("cityName")]
        public string City { get; set; } = string.Empty;

        public ActiveStatus Status { get; set; }
    }

    private async Task SeedAsync()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsString));

        await using var session = theStore.LightweightSession();
        session.Store(new User
        {
            Id = Guid.NewGuid(), FirstName = "Evan", LastName = "Kutch",
            City = "Taggia", Status = ActiveStatus.Active
        });
        session.Store(new User
        {
            Id = Guid.NewGuid(), FirstName = "Jane", LastName = "Doe",
            City = "Taggia", Status = ActiveStatus.Inactive
        });
        session.Store(new User
        {
            Id = Guid.NewGuid(), FirstName = "John", LastName = "Roe",
            City = "Milan", Status = ActiveStatus.Active
        });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task query_by_json_property_name_override()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();

        // City is stored under "cityName" — the predicate must target $.cityName, not $.city.
        var results = await query.Query<User>()
            .Where(u => u.City == "Taggia")
            .ToListAsync();

        results.Count.ShouldBe(2);
        results.ShouldAllBe(u => u.City == "Taggia");
    }

    [Fact]
    public async Task query_by_json_string_enum_member_name_override()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();

        // Active is stored as "enabled" — the predicate must compare against "enabled".
        var active = await query.Query<User>()
            .Where(u => u.Status == ActiveStatus.Active)
            .ToListAsync();

        active.Count.ShouldBe(2);
        active.ShouldAllBe(u => u.Status == ActiveStatus.Active);
    }

    [Fact]
    public async Task query_the_reporters_exact_predicate()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();

        // The exact repro from the issue: combined enum + [JsonPropertyName] predicate.
        var users = await query.Query<User>()
            .Where(u => u.Status == ActiveStatus.Active && u.City == "Taggia")
            .OrderBy(u => u.FirstName).ThenBy(u => u.LastName)
            .ToListAsync();

        users.Count.ShouldBe(1);
        users[0].FirstName.ShouldBe("Evan");
    }
}
