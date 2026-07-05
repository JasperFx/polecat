using System.Text.Json.Serialization;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Patching;

/// <summary>
///     #270: Patch().Set(...) must target the JSON key the document was actually written with. A
///     [JsonPropertyName] override on the property (City → "cityName") and a
///     [JsonStringEnumMemberName] override on an enum member (Active → "enabled") were ignored: the
///     path resolver only applied the naming policy, so JSON_MODIFY wrote a *second* "city" key
///     instead of updating "cityName".
/// </summary>
public class patching_json_naming_attributes : OneOffConfigurationsContext
{
    public enum ActiveStatus
    {
        [JsonStringEnumMemberName("enabled")]
        Active,

        [JsonStringEnumMemberName("disabled")]
        Inactive
    }

    public class PatchUser
    {
        public Guid Id { get; set; }

        [JsonPropertyName("cityName")]
        public string City { get; set; } = string.Empty;

        public ActiveStatus Status { get; set; }
    }

    private async Task<string> RawJsonAsync(Guid id)
    {
        var schema = theStore.Options.DatabaseSchemaName;
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT CAST(data AS nvarchar(max)) FROM [{schema}].[pc_doc_patchuser] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task set_honors_json_property_name_and_enum_member_name()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsString));

        var user = new PatchUser { Id = Guid.NewGuid(), City = "Milan", Status = ActiveStatus.Inactive };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(user);
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<PatchUser>(user.Id).Set(x => x.City, "Taggia");
            session.Patch<PatchUser>(user.Id).Set(x => x.Status, ActiveStatus.Active);
            await session.SaveChangesAsync();
        }

        var json = await RawJsonAsync(user.Id);

        // The patch must UPDATE the existing "cityName" key, not create a stray "city" key.
        json.ShouldContain("\"cityName\":\"Taggia\"");
        json.ShouldNotContain("\"city\":");
        // Enum value serialized through the [JsonStringEnumMemberName] override.
        json.ShouldContain("\"status\":\"enabled\"");

        await using var query = theStore.QuerySession();
        var reloaded = (await query.LoadAsync<PatchUser>(user.Id))!;
        reloaded.City.ShouldBe("Taggia");
        reloaded.Status.ShouldBe(ActiveStatus.Active);
    }
}
