using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Patching;

/// <summary>
///     #262: Patch().Set(...) on an enum property ignored the configured EnumStorage and bound
///     the raw integer value into JSON_MODIFY, so a store configured with EnumStorage.AsString
///     ended up with an integer in the document body instead of the string representation.
///     STJ's string-enum converter happily reads integers back, so these tests assert the raw
///     stored JSON rather than only the round-tripped value.
/// </summary>
public class patching_enum_storage : OneOffConfigurationsContext
{
    private async Task<string> RawJsonAsync(Guid id)
    {
        var schema = theStore.Options.DatabaseSchemaName;
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT CAST(data AS nvarchar(max)) FROM [{schema}].[pc_doc_target] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task set_enum_with_as_string_storage_writes_string_representation()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsString));

        var target = new Target { Id = Guid.NewGuid(), Status = ActiveStatus.Inactive };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(target);
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Target>(target.Id).Set(x => x.Status, ActiveStatus.Active);
            await session.SaveChangesAsync();
        }

        // The bug emitted "status":0; with AsString + CamelCase naming it must be the string name.
        var json = await RawJsonAsync(target.Id);
        json.ShouldContain("\"status\":\"active\"");

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Status.ShouldBe(ActiveStatus.Active);
    }

    [Fact]
    public async Task set_enum_with_as_integer_storage_writes_numeric_representation()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsInteger));

        var target = new Target { Id = Guid.NewGuid(), Status = ActiveStatus.Active };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(target);
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Target>(target.Id).Set(x => x.Status, ActiveStatus.Inactive);
            await session.SaveChangesAsync();
        }

        var json = await RawJsonAsync(target.Id);
        json.ShouldContain("\"status\":1");

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Status.ShouldBe(ActiveStatus.Inactive);
    }
}
