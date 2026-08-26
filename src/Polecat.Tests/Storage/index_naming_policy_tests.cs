using System.Text.Json.Serialization;
using Weasel.Core;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     #510. The computed column an index creates has to read the path the SERIALIZER writes. The
///     index builder hardcoded camelCase while the LINQ translator applied the store's configured
///     policy, so on a <c>Casing.SnakeCase</c> store the column read <c>$.serviceName</c> while the
///     document held <c>service_name</c> — NULL for every row, an index that could never match, and
///     queries silently scanning <c>data</c> instead. Same shape as #507's alias bug, same silence.
///     These assert the column NAME and its DEFINITION, not just that a query returns the right row:
///     a query is satisfied by a fallback scan and so cannot tell a live index from a dead one.
/// </summary>
public class index_naming_policy_tests : OneOffConfigurationsContext
{
    public class SnakeMetric
    {
        public Guid Id { get; set; }
        public string ServiceName { get; set; } = string.Empty;
        public int RequestCount { get; set; }
        public Inner NestedThing { get; set; } = new();
    }

    public class Inner
    {
        public string RegionCode { get; set; } = string.Empty;
    }

    public class AliasedUnderSnake
    {
        public Guid Id { get; set; }

        [JsonPropertyName("BucketLabel")]
        public string Label { get; set; } = string.Empty;
    }

    [Fact]
    public async Task snake_case_store_indexes_the_path_the_serializer_writes()
    {
        ConfigureStore(opts =>
        {
            opts.ConfigureSerialization(casing: Casing.SnakeCase);
            opts.Schema.For<SnakeMetric>().Index(x => x.ServiceName);
        });

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SnakeMetric { Id = id, ServiceName = "checkout" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The column is named and defined from the serialized path, not the CLR name.
        var definition = await ComputedColumnDefinitionAsync("pc_doc_snakemetric", "cc_service_name");
        definition.ShouldNotBeNull("Expected a cc_service_name computed column");
        definition.ShouldContain("$.service_name");

        // And it actually holds the value — the whole point, since the old column was NULL forever.
        (await ReadColumnAsync("pc_doc_snakemetric", "cc_service_name", id)).ShouldBe("checkout");

        await using var query = theStore.QuerySession();
        var found = await query.Query<SnakeMetric>()
            .Where(x => x.ServiceName == "checkout")
            .ToListAsync(TestContext.Current.CancellationToken);
        found.Count.ShouldBe(1);
    }

    [Fact]
    public async Task snake_case_applies_to_every_segment_of_a_nested_path()
    {
        ConfigureStore(opts =>
        {
            opts.ConfigureSerialization(casing: Casing.SnakeCase);
            opts.Schema.For<SnakeMetric>().Index(x => x.NestedThing.RegionCode);
        });

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SnakeMetric { Id = id, NestedThing = new Inner { RegionCode = "eu-west" } });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var definition = await ComputedColumnDefinitionAsync(
            "pc_doc_snakemetric", "cc_nested_thing_region_code");
        definition.ShouldNotBeNull("Expected a cc_nested_thing_region_code computed column");
        definition.ShouldContain("$.nested_thing.region_code");

        (await ReadColumnAsync("pc_doc_snakemetric", "cc_nested_thing_region_code", id))
            .ShouldBe("eu-west");
    }

    [Fact]
    public async Task a_composite_index_types_each_column_through_the_renamed_path()
    {
        // The per-path SqlType overrides are keyed BY PATH, so re-rendering the paths has to carry
        // them across or a composite index silently loses a column's declared type.
        ConfigureStore(opts =>
        {
            opts.ConfigureSerialization(casing: Casing.SnakeCase);
            opts.Schema.For<SnakeMetric>().Index(x => new { x.ServiceName, x.RequestCount });
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SnakeMetric { Id = Guid.NewGuid(), ServiceName = "a", RequestCount = 3 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ComputedColumnDefinitionAsync("pc_doc_snakemetric", "cc_service_name"))
            .ShouldNotBeNull();

        // The numeric column keeps its CLR-derived int typing through the rename — if
        // ResolveClrMemberType failed to resolve the snake_case path it would fall back to a string
        // type here rather than refusing, which is the silent-mistyping mode.
        var countDefinition = await ComputedColumnDefinitionAsync(
            "pc_doc_snakemetric", "cc_request_count");
        countDefinition.ShouldNotBeNull();
        countDefinition!.ToLowerInvariant().ShouldContain("int");
    }

    [Fact]
    public async Task an_explicit_alias_still_wins_over_the_naming_policy()
    {
        // STJ does not apply the policy on top of an explicit [JsonPropertyName], so neither may we.
        // This is also the documented workaround for stores on the old behaviour.
        ConfigureStore(opts =>
        {
            opts.ConfigureSerialization(casing: Casing.SnakeCase);
            opts.Schema.For<AliasedUnderSnake>().Index(x => x.Label);
        });

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new AliasedUnderSnake { Id = id, Label = "kept" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // "BucketLabel" verbatim — NOT snake_cased into "bucket_label".
        var definition = await ComputedColumnDefinitionAsync(
            "pc_doc_aliasedundersnake", "cc_bucketlabel");
        definition.ShouldNotBeNull("Expected the alias to be used verbatim, not snake_cased");
        definition.ShouldContain("$.BucketLabel");

        (await ReadColumnAsync("pc_doc_aliasedundersnake", "cc_bucketlabel", id)).ShouldBe("kept");
    }

    [Fact]
    public async Task the_default_camel_case_store_is_unchanged()
    {
        // The overwhelming majority of stores are on the default, and they must see byte-identical
        // DDL to before this change.
        ConfigureStore(opts => opts.Schema.For<SnakeMetric>().Index(x => x.ServiceName));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SnakeMetric { Id = Guid.NewGuid(), ServiceName = "checkout" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var definition = await ComputedColumnDefinitionAsync("pc_doc_snakemetric", "cc_servicename");
        definition.ShouldNotBeNull("Expected the camelCase column name to be unchanged");
        definition.ShouldContain("$.serviceName");
    }

    private async Task<string?> ComputedColumnDefinitionAsync(string table, string column)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT cc.definition
            FROM sys.computed_columns cc
            WHERE cc.object_id = OBJECT_ID('[{Schema}].[{table}]') AND cc.name = @column;
            """;
        cmd.Parameters.AddWithValue("@column", column);

        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value == null || value == DBNull.Value ? null : (string)value;
    }

    private async Task<string?> ReadColumnAsync(string table, string column, Guid id)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT CONVERT(varchar(250), [{column}]) FROM [{Schema}].[{table}] WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", id);

        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value == DBNull.Value ? null : (string?)value;
    }

    private string Schema => GetType().Name.ToLowerInvariant();
}
