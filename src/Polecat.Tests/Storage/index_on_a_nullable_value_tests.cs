using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Storage;

/// <summary>
///     #875. <c>.Value</c> on a nullable member is C#, not a key: the serializer writes the value at
///     the member itself. A path of <c>$.n.value</c> reads nothing, so the index never matches a row
///     and a patch writes somewhere nothing will ever read.
/// </summary>
public class index_on_a_nullable_value_tests: OneOffConfigurationsContext
{
    public struct Money
    {
        public decimal Amount { get; set; }
    }

    public class Inner
    {
        public int? Count { get; set; }
    }

    public class Doc
    {
        public Guid Id { get; set; }
        public int? N { get; set; }
        public Money? Cost { get; set; }
        public Inner Nested { get; set; } = new();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<string?> ComputedColumnDefinitionAsync(string column)
    {
        await using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(Ct);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT cc.definition
            FROM sys.computed_columns cc
            JOIN sys.tables t ON t.object_id = cc.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.name = 'pc_doc_doc' AND cc.name = @column
            """;
        command.Parameters.AddWithValue("@schema", theStore.Options.DatabaseSchemaName);
        command.Parameters.AddWithValue("@column", column);
        return (string?)await command.ExecuteScalarAsync(Ct);
    }

    [Fact]
    public async Task an_index_on_the_value_of_a_nullable_reads_the_member_itself()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Index(x => x.N!.Value));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = id, N = 5 });
            await session.SaveChangesAsync(Ct);
        }

        var definition = await ComputedColumnDefinitionAsync("cc_n");
        definition.ShouldNotBeNull();
        definition.ShouldContain("$.n");
        definition.ShouldNotContain("$.n.value");

        await using var query = theStore.QuerySession();
        var ids = await query.Query<Doc>().Where(x => x.N == 5).Select(x => x.Id).ToListAsync(Ct);
        ids.ShouldBe([id]);
    }

    [Fact]
    public async Task an_index_on_the_value_of_a_nested_nullable_reads_the_member_itself()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Index(x => x.Nested.Count!.Value));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(Ct);
        }

        var definition = await ComputedColumnDefinitionAsync("cc_nested_count");
        definition.ShouldNotBeNull();
        definition.ShouldContain("$.nested.count");
        definition.ShouldNotContain(".value");
    }

    [Fact]
    public async Task an_index_through_the_value_of_a_nullable_struct_skips_it()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Index(x => x.Cost!.Value.Amount));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(Ct);
        }

        var definition = await ComputedColumnDefinitionAsync("cc_cost_amount");
        definition.ShouldNotBeNull();
        definition.ShouldContain("$.cost.amount");
    }

    [Fact]
    public async Task a_composite_index_on_values_of_nullables_reads_the_members_themselves()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Index(x => new { Number = x.N!.Value, Count = x.Nested.Count!.Value }));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid() });
            await session.SaveChangesAsync(Ct);
        }

        (await ComputedColumnDefinitionAsync("cc_n")).ShouldNotBeNull().ShouldNotContain(".value");
        (await ComputedColumnDefinitionAsync("cc_nested_count")).ShouldNotBeNull().ShouldNotContain(".value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_patch_of_the_value_of_a_nullable_writes_the_member_itself(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = id, N = 5 });
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Doc>(id).Set(x => x.N!.Value, 6).Increment(x => x.N!.Value, 2);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Doc>(id, Ct))!.N.ShouldBe(8);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_patch_through_the_value_of_a_nullable_struct_skips_it(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = id, Cost = new Money { Amount = 1 } });
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Doc>(id).Set(x => x.Cost!.Value.Amount, 2m);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Doc>(id, Ct))!.Cost!.Value.Amount.ShouldBe(2m);
    }
}
