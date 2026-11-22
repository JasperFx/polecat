using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     #1128: the locked read an append's version check rests on (#1072) is about a thousand characters of
///     SQL, and building it on every append cost about 9 KB per save. Its text only depends on the schema,
///     the columns, the predicate, the id list and whether the tenant's ordinal is named, so it is built once
///     for each of those. Pure unit tests, no database.
/// </summary>
public class locked_stream_read_sql_is_built_once
{
    private const string Match = "id = @id AND tenant_id = @tenant_id";

    [Fact]
    public void the_same_read_is_the_same_text()
    {
        var graph = new StoreOptions().EventGraph;

        var first = graph.LockedStreamReadSql("version, is_archived", Match, StorageConstants.DefaultTenantId, new SqlCommand());
        var second = graph.LockedStreamReadSql("version, is_archived", Match, StorageConstants.DefaultTenantId, new SqlCommand());

        second.ShouldBeSameAs(first);
        first.ShouldContain("sp_getapplock");
    }

    [Fact]
    public void reading_it_again_allocates_nothing()
    {
        var graph = new StoreOptions().EventGraph;
        var command = new SqlCommand();
        graph.LockedStreamReadSql("version", Match, StorageConstants.DefaultTenantId, command);

        // A thousand calls, because the runtime can allocate a little once along the way (176 bytes on
        // .NET 10). Anything a call allocated would be at least 24 bytes each time.
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            graph.LockedStreamReadSql("version", Match, StorageConstants.DefaultTenantId, command);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        allocated.ShouldBeLessThan(1000);
    }

    [Fact]
    public void each_read_keeps_its_own_text()
    {
        var graph = new StoreOptions().EventGraph;
        const string ids = "SELECT CAST(value AS uniqueidentifier) AS id FROM OPENJSON(@ids)";

        var one = graph.LockedStreamReadSql("version", Match, StorageConstants.DefaultTenantId, new SqlCommand());
        var two = graph.LockedStreamReadSql("version, is_archived", Match, StorageConstants.DefaultTenantId, new SqlCommand());
        var many = graph.LockedStreamReadSql("s.id, s.version, s.is_archived", "s.tenant_id = @tenant_id",
            StorageConstants.DefaultTenantId, new SqlCommand(), ids);

        one.ShouldStartWith("DECLARE");
        one.ShouldEndWith($"SELECT version FROM {graph.StreamsTableName} WITH (UPDLOCK) WHERE {Match};");
        two.ShouldEndWith($"SELECT version, is_archived FROM {graph.StreamsTableName} WITH (UPDLOCK) WHERE {Match};");
        many.ShouldContain("OPENJSON(@ids)");
        many.ShouldNotBe(two);
    }

    [Fact]
    public void a_different_schema_is_a_different_text()
    {
        var options = new StoreOptions();
        var graph = options.EventGraph;

        var dbo = graph.LockedStreamReadSql("version", Match, StorageConstants.DefaultTenantId, new SqlCommand());

        options.DatabaseSchemaName = "elsewhere";
        var elsewhere = graph.LockedStreamReadSql("version", Match, StorageConstants.DefaultTenantId, new SqlCommand());

        elsewhere.ShouldNotBe(dbo);
        elsewhere.ShouldContain(graph.StreamsTableName);
        dbo.ShouldNotContain(graph.StreamsTableName);
    }
}
