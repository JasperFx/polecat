using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// Nested-member support for Index(...) expressions. Previously x => x.Address.City silently
/// resolved to "$.city" (only the leaf member name), which never matched the query translator's
/// "$.address.city" path — so the index was unusable AND pointed at the wrong JSON path. Index
/// path resolution now walks the full member chain, identical to MemberFactory.BuildJsonPath.
/// </summary>
public class nested_member_index_tests : OneOffConfigurationsContext
{
    public class Address
    {
        public string City { get; set; } = string.Empty;
        public string PostalCode { get; set; } = string.Empty;
    }

    public class Customer
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Address Address { get; set; } = new();
    }

    private const string Table = "pc_doc_customer";
    private string Schema => GetType().Name.ToLowerInvariant();

    private static string SqlFor<T>(IQuerySession session, IQueryable<T> query)
    {
        var provider = (PolecatLinqQueryProvider)query.Provider;
        return provider.BuildSql(query.Expression, session.TenantId);
    }

    // ---- unit: path resolution ------------------------------------------------------------------

    [Fact]
    public void resolve_json_paths_walks_a_nested_member()
    {
        DocumentIndex.ResolveJsonPaths<Customer>(x => x.Address.City)
            .ShouldBe(["$.address.city"]);
    }

    [Fact]
    public void resolve_json_paths_handles_composite_with_nested_members()
    {
        DocumentIndex.ResolveJsonPaths<Customer>(x => new { x.Name, x.Address.City })
            .ShouldBe(["$.name", "$.address.city"]);
    }

    // ---- integration: computed column + index path alignment ------------------------------------

    [Fact]
    public async Task nested_member_index_creates_the_expected_computed_column()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Address.City));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "A", Address = new Address { City = "Austin" } });
            await session.SaveChangesAsync();
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*) FROM sys.computed_columns
            WHERE object_id = OBJECT_ID('[{Schema}].[{Table}]') AND name = 'cc_address_city';
            """;
        ((int)(await cmd.ExecuteScalarAsync())!).ShouldBe(1);
    }

    [Fact]
    public async Task nested_member_query_targets_the_index_path_not_the_leaf()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Address.City));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.QuerySession();
        var query = session.Query<Customer>().Where(x => x.Address.City == "Austin");
        var sql = SqlFor(session, query);

        // The index computed column is over '$.address.city'; the predicate must target the same
        // path (and be rewritten to the computed-column expression so the index is seekable).
        // Form-agnostic: the indexed locator is JSON_VALUE(... '$.address.city' RETURNING type) on
        // native json (#236) or CAST(JSON_VALUE(... '$.address.city') AS type) on nvarchar(max), so
        // assert the path occurrence without the trailing ')'.
        sql.ShouldContain("JSON_VALUE(data, '$.address.city'");
        sql.ShouldNotContain("'$.city'"); // the old, wrong leaf-only path
    }

    [Fact]
    public async Task end_to_end_nested_member_query_returns_correct_rows()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Address.City));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "A", Address = new Address { City = "Austin" } });
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "B", Address = new Address { City = "Austin" } });
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "C", Address = new Address { City = "Dallas" } });
            await session.SaveChangesAsync();
        }

        await using var session2 = theStore.QuerySession();
        var austin = await session2.Query<Customer>()
            .Where(x => x.Address.City == "Austin")
            .ToListAsync();

        austin.Count.ShouldBe(2);
        austin.ShouldAllBe(c => c.Address.City == "Austin");
    }
}
