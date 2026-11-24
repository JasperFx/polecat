using Polecat.Linq;
using Polecat.Pagination;

namespace Polecat.Tests.Linq.DocumentationSamples;

// Compile-only samples for docs/documents/querying/linq/*.md. Nothing here runs as a test; the behavior each
// sample shows is covered by the tests named next to it, in Polecat.LinqTests/Linq unless it says otherwise.

public class User
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = "";
    public string LastName { get; set; } = "";
    public string Email { get; set; } = "";
    public string? Nickname { get; set; }
    public string Code { get; set; } = "";
    public int Age { get; set; }
    public bool Active { get; set; }
    public bool Admin { get; set; }
    public bool Internal { get; set; }
    public int? Rank { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, int> Scores { get; set; } = new();
}

public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string Code { get; set; } = "";
    public string City { get; set; } = "";
    public string State { get; set; } = "";
    public int Number { get; set; }
    public int Level { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public decimal Total { get; set; }
    public decimal Discount { get; set; }
    public int[] Values { get; set; } = [];
    public int[][] Grid { get; set; } = [];
    public ProductPart[] Children { get; set; } = [];
}

public class ProductPart
{
    public string Name { get; set; } = "";
}

public class Address
{
    public string City { get; set; } = "";
}

public class Order
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Status { get; set; } = "";
    public Guid CustomerId { get; set; }
    public decimal Amount { get; set; }
    public Address ShippingAddress { get; set; } = new();
    public List<OrderLine> Lines { get; set; } = [];
}

public class OrderLine
{
    public string Sku { get; set; } = "";
    public int Quantity { get; set; }
}

public class Customer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
}

public class Post
{
    public Guid Id { get; set; }
    public List<string> Tags { get; set; } = [];
}

public class linq_documentation_samples
{
    // ---- docs/documents/querying/linq/index.md

    // Covered by basic_where_queries.cs, ordering_and_paging.cs and single_value_queries.cs
    public async Task basic_queries(IQuerySession session)
    {
        #region sample_polecat_linq_basic_queries
        // Simple filter
        var smiths = await session.Query<User>()
            .Where(x => x.LastName == "Smith")
            .ToListAsync();

        // With ordering
        var sorted = await session.Query<User>()
            .OrderBy(x => x.LastName)
            .ThenBy(x => x.FirstName)
            .ToListAsync();

        // First or default
        var first = await session.Query<User>()
            .FirstOrDefaultAsync(x => x.Email == "alice@example.com");

        // Aggregates
        var count = await session.Query<User>().CountAsync();
        var any = await session.Query<User>().AnyAsync(x => x.Internal);
        #endregion
    }

    // Covered by ordering_and_paging.cs and Pagination/paged_list_tests.cs
    public async Task paging(IQuerySession session)
    {
        #region sample_polecat_linq_paging
        var page = await session.Query<User>()
            .OrderBy(x => x.LastName)
            .Skip(20)
            .Take(10)
            .ToListAsync();

        // Or with the page count and total, in one call
        var pagedList = await session.Query<User>()
            .OrderBy(x => x.LastName)
            .ToPagedListAsync(pageNumber: 2, pageSize: 10);
        #endregion
    }

    // Covered by to_async_enumerable_tests.cs
    public async Task streaming(IQuerySession session, TextWriter writer, CancellationToken cancellationToken)
    {
        #region sample_polecat_linq_streaming_results
        await foreach (var user in session.Query<User>()
                           .Where(x => x.Internal)
                           .OrderBy(x => x.LastName)
                           .ToAsyncEnumerable(cancellationToken))
        {
            await writer.WriteLineAsync($"{user.LastName},{user.FirstName}");
        }
        #endregion
    }

    // Covered by additional_linq_operator_tests.cs (query_nullable_datetime_has_value_with_or) and
    // basic_where_queries.cs (where_with_closure_variable)
    public async Task optional_filter(IQuerySession session)
    {
        #region sample_polecat_linq_optional_filter
        int? excluded = null;

        var results = await session.Query<User>()
            .Where(x => !excluded.HasValue || x.Age != excluded.Value)
            .ToListAsync();
        #endregion
    }

    // Covered by join_operator_tests.cs (left_join_keeps_an_outer_row_with_no_match and the tests beside it)
    public async Task left_join(IQuerySession session)
    {
        #region sample_polecat_linq_left_join
        var rows = await session.Query<Customer>()
            .Where(c => c.City == "Seattle")
            .LeftJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId,
                (c, o) => new { c.Name, Amount = o == null ? (decimal?)null : o.Amount })
            .OrderBy(x => x.Name)
            .ToListAsync();
        #endregion
    }

    // Covered by group_join_tests.cs (inner_join_basic, left_join_includes_unmatched)
    public async Task group_join(IQuerySession session)
    {
        #region sample_polecat_linq_group_join
        // An inner join
        var inner = await session.Query<Customer>()
            .GroupJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
            .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
            .ToListAsync();

        // A left join: DefaultIfEmpty() keeps the customers with no orders
        var left = await session.Query<Customer>()
            .GroupJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
            .SelectMany(x => x.orders.DefaultIfEmpty(), (x, o) => new { x.c.Name, Amount = o == null ? 0m : o.Amount })
            .ToListAsync();
        #endregion
    }

    // Covered by order_by_after_projection_tests.cs and projected_member_tests.cs
    public async Task operators_after_select(IQuerySession session)
    {
        #region sample_polecat_linq_operators_after_select
        var cities = await session.Query<Order>()
            .Select(x => x.ShippingAddress)
            .Where(a => a.City != "Oslo")
            .OrderBy(a => a.City)
            .ToListAsync();

        var names = await session.Query<User>()
            .Select(x => new { Label = x.LastName })
            .Where(y => y.Label.StartsWith("Sm"))
            .OrderBy(y => y.Label)
            .ToListAsync();
        #endregion
    }

    // Covered by linq_extensions.cs (any_tenant_*, tenant_is_one_of_queries_specific_tenants)
    public async Task across_tenants(IDocumentStore store)
    {
        #region sample_polecat_linq_across_tenants
        await using var session = store.QuerySession(new SessionOptions { TenantId = "tenant-a" });

        // Every tenant's documents, not only tenant-a's
        var everyone = await session.Query<User>()
            .AnyTenant()
            .ToListAsync();

        // Only these tenants' documents
        var some = await session.Query<User>()
            .TenantIsOneOf("tenant-a", "tenant-c")
            .ToListAsync();
        #endregion
    }

    // ---- docs/documents/querying/linq/operators.md

    // Covered by basic_where_queries.cs
    public async Task comparisons(IQuerySession session)
    {
        #region sample_polecat_linq_comparison_operators
        var adults = await session.Query<User>()
            .Where(x => x.Age >= 18 && x.Age < 65)
            .ToListAsync();

        var notTwentyFive = await session.Query<User>()
            .Where(x => x.Age != 25)
            .ToListAsync();
        #endregion
    }

    // Covered by basic_where_queries.cs (where_boolean_*, where_and_condition, where_or_condition) and
    // additional_linq_operator_tests.cs (negating_predicate_*)
    public async Task boolean_logic(IQuerySession session)
    {
        #region sample_polecat_linq_boolean_logic
        var activeAdults = await session.Query<User>()
            .Where(x => x.Active && x.Age > 18)
            .ToListAsync();

        var activeOrAdmin = await session.Query<User>()
            .Where(x => x.Active || x.Admin)
            .ToListAsync();

        var inactive = await session.Query<User>()
            .Where(x => !x.Active)
            .ToListAsync();
        #endregion
    }

    // Covered by basic_where_queries.cs (where_null_check, where_not_null_check) and
    // additional_linq_operator_tests.cs (query_nullable_has_value and the tests beside it)
    public async Task null_checks(IQuerySession session)
    {
        #region sample_polecat_linq_null_checks
        var withNickname = await session.Query<User>()
            .Where(x => x.Nickname != null)
            .ToListAsync();

        var unranked = await session.Query<User>()
            .Where(x => !x.Rank.HasValue)
            .ToListAsync();
        #endregion
    }

    // Covered by additional_linq_operator_tests.cs (use_modulo) and computed_ordering_key_tests.cs
    public async Task arithmetic(IQuerySession session)
    {
        #region sample_polecat_linq_arithmetic
        var bigOrders = await session.Query<Product>()
            .Where(x => x.Price * x.Quantity > 100)
            .ToListAsync();

        var discounted = await session.Query<Product>()
            .Where(x => x.Total - x.Discount < 50)
            .ToListAsync();

        var everyThird = await session.Query<Product>()
            .Where(x => x.Number % 3 == 0)
            .ToListAsync();
        #endregion
    }

    // Covered by collection_querying.cs, large_in_list_tests.cs and linq_extensions.cs
    public async Task collection_operations(IQuerySession session, Guid id1, Guid id2, Guid id3)
    {
        #region sample_polecat_linq_contains_over_a_local_collection
        // A SQL IN over a local array, List<T> or HashSet<T>
        var ids = new[] { id1, id2, id3 };
        var chosen = await session.Query<User>()
            .Where(x => ids.Contains(x.Id))
            .ToListAsync();

        // The same, written the other way round
        var named = await session.Query<User>()
            .Where(x => x.LastName.IsOneOf("Smith", "Jones"))
            .ToListAsync();
        #endregion
    }

    // Covered by computed_ordering_key_tests.cs and document_array_index_tests.cs
    public async Task computed_ordering_keys(IQuerySession session)
    {
        #region sample_polecat_linq_computed_ordering_keys
        var byNegatedNumber = await session.Query<Product>()
            .OrderBy(x => -x.Number)
            .ToListAsync();

        var byRemainder = await session.Query<Product>()
            .OrderBy(x => x.Number % 3).ThenBy(x => x.Price - x.Number * 2m)
            .ToListAsync();

        var byText = await session.Query<Product>()
            .OrderBy(x => x.Code + x.Name).ThenBy(x => x.Name.ToUpper())
            .ToListAsync();

        var byLength = await session.Query<Product>()
            .OrderBy(x => x.Name.Length).ThenByDescending(x => x.Level.ToString())
            .ToListAsync();

        var byElement = await session.Query<Product>()
            .OrderBy(x => x.Values[1]).ThenBy(x => x.Children[0].Name)
            .ToListAsync();
        #endregion
    }

    // Covered by select_projections.cs and projected_single_value_tests.cs
    public async Task projections(IQuerySession session)
    {
        #region sample_polecat_linq_select
        var names = await session.Query<User>()
            .Select(x => new { x.FirstName, x.LastName })
            .ToListAsync();

        var emails = await session.Query<User>()
            .Select(x => x.Email)
            .ToListAsync();

        var firstEmail = await session.Query<User>()
            .Select(x => x.Email)
            .FirstOrDefaultAsync();
        #endregion
    }

    // Covered by distinct_projection_tests.cs and select_projections.cs (distinct_scalar)
    public async Task distinct(IQuerySession session)
    {
        #region sample_polecat_linq_distinct
        var cities = await session.Query<Product>()
            .Select(x => x.City).Distinct()
            .ToListAsync();

        var places = await session.Query<Product>()
            .Select(x => new { x.City, x.State }).Distinct().OrderBy(x => x.City)
            .ToListAsync();

        var howMany = await session.Query<Product>()
            .Select(x => new { x.City, x.State }).Distinct()
            .CountAsync();
        #endregion
    }

    // Covered by distinct_by_operator.cs
    public async Task distinct_by(IQuerySession session)
    {
        #region sample_polecat_linq_distinct_by
        // One whole document per city: the first in the ordering wins
        var cheapestInEachCity = await session.Query<Product>()
            .OrderBy(x => x.Price)
            .DistinctBy(x => x.City)
            .ToListAsync();

        // Or after a projection, on one of its members
        var oneNamePerCity = await session.Query<Product>()
            .Select(x => new { x.City, x.Name })
            .DistinctBy(x => x.City)
            .ToListAsync();
        #endregion
    }

    // Covered by single_value_queries.cs and additional_linq_operator_tests.cs (last_async_*)
    public async Task results_and_aggregates(IQuerySession session)
    {
        #region sample_polecat_linq_aggregates
        var count = await session.Query<User>().CountAsync();
        var big = await session.Query<User>().LongCountAsync();
        var anyActive = await session.Query<User>().AnyAsync(x => x.Active);

        var first = await session.Query<User>().OrderBy(x => x.Age).FirstAsync();
        var single = await session.Query<User>().Where(x => x.Email == "alice@example.com").SingleOrDefaultAsync();
        var oldest = await session.Query<User>().OrderBy(x => x.Age).LastAsync();

        var youngest = await session.Query<User>().MinAsync(x => x.Age);
        var eldest = await session.Query<User>().MaxAsync(x => x.Age);
        var totalAge = await session.Query<User>().SumAsync(x => x.Age);
        var averageAge = await session.Query<User>().AverageAsync(x => x.Age);
        #endregion
    }

    // Covered by group_by_operator.cs
    public async Task group_by_composite_key(IQuerySession session)
    {
        #region sample_polecat_linq_group_by_composite_key
        var results = await session.Query<LinqTarget>()
            .GroupBy(x => new { x.Color, x.Name })
            .Select(g => new { Color = g.Key.Color, Text = g.Key.Name, Count = g.Count() })
            .ToListAsync();
        #endregion
    }

    // Covered by group_by_operator.cs
    public async Task group_by_where_before(IQuerySession session)
    {
        #region sample_polecat_linq_group_by_where_before
        var results = await session.Query<LinqTarget>()
            .Where(x => x.Age > 20)
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();
        #endregion
    }

    // Covered by group_by_operator.cs and group_by_having_sql_injection.cs
    public async Task group_by_having(IQuerySession session)
    {
        #region sample_polecat_linq_group_by_having
        // The Where() after GroupBy() becomes a SQL HAVING clause
        var results = await session.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Where(g => g.Count() > 1)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();
        #endregion
    }

    // Covered by group_by_ordering_tests.cs
    public async Task group_by_ordering_and_paging(IQuerySession session)
    {
        #region sample_polecat_linq_group_by_ordering_and_paging
        var page = await session.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Total = g.Sum(x => x.Age) })
            .OrderByDescending(x => x.Total)
            .Skip(10).Take(10)
            .ToListAsync();
        #endregion
    }

    // Covered by group_by_operator.cs
    public async Task count_by_as_group_by(IQuerySession session)
    {
        #region sample_polecat_linq_count_by_as_group_by
        // Translated to SQL: SELECT color, COUNT(*) ... GROUP BY color
        var counts = await session.Query<LinqTarget>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Count = g.Count() })
            .ToListAsync();
        #endregion
    }

    // ---- docs/documents/querying/linq/strings.md

    // Covered by string_querying.cs (string_case_insensitive_equals) and linq_support_summary_tests.cs
    public async Task case_insensitive_equals(IQuerySession session)
    {
        #region sample_polecat_linq_case_insensitive_equals
        var results = await session.Query<User>()
            .Where(x => x.Email.Equals("Admin@Example.com", StringComparison.OrdinalIgnoreCase))
            .ToListAsync();
        #endregion
    }

    // ---- docs/documents/querying/linq/child-collections.md

    // Covered by document_array_index_tests.cs, static_member_predicate_tests.cs and linq_support_summary_tests.cs
    public async Task indexing(IQuerySession session)
    {
        #region sample_polecat_linq_indexing
        var byElement = await session.Query<Product>()
            .Where(x => x.Values[1] == 3)
            .ToListAsync();

        var byNestedElement = await session.Query<Product>()
            .Where(x => x.Grid[1][0] > 6 && x.Children[0].Name == "first")
            .ToListAsync();

        var orderedByElement = await session.Query<Product>()
            .OrderBy(x => x.Values.ElementAt(2))
            .ToListAsync();

        var byKey = await session.Query<User>()
            .Where(x => x.Scores["math"] > 80)
            .ToListAsync();
        #endregion
    }

    // Covered by select_many_over_document_collections_tests.cs
    public async Task select_many(IQuerySession session)
    {
        #region sample_polecat_linq_select_many
        var lines = await session.Query<Order>()
            .Where(x => x.Status == "open")
            .OrderBy(x => x.Number).Take(10)
            .SelectMany(x => x.Lines)
            .Where(l => l.Quantity > 1)
            .ToListAsync();

        var tags = await session.Query<Post>().SelectMany(x => x.Tags).Distinct().ToListAsync();

        var rows = await session.Query<Order>()
            .SelectMany(x => x.Lines, (x, l) => new { x.Number, l.Sku })
            .ToListAsync();
        #endregion
    }
}
