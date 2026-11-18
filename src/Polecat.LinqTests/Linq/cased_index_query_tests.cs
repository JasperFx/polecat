using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #1030: a <c>Casing = Lower</c> or <c>Upper</c> index is used by <c>x.Member.ToLower() == value</c>
///     and <c>x.Member.ToUpper() == value</c>. Before, the comparison folded the full read of the document,
///     never matched the computed column, and scanned, so a cased index only enforced uniqueness.
/// </summary>
public class cased_index_query_tests : OneOffConfigurationsContext
{
    public class Customer
    {
        public Guid Id { get; set; }
        public string Email { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    private const string Table = "pc_doc_customer";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string SqlFor<T>(IQuerySession session, IQueryable<T> query)
    {
        var provider = (PolecatLinqQueryProvider)query.Provider;
        return provider.BuildSql(query.Expression, session.TenantId);
    }

    private static string Column(string path, IndexCasing casing, string sqlType = "varchar(250)")
        => DocumentIndex.ComputedColumnExpression(path, sqlType, casing, ConnectionSource.SupportsNativeJson);

    private async Task<IQuerySession> seededAsync(Action<StoreOptions> configure, bool? nativeJson = null)
    {
        if (nativeJson == true)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            if (nativeJson.HasValue) opts.UseNativeJsonType = nativeJson.Value;
            configure(opts);
        });

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < 2000; i++)
            {
                session.Store(new Customer
                {
                    Id = Guid.NewGuid(), Email = $"Customer{i}@Example.com", Region = $"Region-{i % 7}",
                    Name = $"n{i}"
                });
            }

            session.Store(new Customer { Id = Guid.NewGuid(), Email = "TÖKYÖ@example.com", Name = "tokyo" });
            session.Store(new Customer { Id = Guid.NewGuid(), Email = new string('A', 300), Name = "long" });
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    private static void shouldSeek(IReadOnlyList<PlanAccess> accesses)
    {
        accesses.ShouldContain(x => x.Operator.EndsWith("Index Seek") && x.Table == Table);
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan") && x.Table == Table);
    }

    [Fact]
    public async Task to_lower_targets_a_lower_casing_index_column()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower);
        });

        await using var session = theStore.QuerySession();

        // Compared with the cc_email_lower column, so the index is sought
        var query = session.Query<Customer>()
            .Where(x => x.Email.ToLower() == "customer42@example.com");

        SqlFor(session, query).ShouldContain($"{Column("$.email", IndexCasing.Lower)} = @");
    }

    [Fact]
    public async Task to_upper_targets_an_upper_casing_index_column()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Upper));

        await using var session = theStore.QuerySession();
        var query = session.Query<Customer>().Where(x => x.Email.ToUpperInvariant() == "CUSTOMER42@EXAMPLE.COM");

        SqlFor(session, query).ShouldContain($"{Column("$.email", IndexCasing.Upper)} = @");
    }

    [Fact]
    public async Task the_value_on_the_left_targets_the_column_too()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower));

        await using var session = theStore.QuerySession();
        var query = session.Query<Customer>().Where(x => "customer42@example.com" == x.Email.ToLowerInvariant());

        SqlFor(session, query).ShouldContain($"{Column("$.email", IndexCasing.Lower)} = @");
    }

    [Fact]
    public async Task a_composite_cased_index_covers_each_of_its_members()
    {
        ConfigureStore(opts =>
            opts.Schema.For<Customer>().Index(x => new { x.Region, x.Email }, x => x.Casing = IndexCasing.Upper));

        await using var session = theStore.QuerySession();
        var query = session.Query<Customer>().Where(x => x.Region.ToUpper() == "REGION-3");

        SqlFor(session, query).ShouldContain($"{Column("$.region", IndexCasing.Upper)} = @");
    }

    [Fact]
    public async Task a_declared_sql_type_is_the_column_compared_with()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email, x =>
        {
            x.Casing = IndexCasing.Lower;
            x.SqlType = "nvarchar(320)";
        }));

        await using var session = theStore.QuerySession();
        var query = session.Query<Customer>().Where(x => x.Email.ToLower() == "customer42@example.com");

        SqlFor(session, query).ShouldContain($"{Column("$.email", IndexCasing.Lower, "nvarchar(320)")} = @");
    }

    [Fact]
    public async Task the_other_casing_is_not_rewritten_onto_the_column()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower));

        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Customer>().Where(x => x.Email.ToUpper() == "CUSTOMER42@EXAMPLE.COM"));

        sql.ShouldNotContain(Column("$.email", IndexCasing.Lower));
        sql.ShouldNotContain(Column("$.email", IndexCasing.Upper));
    }

    [Fact]
    public async Task a_default_index_is_not_used_for_a_case_fold()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email));

        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Customer>().Where(x => x.Email.ToLower() == "customer42@example.com"));

        sql.ShouldNotContain(Column("$.email", IndexCasing.Lower));
        sql.ShouldNotContain($"LOWER({Column("$.email", IndexCasing.Default)})");
    }

    [Fact]
    public async Task an_unindexed_member_keeps_the_document_read()
    {
        ConfigureStore(opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower));

        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Customer>().Where(x => x.Name.ToLower() == "n42"));

        sql.ShouldNotContain("cc_");
        sql.ShouldNotContain(Column("$.name", IndexCasing.Lower));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task to_lower_seeks_a_lower_casing_index(bool nativeJson)
    {
        await using var query = await seededAsync(
            opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower), nativeJson);

        var queryable = query.Query<Customer>().Where(x => x.Email.ToLower() == "customer42@example.com");

        (await queryable.ToListAsync(Ct)).Single().Name.ShouldBe("n42");
        shouldSeek(await QueryPlanProbe.AccessesAsync(query, queryable, Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task to_upper_seeks_an_upper_casing_index(bool nativeJson)
    {
        await using var query = await seededAsync(
            opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Upper), nativeJson);

        var queryable = query.Query<Customer>().Where(x => x.Email.ToUpper() == "CUSTOMER42@EXAMPLE.COM");

        (await queryable.ToListAsync(Ct)).Single().Name.ShouldBe("n42");
        shouldSeek(await QueryPlanProbe.AccessesAsync(query, queryable, Ct));
    }

    [Fact]
    public async Task the_answers_are_the_same_as_reading_the_document()
    {
        await using var query = await seededAsync(
            opts => opts.Schema.For<Customer>().Index(x => x.Email, x => x.Casing = IndexCasing.Lower));

        // Outside the varchar column's code page, and wider than the column: both go to the document
        (await query.Query<Customer>().Where(x => x.Email.ToLower() == "tökyö@example.com").ToListAsync(Ct))
            .Single().Name.ShouldBe("tokyo");
        (await query.Query<Customer>().Where(x => x.Email.ToLower() == new string('a', 300)).ToListAsync(Ct))
            .Single().Name.ShouldBe("long");

        // The column holds the first 250 characters, and that is not a match for the whole value
        (await query.Query<Customer>().Where(x => x.Email.ToLower() == new string('a', 250)).ToListAsync(Ct))
            .ShouldBeEmpty();

        (await query.Query<Customer>().Where(x => x.Email.ToLower() == "nobody@example.com").ToListAsync(Ct))
            .ShouldBeEmpty();

        (await query.Query<Customer>().CountAsync(x => x.Email.ToLower() != "customer42@example.com", Ct))
            .ShouldBe(2001);
        (await query.Query<Customer>().CountAsync(x => !(x.Email.ToLower() == "customer42@example.com"), Ct))
            .ShouldBe(2001);
        (await query.Query<Customer>().CountAsync(x => x.Email.ToLower() == null, Ct)).ShouldBe(0);
    }
}
