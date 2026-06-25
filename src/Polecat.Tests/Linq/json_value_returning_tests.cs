using Polecat.Linq;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Linq;

/// <summary>
/// #217: on SQL Server 2025 native json storage, prefer JSON_VALUE(data, '$.x' RETURNING type)
/// over CAST(JSON_VALUE(data, '$.x') AS type). RETURNING is only valid against the native json
/// column type (UseNativeJsonType) and does not support uniqueidentifier, so Guid members and
/// nvarchar(max) storage keep CAST.
/// </summary>
public class json_value_returning_tests : OneOffConfigurationsContext
{
    public enum Status
    {
        Active,
        Inactive
    }

    public class Doc
    {
        public Guid Id { get; set; }
        public int Count { get; set; }
        public decimal Price { get; set; }
        public DateTimeOffset BucketEnd { get; set; }
        public Guid Ref { get; set; }
        public Status Status { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static string SqlFor<T>(IQuerySession session, IQueryable<T> query)
    {
        var provider = (PolecatLinqQueryProvider)query.Provider;
        return provider.BuildSql(query.Expression, session.TenantId);
    }

    [Fact]
    public async Task native_json_uses_returning_for_numeric()
    {
        ConfigureStore(_ => { }); // UseNativeJsonType defaults to true (native json on SQL 2025)
        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Doc>().Where(x => x.Count == 5));

        sql.ShouldContain("JSON_VALUE(data, '$.count' RETURNING int)");
        sql.ShouldNotContain("CAST(JSON_VALUE(data, '$.count')");
    }

    [Fact]
    public async Task native_json_uses_returning_for_decimal_and_datetimeoffset()
    {
        ConfigureStore(_ => { });
        var cutoff = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using var session = theStore.QuerySession();
        var sql = SqlFor(session,
            session.Query<Doc>().Where(x => x.Price > 10m && x.BucketEnd >= cutoff));

        sql.ShouldContain("JSON_VALUE(data, '$.price' RETURNING decimal(18,6))");
        sql.ShouldContain("JSON_VALUE(data, '$.bucketEnd' RETURNING datetimeoffset)");
    }

    [Fact]
    public async Task guid_member_keeps_cast_because_returning_has_no_uniqueidentifier()
    {
        ConfigureStore(_ => { });
        var someGuid = Guid.NewGuid();
        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Doc>().Where(x => x.Ref == someGuid));

        sql.ShouldContain("CAST(JSON_VALUE(data, '$.ref') AS uniqueidentifier)");
        sql.ShouldNotContain("RETURNING uniqueidentifier");
    }

    [Fact]
    public async Task enum_as_integer_uses_returning_int()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsInteger));
        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Doc>().Where(x => x.Status == Status.Inactive));

        sql.ShouldContain("JSON_VALUE(data, '$.status' RETURNING int)");
    }

    [Fact]
    public async Task nvarchar_storage_falls_back_to_cast()
    {
        // Without the native json column type, RETURNING is a syntax error — must use CAST.
        ConfigureStore(opts => opts.UseNativeJsonType = false);
        await using var session = theStore.QuerySession();
        var sql = SqlFor(session, session.Query<Doc>().Where(x => x.Count == 5));

        sql.ShouldContain("CAST(JSON_VALUE(data, '$.count') AS int)");
        // Note: the schema name itself contains "returning", so assert the clause, not the word.
        sql.ShouldNotContain("' RETURNING ");
    }

    [Fact]
    public async Task returning_query_returns_correct_rows_end_to_end()
    {
        ConfigureStore(_ => { });
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), Count = 1, Price = 5m, BucketEnd = t0, Status = Status.Active });
            session.Store(new Doc { Id = Guid.NewGuid(), Count = 7, Price = 50m, BucketEnd = t0.AddHours(3), Status = Status.Inactive });
            session.Store(new Doc { Id = Guid.NewGuid(), Count = 9, Price = 99m, BucketEnd = t0.AddHours(6), Status = Status.Inactive });
            await session.SaveChangesAsync();
        }

        await using var session2 = theStore.QuerySession();

        (await session2.Query<Doc>().Where(x => x.Count >= 7).ToListAsync()).Count.ShouldBe(2);
        (await session2.Query<Doc>().Where(x => x.Price > 10m).ToListAsync()).Count.ShouldBe(2);
        (await session2.Query<Doc>().Where(x => x.BucketEnd >= t0.AddHours(1)).ToListAsync()).Count.ShouldBe(2);
        (await session2.Query<Doc>().Where(x => x.Status == Status.Inactive).ToListAsync()).Count.ShouldBe(2);
    }
}
