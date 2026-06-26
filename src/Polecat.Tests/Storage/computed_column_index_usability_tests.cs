using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// #223: Index(...) computed-column indexes must be usable by the LINQ translator, and a
/// DateTimeOffset member must be indexable at all.
///
/// Before this fix:
///   - the translator emitted a different expression per member type than the index column used
///     (bare JSON_VALUE for strings, CAST(... AS datetimeoffset) for dates), so the persisted
///     computed column never matched the predicate and the index was dead weight; and
///   - a DateTimeOffset index failed at schema-apply ("computed column ... cannot be persisted
///     because the column is non-deterministic") because CAST(string AS datetimeoffset) is
///     non-deterministic.
///
/// Now a single shared expression builder feeds both the index DDL and the translator, dates use a
/// deterministic CONVERT(..., 126) (persistable), and each column is typed from its CLR member type.
/// </summary>
public class computed_column_index_usability_tests : OneOffConfigurationsContext
{
    public class IndexedMetric
    {
        public Guid Id { get; set; }
        public string ServiceName { get; set; } = string.Empty;
        public DateTimeOffset BucketEnd { get; set; }
        public int Count { get; set; }
    }

    private const string Table = "pc_doc_indexedmetric";

    private static string SqlFor<T>(IQuerySession session, IQueryable<T> query)
    {
        var provider = (PolecatLinqQueryProvider)query.Provider;
        return provider.BuildSql(query.Expression, session.TenantId);
    }

    // ---- unit: the shared expression builder ----------------------------------------------------

    [Fact]
    public void datetimeoffset_uses_deterministic_convert_so_it_can_be_persisted()
    {
        // Date/time stays on CONVERT(..., 126) regardless of native json: a CAST of a string to a
        // date/time type is non-deterministic and cannot be PERSISTED, and #236's RETURNING swap
        // only applies to the non-date branch.
        DocumentIndex.ComputedColumnExpression("$.bucketEnd", "datetimeoffset", IndexCasing.Default, useReturning: true)
            .ShouldBe("CONVERT(datetimeoffset, JSON_VALUE(data, '$.bucketEnd'), 126)");
        DocumentIndex.ComputedColumnExpression("$.bucketEnd", "datetimeoffset", IndexCasing.Default, useReturning: false)
            .ShouldBe("CONVERT(datetimeoffset, JSON_VALUE(data, '$.bucketEnd'), 126)");
    }

    [Fact]
    public void string_uses_returning_on_native_json_and_cast_otherwise()
    {
        // #236: on native json storage the persisted computed column uses JSON_VALUE(... RETURNING),
        // matching the #217 query-side form, so the index stays seekable.
        DocumentIndex.ComputedColumnExpression("$.serviceName", "varchar(250)", IndexCasing.Default, useReturning: true)
            .ShouldBe("JSON_VALUE(data, '$.serviceName' RETURNING varchar(250))");

        // On nvarchar(max) storage RETURNING is a syntax error, so fall back to CAST.
        DocumentIndex.ComputedColumnExpression("$.serviceName", "varchar(250)", IndexCasing.Default, useReturning: false)
            .ShouldBe("CAST(JSON_VALUE(data, '$.serviceName') AS varchar(250))");
    }

    [Fact]
    public void uniqueidentifier_keeps_cast_even_on_native_json()
    {
        // RETURNING has no uniqueidentifier type, so Guid-typed computed columns (e.g. FK columns)
        // always fall back to CAST.
        DocumentIndex.ComputedColumnExpression("$.userId", "uniqueidentifier", IndexCasing.Default, useReturning: true)
            .ShouldBe("CAST(JSON_VALUE(data, '$.userId') AS uniqueidentifier)");
    }

    [Fact]
    public void resolve_sql_type_precedence_per_path_then_index_then_clr()
    {
        var index = new DocumentIndex(["$.bucketEnd"]);
        // CLR-derived when nothing set
        index.ResolveSqlType("$.bucketEnd", typeof(DateTimeOffset)).ShouldBe("datetimeoffset");
        index.ResolveSqlType("$.count", typeof(int)).ShouldBe("int");
        index.ResolveSqlType("$.serviceName", typeof(string)).ShouldBe("varchar(250)");

        // index-wide override wins over CLR
        index.SqlType = "varchar(500)";
        index.ResolveSqlType("$.bucketEnd", typeof(DateTimeOffset)).ShouldBe("varchar(500)");

        // per-path override wins over everything
        index.SqlTypeByPath["$.bucketEnd"] = "datetime2";
        index.ResolveSqlType("$.bucketEnd", typeof(DateTimeOffset)).ShouldBe("datetime2");
    }

    // ---- gap 2: DateTimeOffset index can be created + persisted ----------------------------------

    [Fact]
    public async Task datetimeoffset_index_applies_cleanly_and_is_persisted()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<IndexedMetric>().Index(x => x.BucketEnd);
        });

        // Storing a doc triggers AutoCreate of the table + persisted computed column + index.
        // Previously this threw: "computed column 'cc_bucketend' ... cannot be persisted because
        // the column is non-deterministic." Must now succeed.
        await Should.NotThrowAsync(async () =>
        {
            await using var session = theStore.LightweightSession();
            session.Store(new IndexedMetric
            {
                Id = Guid.NewGuid(), ServiceName = "svc", BucketEnd = DateTimeOffset.UtcNow, Count = 1
            });
            await session.SaveChangesAsync();
        });

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT cc.is_persisted, TYPE_NAME(cc.user_type_id)
            FROM sys.computed_columns cc
            WHERE cc.object_id = OBJECT_ID('[{_schema()}].[{Table}]') AND cc.name = 'cc_bucketend';
            """;
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();
        reader.GetBoolean(0).ShouldBeTrue();       // PERSISTED
        reader.GetString(1).ShouldBe("datetimeoffset");
    }

    [Fact]
    public async Task int_index_column_uses_returning_definition_on_native_json()
    {
        // #236: the persisted computed column for a non-date member should use
        // JSON_VALUE(... RETURNING type) on native json storage rather than CAST.
        ConfigureStore(opts => opts.Schema.For<IndexedMetric>().Index(x => x.Count));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedMetric
            {
                Id = Guid.NewGuid(), ServiceName = "svc", BucketEnd = DateTimeOffset.UtcNow, Count = 7
            });
            await session.SaveChangesAsync();
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT cc.is_persisted, cc.definition
            FROM sys.computed_columns cc
            WHERE cc.object_id = OBJECT_ID('[{_schema()}].[{Table}]') AND cc.name = 'cc_count';
            """;
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();
        reader.GetBoolean(0).ShouldBeTrue(); // PERSISTED
        var definition = reader.GetString(1);
        if (ConnectionSource.SupportsNativeJson)
        {
            definition.ShouldContain("RETURNING");
        }
        else
        {
            definition.ShouldNotContain("RETURNING"); // CAST fallback on nvarchar(max) storage
        }
    }

    // ---- gap 1: the translator targets the computed column --------------------------------------

    [Fact]
    public async Task string_equality_predicate_targets_the_computed_column()
    {
        ConfigureStore(opts => opts.Schema.For<IndexedMetric>().Index(x => x.ServiceName));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.QuerySession();
        var query = session.Query<IndexedMetric>().Where(x => x.ServiceName == "svc-A");

        // Bare JSON_VALUE (the old behavior) could never match the varchar(250) computed column.
        // The predicate must use the EXACT same expression as the persisted column — RETURNING on
        // native json (#236), CAST on nvarchar(max) — so the index is seekable on either.
        var expected = DocumentIndex.ComputedColumnExpression(
            "$.serviceName", "varchar(250)", IndexCasing.Default, ConnectionSource.SupportsNativeJson);
        SqlFor(session, query).ShouldContain(expected);
    }

    [Fact]
    public async Task datetimeoffset_range_predicate_targets_the_computed_column()
    {
        ConfigureStore(opts => opts.Schema.For<IndexedMetric>().Index(x => x.BucketEnd));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var cutoff = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using var session = theStore.QuerySession();
        var query = session.Query<IndexedMetric>().Where(x => x.BucketEnd >= cutoff);

        SqlFor(session, query)
            .ShouldContain("CONVERT(datetimeoffset, JSON_VALUE(data, '$.bucketEnd'), 126)");
    }

    [Fact]
    public async Task non_indexed_member_is_not_rewritten_to_the_index_column()
    {
        // Only indexed members are rewritten to the computed-column expression; an unrelated member
        // keeps its normal typed locator. On native json storage (#217) that locator is the
        // RETURNING form; the point of this test is that it is NOT a computed-column (cc_) reference.
        ConfigureStore(opts => opts.Schema.For<IndexedMetric>().Index(x => x.ServiceName));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.QuerySession();
        var query = session.Query<IndexedMetric>().Where(x => x.Count == 5);

        var sql = SqlFor(session, query);
        // Form-agnostic: this test applies schema (so it runs on servers without native json too,
        // e.g. Azure SQL Edge → CAST instead of #217 RETURNING). The point is that the member is
        // referenced by its raw JSON path and NOT rewritten to the index's computed column (cc_).
        sql.ShouldContain("JSON_VALUE(data, '$.count'");
        sql.ShouldNotContain("cc_");
    }

    // ---- gap 3: composite index types each column independently ----------------------------------

    [Fact]
    public async Task composite_index_types_string_and_date_columns_independently()
    {
        ConfigureStore(opts =>
            opts.Schema.For<IndexedMetric>().Index(x => new { x.ServiceName, x.BucketEnd }));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedMetric
            {
                Id = Guid.NewGuid(), ServiceName = "svc", BucketEnd = DateTimeOffset.UtcNow, Count = 1
            });
            await session.SaveChangesAsync();
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT cc.name, TYPE_NAME(cc.user_type_id)
            FROM sys.computed_columns cc
            WHERE cc.object_id = OBJECT_ID('[{_schema()}].[{Table}]')
            ORDER BY cc.name;
            """;
        var types = new Dictionary<string, string>();
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync()) types[reader.GetString(0)] = reader.GetString(1);
        }

        types["cc_servicename"].ShouldBe("varchar");
        types["cc_bucketend"].ShouldBe("datetimeoffset");
    }

    // ---- end-to-end: the issue's MetricsSample scenario -----------------------------------------

    [Fact]
    public async Task end_to_end_query_and_delete_over_string_and_datetimeoffset()
    {
        ConfigureStore(opts =>
            opts.Schema.For<IndexedMetric>().Index(x => new { x.ServiceName, x.BucketEnd }));

        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedMetric { Id = Guid.NewGuid(), ServiceName = "svc-A", BucketEnd = t0, Count = 1 });
            session.Store(new IndexedMetric { Id = Guid.NewGuid(), ServiceName = "svc-A", BucketEnd = t0.AddHours(2), Count = 2 });
            session.Store(new IndexedMetric { Id = Guid.NewGuid(), ServiceName = "svc-A", BucketEnd = t0.AddHours(5), Count = 3 });
            session.Store(new IndexedMetric { Id = Guid.NewGuid(), ServiceName = "svc-B", BucketEnd = t0.AddHours(5), Count = 9 });
            await session.SaveChangesAsync();
        }

        var cutoff = t0.AddHours(1);
        await using (var session = theStore.QuerySession())
        {
            // WHERE ServiceName = 'svc-A' AND BucketEnd >= cutoff
            var rows = await session.Query<IndexedMetric>()
                .Where(x => x.ServiceName == "svc-A" && x.BucketEnd >= cutoff)
                .ToListAsync();
            rows.Count.ShouldBe(2);
            rows.ShouldAllBe(x => x.ServiceName == "svc-A" && x.BucketEnd >= cutoff);
        }

        // DeleteWhere(x => x.BucketEnd < cutoff) — the pruning path from the issue.
        await using (var session = theStore.LightweightSession())
        {
            session.DeleteWhere<IndexedMetric>(x => x.BucketEnd < cutoff);
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.QuerySession())
        {
            var remaining = await session.Query<IndexedMetric>().ToListAsync();
            remaining.Count.ShouldBe(3); // the single BucketEnd == t0 row was pruned
            remaining.ShouldAllBe(x => x.BucketEnd >= cutoff);
        }
    }

    private string _schema() => GetType().Name.ToLowerInvariant();
}
