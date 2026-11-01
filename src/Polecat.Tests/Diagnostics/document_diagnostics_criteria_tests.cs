using JasperFx;
using JasperFx.Documents;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.TestUtils;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Diagnostics;

public class CriteriaTicket
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public int Severity { get; set; }
    public CriteriaOrderStatus Status { get; set; }
}

/// <summary>
///     jasperfx#869: what the shared criteria facts do not reach on Polecat — criteria together with the
///     metadata filters, with <see cref="DocumentQueryOptions.IncludeSoftDeleted" />, with a database per
///     tenant, across pages, and the refusals whose CRITERION matters.
/// </summary>
public class document_diagnostics_criteria_tests
{
    private static async Task DropTableAsync(string connectionString, string schema)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"IF OBJECT_ID('[{schema}].[pc_doc_criteriaticket]','U') IS NOT NULL DROP TABLE [{schema}].[pc_doc_criteriaticket];";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<DocumentStore> StoreAsync(string schema, Action<StoreOptions> configure)
    {
        await DropTableAsync(ConnectionSource.ConnectionString, schema);
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<CriteriaTicket>();
            configure(opts);
        });
    }

    private static Task<DocumentQueryResult> QueryAsync(DocumentStore store, DocumentQueryOptions options)
        => ((IDocumentStoreDiagnostics)store).QueryDocumentsAsync(typeof(CriteriaTicket).FullName!, options,
            TestContext.Current.CancellationToken);

    private static string[] TitlesOf(DocumentQueryResult result)
        => result.Documents.Select(x => System.Text.Json.JsonDocument.Parse(x.Json).RootElement
            .GetProperty("title").GetString()!).ToArray();

    [Fact]
    public async Task where_and_the_metadata_filters_both_narrow_the_page_and_the_total()
    {
        await using var store = await StoreAsync("gh869_meta", opts =>
            opts.Schema.For<CriteriaTicket>().Metadata(m =>
            {
                m.CorrelationId.Enabled = true;
                m.CausationId.Enabled = true;
                m.LastModifiedBy.Enabled = true;
            }));

        // severity 1..6; correlation alternates c0/c1; last-modified-by is b0 for the first three.
        for (var i = 1; i <= 6; i++)
        {
            await using var session = store.LightweightSession();
            session.CorrelationId = i % 2 == 0 ? "c0" : "c1";
            session.CausationId = "u0";
            session.LastModifiedBy = i <= 3 ? "b0" : "b1";
            session.Store(new CriteriaTicket { Id = Guid.NewGuid(), Title = $"t{i}", Severity = i });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var both = await QueryAsync(store, new DocumentQueryOptions(1, 10)
        {
            Where = "Severity > @0", Arguments = [1], OrderBy = "Severity desc", CorrelationId = "c0"
        });
        TitlesOf(both).ShouldBe(["t6", "t4", "t2"]);
        both.TotalCount.ShouldBe(3);

        var all = await QueryAsync(store, new DocumentQueryOptions(1, 10)
        {
            Where = "Severity > @0", Arguments = [1], CorrelationId = "c0", CausationId = "u0", LastModifiedBy = "b0"
        });
        TitlesOf(all).ShouldBe(["t2"]);
        all.TotalCount.ShouldBe(1);
    }

    [Fact]
    public async Task where_with_include_soft_deleted_returns_the_deleted_matches_flagged()
    {
        await using var store = await StoreAsync("gh869_soft",
            opts => opts.Policies.ForDocument<CriteriaTicket>(p => p.SoftDeleted = true));

        var tickets = Enumerable.Range(1, 4)
            .Select(i => new CriteriaTicket { Id = Guid.NewGuid(), Title = $"t{i}", Severity = i }).ToArray();
        await using (var session = store.LightweightSession())
        {
            session.Store(tickets);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            session.Delete<CriteriaTicket>(tickets[3].Id);
            session.Delete<CriteriaTicket>(tickets[0].Id);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var live = await QueryAsync(store, new DocumentQueryOptions(1, 10) { Where = "Severity >= @0", Arguments = [2] });
        TitlesOf(live).ShouldBe(["t2", "t3"], ignoreOrder: true);
        live.TotalCount.ShouldBe(2);

        var withDeleted = await QueryAsync(store, new DocumentQueryOptions(1, 10)
        {
            Where = "Severity >= @0", Arguments = [2], OrderBy = "Severity", IncludeSoftDeleted = true
        });
        TitlesOf(withDeleted).ShouldBe(["t2", "t3", "t4"]);
        withDeleted.Documents.Select(x => x.IsDeleted).ShouldBe([false, false, true]);
        withDeleted.TotalCount.ShouldBe(3);
    }

    [Fact]
    public async Task where_pages_through_the_matching_rows_with_a_stable_total()
    {
        await using var store = await StoreAsync("gh869_paging", _ => { });
        await using (var session = store.LightweightSession())
        {
            for (var i = 1; i <= 9; i++)
            {
                session.Store(new CriteriaTicket { Id = Guid.NewGuid(), Title = $"t{i}", Severity = i % 3 });
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Severity 1 or 2: six rows, ordered by severity with ties broken by id, two pages of four.
        var options = new DocumentQueryOptions(1, 4) { Where = "Severity > @0", Arguments = [0], OrderBy = "Severity desc" };
        var first = await QueryAsync(store, options);
        var second = await QueryAsync(store, options with { PageNumber = 2 });

        first.TotalCount.ShouldBe(6);
        second.TotalCount.ShouldBe(6);
        first.Documents.Count.ShouldBe(4);
        second.Documents.Count.ShouldBe(2);
        TitlesOf(first).Concat(TitlesOf(second)).ShouldBe(["t2", "t5", "t8", "t1", "t4", "t7"], ignoreOrder: true);
        TitlesOf(first).Take(3).ShouldBe(["t2", "t5", "t8"], ignoreOrder: true);
        (await QueryAsync(store, options)).Documents.Select(x => x.Id).ShouldBe(first.Documents.Select(x => x.Id));
    }

    [Fact]
    public async Task an_ordering_by_an_enum_stored_as_its_name_is_refused_as_the_ordering()
    {
        await using var store = await StoreAsync("gh869_enum_names",
            opts => opts.ConfigureSerialization(EnumStorage.AsString));

        var ex = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(
            () => QueryAsync(store, new DocumentQueryOptions(1, 10) { OrderBy = "Status desc" }));

        ex.Criterion.ShouldBe(nameof(DocumentQueryOptions.OrderBy));
        ex.Message.ShouldContain(DiagnosticsCriteria.EnumOrderingRefusal);

        // Equality on the same member still works: only order and range are refused.
        await using (var session = store.LightweightSession())
        {
            session.Store(
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "shipped", Status = CriteriaOrderStatus.Shipped },
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "pending", Status = CriteriaOrderStatus.Pending });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var equality = await QueryAsync(store, new DocumentQueryOptions(1, 10) { Where = "Status = \"Shipped\"" });
        TitlesOf(equality).ShouldBe(["shipped"]);
    }

    [Fact]
    public async Task an_enum_stored_as_an_integer_orders_by_value()
    {
        await using var store = await StoreAsync("gh869_enum_ints", _ => { });
        await using (var session = store.LightweightSession())
        {
            session.Store(
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "shipped", Status = CriteriaOrderStatus.Shipped },
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "cancelled", Status = CriteriaOrderStatus.Cancelled },
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "pending", Status = CriteriaOrderStatus.Pending });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var result = await QueryAsync(store, new DocumentQueryOptions(1, 10) { OrderBy = "Status desc" });

        TitlesOf(result).ShouldBe(["cancelled", "shipped", "pending"]);
    }

    [Fact]
    public async Task a_date_part_and_the_range_it_used_to_suggest_agree()
    {
        await using var store = await CriteriaShapes.CreateStoreAsync("gh869_dates", EnumStorage.AsInteger);
        await CriteriaShapes.SeedAsync(store);

        var part = await ((IDocumentStoreDiagnostics)store).QueryDocumentsAsync(typeof(CriteriaOrder).FullName!,
            new DocumentQueryOptions(1, 10) { Where = "PlacedAt.Year = 2025" }, TestContext.Current.CancellationToken);

        var range = await ((IDocumentStoreDiagnostics)store).QueryDocumentsAsync(typeof(CriteriaOrder).FullName!,
            new DocumentQueryOptions(1, 10)
            {
                Where = "PlacedAt >= @0 and PlacedAt < @1",
                Arguments = [new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)]
            }, TestContext.Current.CancellationToken);

        var expected = CriteriaShapes.Documents().Where(x => x.PlacedAt.Year == 2025).Select(x => x.Id.ToString());
        part.Documents.Select(x => x.Id).ShouldBe(expected, ignoreOrder: true);
        range.Documents.Select(x => x.Id).ShouldBe(expected, ignoreOrder: true);
    }

    [Fact]
    public async Task the_provider_translates_only_where_and_ordering_as_criteria()
    {
        await using var store = await StoreAsync("gh869_translate", _ => { });
        await using var session = store.QuerySession();
        var queryable = session.Query<CriteriaTicket>().Where(x => x.Severity > 1).Take(3);

        var provider = (PolecatLinqQueryProvider)queryable.Provider;
        Should.Throw<Polecat.Linq.BadLinqExpressionException>(() => provider.TranslateCriteria(queryable.Expression))
            .Message.ShouldContain("Take");

        var criteria = session.Query<CriteriaTicket>().Where(x => x.Severity > 1).OrderBy(x => x.Title);
        var statement = provider.TranslateCriteria(criteria.Expression);
        statement.Wheres.Count.ShouldBe(1);
        statement.OrderBys.Count.ShouldBe(1);
    }
}

/// <summary>
///     jasperfx#869 under database-per-tenant: the criteria run in the named tenant's OWN database, beside
///     the routing the diagnostics surface already did, and an all-tenants read is still refused.
/// </summary>
public class document_diagnostics_criteria_database_per_tenant : IAsyncLifetime
{
    private const string TenantA = "gh869_a";
    private const string TenantB = "gh869_b";
    private static readonly string DbA = ConnectionSource.Scoped("gh869_a");
    private static readonly string DbB = ConnectionSource.Scoped("gh869_b");

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task where_reads_only_the_named_tenants_database()
    {
        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(DbA);
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant(TenantA, ConnectionSource.ConnectionStringFor(DbA));
                tenancy.AddTenant(TenantB, ConnectionSource.ConnectionStringFor(DbB));
            });
        });

        await using (var a = store.LightweightSession(TenantA))
        {
            a.Store(new CriteriaTicket { Id = Guid.NewGuid(), Title = "a-low", Severity = 1 },
                new CriteriaTicket { Id = Guid.NewGuid(), Title = "a-high", Severity = 9 });
            await a.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var b = store.LightweightSession(TenantB))
        {
            b.Store(new CriteriaTicket { Id = Guid.NewGuid(), Title = "b-high", Severity = 8 });
            await b.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var diagnostics = (IDocumentStoreDiagnostics)store;
        var typeName = typeof(CriteriaTicket).FullName!;

        var inA = await diagnostics.QueryDocumentsAsync(typeName,
            new DocumentQueryOptions(1, 10) { TenantId = TenantA, Where = "Severity > @0", Arguments = [5] },
            TestContext.Current.CancellationToken);
        inA.TotalCount.ShouldBe(1);
        inA.Documents.Single().Json.ShouldContain("a-high");

        var inB = await diagnostics.QueryDocumentsAsync(typeName,
            new DocumentQueryOptions(1, 10) { TenantId = TenantB, Where = "Severity > @0", Arguments = [5] },
            TestContext.Current.CancellationToken);
        inB.TotalCount.ShouldBe(1);
        inB.Documents.Single().Json.ShouldContain("b-high");

        var everyTenant = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(() =>
            diagnostics.QueryDocumentsAsync(typeName,
                new DocumentQueryOptions(1, 10) { AllTenants = true, Where = "Severity > @0", Arguments = [5] },
                TestContext.Current.CancellationToken));
        everyTenant.Criterion.ShouldBe(nameof(DocumentQueryOptions.AllTenants));
    }
}
