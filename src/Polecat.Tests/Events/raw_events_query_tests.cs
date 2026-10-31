using System.Linq.Expressions;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Documents;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
/// jasperfx#1094 / #827: <c>IReadOnlyEventStore.QueryRawEvents()</c>, the store-agnostic
/// <c>IQueryable&lt;IEvent&gt;</c>. The Where and OrderBy below are written against <see cref="IEvent" />
/// only, the same expressions the Marten and Fisher tests use over the same seeded events, so each
/// store is held to the same rows in the same order. A member the provider cannot translate must fail
/// by name; a tenant it cannot honor must be refused, never ignored.
/// </summary>
public class raw_events_query_tests
{
    private const string Schema = "raw_events_query";
    private const string ConjoinedSchema = "raw_events_query_conjoined";

    public record Shipped(string Cargo);
    public record Delivered(string Cargo);

    private static async Task DropEventTablesAsync(string schema)
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('[{schema}].[pc_events]','U') IS NOT NULL DROP TABLE [{schema}].[pc_events];
            IF OBJECT_ID('[{schema}].[pc_streams]','U') IS NOT NULL DROP TABLE [{schema}].[pc_streams];
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DocumentStore Build(string schema, bool conjoined)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            if (conjoined) opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
    }

    private static async Task<(DocumentStore store, Guid a, Guid b)> SeedAsync()
    {
        await DropEventTablesAsync(Schema);
        var store = Build(Schema, false);
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.CorrelationId = "corr-1";
        session.CausationId = "cause-1";
        session.Events.StartStream(a, new Shipped("apples"), new Delivered("apples"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var second = store.LightweightSession();
        second.CorrelationId = "corr-2";
        second.CausationId = "cause-2";
        second.Events.StartStream(b, new Shipped("bricks"));
        await second.SaveChangesAsync(TestContext.Current.CancellationToken);

        return (store, a, b);
    }

    private static IReadOnlyEventStore Reader(DocumentStore store) =>
        ((IEventStore)store).OpenReadOnlyEventStore();

    [Fact]
    public async Task filters_and_orders_by_event_metadata()
    {
        var (store, a, b) = await SeedAsync();
        await using var _ = store;
        var token = TestContext.Current.CancellationToken;
        var reader = Reader(store);

        var all = await reader.QueryRawEvents().OrderBy(e => e.Sequence).ToListAsync(token);
        all.Count.ShouldBe(3);
        all.Select(e => e.Sequence).ShouldBe(all.Select(e => e.Sequence).OrderBy(x => x));

        var shipped = await reader.QueryRawEvents()
            .Where(e => e.EventTypeName == "shipped")
            .OrderByDescending(e => e.Sequence).ToListAsync(token);
        shipped.Select(e => e.StreamId).ShouldBe([b, a]);

        (await reader.QueryRawEvents().Where(e => e.StreamId == a).CountAsync(token)).ShouldBe(2);
        (await reader.QueryRawEvents().Where(e => e.CorrelationId == "corr-2").CountAsync(token)).ShouldBe(1);
        (await reader.QueryRawEvents().Where(e => e.CausationId == "cause-1").CountAsync(token)).ShouldBe(2);
        (await reader.QueryRawEvents().Where(e => e.Sequence > all[0].Sequence).CountAsync(token)).ShouldBe(2);

        var cutoff = all[1].Timestamp;
        (await reader.QueryRawEvents().Where(e => e.Timestamp >= cutoff).CountAsync(token))
            .ShouldBeGreaterThanOrEqualTo(2);
        all.ShouldAllBe(e => e.TenantId == StorageConstants.DefaultTenantId);
    }

    [Fact]
    public async Task a_member_the_provider_cannot_translate_is_refused_by_name()
    {
        var (store, _, _) = await SeedAsync();
        await using var _ = store;

        var ex = await Should.ThrowAsync<Exception>(() => Reader(store).QueryRawEvents()
            .Where(e => e.Data.GetHashCode() == 5).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("Data");
    }

    [Fact]
    public async Task a_tenant_scope_on_a_store_without_a_tenant_dimension_is_refused()
    {
        var (store, _, _) = await SeedAsync();
        await using var _ = store;

        var ex = Should.Throw<NotSupportedException>(() => Reader(store).QueryRawEvents("tenant-a"));
        ex.Message.ShouldContain("tenant");
    }

    [Fact]
    public async Task a_tenant_scope_on_a_conjoined_store_narrows_to_that_tenant()
    {
        await DropEventTablesAsync(ConjoinedSchema);
        await using var store = Build(ConjoinedSchema, true);

        foreach (var (tenant, cargo) in new[] { ("tenant-a", "apples"), ("tenant-b", "bricks") })
        {
            await using var session = store.LightweightSession(tenant);
            session.Events.StartStream(Guid.NewGuid(), new Shipped(cargo));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var reader = Reader(store);
        var rows = await reader.QueryRawEvents("tenant-a").ToListAsync(TestContext.Current.CancellationToken);
        var row = rows.ShouldHaveSingleItem();
        row.TenantId.ShouldBe("tenant-a");
    }
}
