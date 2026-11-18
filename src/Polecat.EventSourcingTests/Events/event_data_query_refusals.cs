using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Documents;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
/// jasperfx#1073: the shapes EventDataQueryCompliance deliberately does not pin for
/// <c>IReadOnlyEventStore.QueryEventData&lt;T&gt;()</c>, by the same jasperfx#737 rule as
/// <see cref="stream_state_query_refusals" />. A predicate or scope the store cannot honor must THROW
/// naming it, never silently match every event, and the tenant-scoped overload is honored where there
/// is a tenant dimension and refused where there is not.
/// </summary>
public class event_data_query_refusals
{
    private const string Schema = "event_data_query_refusals";
    private const string ConjoinedSchema = "event_data_query_conjoined";

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

    private static async Task<DocumentStore> CreateStoreAsync()
    {
        await DropEventTablesAsync(Schema);

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            // Deliberately NOT conjoined: the tenant-refusal fact depends on this store having no
            // tenant dimension.
        });

        // One event, so a silently-ignored clause would have something to wrongly return.
        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new CargoWeighed("crates", 12));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return store;
    }

    public record CargoWeighed(string Cargo, int Tonnes);

    [Fact]
    public async Task a_predicate_the_provider_cannot_translate_is_refused_by_name()
    {
        await using var store = await CreateStoreAsync();

        var weighings = ((IEventStore)store).OpenReadOnlyEventStore().QueryEventData<CargoWeighed>();

        var ex = await Should.ThrowAsync<Exception>(
            () => weighings.Where(x => x.Cargo.GetHashCode() == 5).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain(nameof(GetHashCode));
    }

    [Fact]
    public async Task a_tenant_scope_on_a_store_without_a_tenant_dimension_is_refused()
    {
        await using var store = await CreateStoreAsync();

        var readOnly = ((IEventStore)store).OpenReadOnlyEventStore();

        // Refused at the call, not at execution: unscoped rows must never come back dressed as a
        // tenant's.
        var ex = Should.Throw<NotSupportedException>(() => readOnly.QueryEventData<CargoWeighed>("tenant-a"));
        ex.Message.ShouldContain("tenant");

        // And the null form still answers on the same store.
        (await readOnly.QueryEventData<CargoWeighed>().ToListAsync(TestContext.Current.CancellationToken))
            .ShouldHaveSingleItem().Cargo.ShouldBe("crates");
    }

    [Fact]
    public async Task a_tenant_scope_on_a_conjoined_store_narrows_to_that_tenant()
    {
        await DropEventTablesAsync(ConjoinedSchema);

        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = ConjoinedSchema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        foreach (var (tenant, cargo) in new[] { ("tenant-a", "apples"), ("tenant-b", "bricks") })
        {
            await using var session = store.LightweightSession(tenant);
            session.Events.StartStream(Guid.NewGuid(), new CargoWeighed(cargo, 1));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var readOnly = ((IEventStore)store).OpenReadOnlyEventStore();

        (await readOnly.QueryEventData<CargoWeighed>("tenant-a").ToListAsync(TestContext.Current.CancellationToken))
            .ShouldHaveSingleItem().Cargo.ShouldBe("apples");

        (await readOnly.QueryEventData<CargoWeighed>("tenant-b").CountAsync(TestContext.Current.CancellationToken))
            .ShouldBe(1);
    }
}
