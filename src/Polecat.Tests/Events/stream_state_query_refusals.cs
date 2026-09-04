using JasperFx;
using JasperFx.Events;
using Polecat.Linq;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
/// #534 (jasperfx#740): the two refusal shapes StreamStateQueryCompliance deliberately cannot pin,
/// because both enrolled stores translate every StreamState member and both have a tenant
/// dimension available. The contract's rule is the jasperfx#737 one: a predicate or scope the
/// store cannot honor must THROW naming it, never silently match all rows — an ignored clause
/// returns unfiltered streams that read as filtered.
/// </summary>
public class stream_state_query_refusals
{
    private const string Schema = "stream_query_refusals";

    private static async Task<DocumentStore> CreateStoreAsync()
    {
        await using (var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF OBJECT_ID('[{Schema}].[pc_events]','U') IS NOT NULL DROP TABLE [{Schema}].[pc_events];
                IF OBJECT_ID('[{Schema}].[pc_streams]','U') IS NOT NULL DROP TABLE [{Schema}].[pc_streams];
                """;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            // Deliberately NOT conjoined: the tenant-refusal fact depends on this store having no
            // tenant dimension.
        });

        // One stream, so a silently-ignored clause would have something to wrongly return.
        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new ShipmentNoted("crates"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return store;
    }

    public record ShipmentNoted(string Cargo);

    [Fact]
    public async Task a_member_the_provider_cannot_translate_is_refused_by_name()
    {
        await using var store = await CreateStoreAsync();

        var streams = ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates();

        // AggregateType.Name reaches THROUGH a translatable member into one Polecat has no column
        // for — the realistic near-miss of the supported typeof-equality form.
        var ex = await Should.ThrowAsync<NotSupportedException>(
            () => streams.Where(x => x.AggregateType!.Name == "Whatever")
                .ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("Name");
        ex.Message.ShouldContain(nameof(StreamState));
    }

    [Fact]
    public async Task a_tenant_scope_on_a_store_without_a_tenant_dimension_is_refused()
    {
        await using var store = await CreateStoreAsync();

        var readOnly = ((IEventStore)store).OpenReadOnlyEventStore();

        // Refused at the call, not at execution: unscoped rows must never come back dressed as a
        // tenant's.
        var ex = Should.Throw<NotSupportedException>(() => readOnly.QueryStreamStates("tenant-a"));

        ex.Message.ShouldContain("tenant");

        // And the null form still answers on the same store.
        var all = await readOnly.QueryStreamStates().ToListAsync(TestContext.Current.CancellationToken);
        all.ShouldHaveSingleItem();
    }
}
