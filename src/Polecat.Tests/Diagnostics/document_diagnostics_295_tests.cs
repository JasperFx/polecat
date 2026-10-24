using System.Text.Json.Nodes;
using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Core.Reflection;
using JasperFx.Documents;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Weasel.Core;

namespace Polecat.Tests.Diagnostics;

public class ExplainTicket
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public int Severity { get; set; }
}

public enum LedgerState
{
    Open,
    Closed
}

public record LedgerOpened(string Owner);
public record LedgerCredited(decimal Amount);
public record LedgerClosed;

/// <summary>A self-aggregating document: Snapshot registers a single-stream projection for it.</summary>
public partial class Ledger
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = "";
    public decimal Balance { get; set; }
    public LedgerState State { get; set; }
    public List<decimal> Credits { get; set; } = new();

    public static Ledger Create(LedgerOpened e) => new() { Owner = e.Owner, State = LedgerState.Open };

    public void Apply(LedgerCredited e)
    {
        Balance += e.Amount;
        Credits.Add(e.Amount);
    }

    public void Apply(LedgerClosed _) => State = LedgerState.Closed;
}

/// <summary>
///     The Polecat half of JasperFx 2.95's document diagnostics (polecat#820): explain (jasperfx#1078),
///     the indexes-reported signal and per-type metadata (jasperfx#1081 / #1077), and the live-aggregated
///     render (jasperfx#1082). The shared facts live in DocumentStoreDiagnosticsCompliance; these are the
///     ones it cannot reach — a real index, a seek versus a scan, a projection.
/// </summary>
public class document_diagnostics_295_tests
{
    private static async Task DropTablesAsync(string schema, params string[] tables)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        foreach (var table in tables)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID('[{schema}].[{table}]','U') IS NOT NULL DROP TABLE [{schema}].[{table}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task<DocumentStore> TicketStoreAsync(string schema, Action<StoreOptions>? configure = null)
    {
        await DropTablesAsync(schema, "pc_doc_explainticket");
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<ExplainTicket>();
            configure?.Invoke(opts);
        });
    }

    private static async Task<List<ExplainTicket>> SeedTicketsAsync(DocumentStore store, int count)
    {
        var tickets = Enumerable.Range(1, count)
            .Select(i => new ExplainTicket { Id = Guid.NewGuid(), Title = $"t{i}", Severity = i })
            .ToList();

        await using var session = store.LightweightSession();
        session.Store(tickets.ToArray());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return tickets;
    }

    private static string TicketType => typeof(ExplainTicket).FullName!;

    // ---------------------------------------------------------------- explain (jasperfx#1078)

    [Fact]
    public async Task explain_reports_a_scan_for_a_filter_no_index_serves()
    {
        await using var store = await TicketStoreAsync("explain_scan");
        await SeedTicketsAsync(store, 6);

        var plan = await ((IDocumentStoreDiagnostics)store).ExplainDocumentQueryAsync(TicketType,
            new DocumentQueryOptions(1, 10) { Where = "Severity > @0", Arguments = [4] },
            TestContext.Current.CancellationToken);

        plan.ShouldNotBeNull();
        plan.Engine.ShouldBe("SqlServer");
        plan.IsActual.ShouldBeTrue();
        plan.PlanText.ShouldContain("ShowPlanXML");
        plan.RowsReturned.ShouldBe(2);

        // Every row was read to find the two, and the plan says it scanned.
        plan.RowsExamined.ShouldBe(6);
        plan.ScansTable.ShouldBe(true);
        plan.IndexesUsed.ShouldNotBeNull();
    }

    [Fact]
    public async Task explain_reports_the_index_a_seek_uses()
    {
        await using var store = await TicketStoreAsync("explain_seek");
        var tickets = await SeedTicketsAsync(store, 6);

        var plan = await ((IDocumentStoreDiagnostics)store).ExplainDocumentQueryAsync(TicketType,
            new DocumentQueryOptions(1, 10, IdEquals: tickets[2].Id.ToString()),
            TestContext.Current.CancellationToken);

        plan.ShouldNotBeNull();
        plan.RowsReturned.ShouldBe(1);
        plan.RowsExamined.ShouldBe(1);
        plan.ScansTable.ShouldBe(false);
        plan.IndexesUsed.ShouldNotBeNull().ShouldNotBeEmpty();
    }

    [Fact]
    public async Task explain_names_a_declared_secondary_index()
    {
        await using var store = await TicketStoreAsync("explain_ix",
            opts => opts.Schema.For<ExplainTicket>().Index(x => x.Severity));
        await SeedTicketsAsync(store, 200);

        var plan = await ((IDocumentStoreDiagnostics)store).ExplainDocumentQueryAsync(TicketType,
            new DocumentQueryOptions(1, 10) { Where = "Severity = @0", Arguments = [7] },
            TestContext.Current.CancellationToken);

        plan.ShouldNotBeNull();
        plan.RowsReturned.ShouldBe(1);
        plan.ScansTable.ShouldBe(false);
        plan.IndexesUsed!.ShouldContain(x => x.Contains("severity", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task explain_refuses_an_uncaptured_metadata_filter_and_writes_nothing()
    {
        await using var store = await TicketStoreAsync("explain_refuse");
        await SeedTicketsAsync(store, 2);
        var diagnostics = (IDocumentStoreDiagnostics)store;

        var ex = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(() =>
            diagnostics.ExplainDocumentQueryAsync(TicketType,
                new DocumentQueryOptions(1, 10) { CorrelationId = "c" }, TestContext.Current.CancellationToken));
        ex.Criterion.ShouldBe(nameof(DocumentQueryOptions.CorrelationId));

        (await diagnostics.ExplainDocumentQueryAsync("No.Such.Type", new DocumentQueryOptions(1, 10),
            TestContext.Current.CancellationToken)).ShouldBeNull();

        (await diagnostics.QueryDocumentsAsync(TicketType, new DocumentQueryOptions(1, 10),
            TestContext.Current.CancellationToken)).TotalCount.ShouldBe(2);
    }

    // ---------------------------------------------------- descriptors (jasperfx#1081 / jasperfx#1077)

    private static async Task<DocumentMappingDescriptor> DescriptorForAsync(DocumentStore store, Type type)
    {
        var usage = await ((IDocumentStoreUsageSource)store).TryCreateUsage(TestContext.Current.CancellationToken);
        return usage!.Documents.Single(x => x.DocumentType.FullName == type.FullName);
    }

    [Fact]
    public async Task an_unindexed_type_reports_its_empty_index_list_as_complete()
    {
        await using var store = await TicketStoreAsync("desc_noindex");

        var descriptor = await DescriptorForAsync(store, typeof(ExplainTicket));

        descriptor.Indexes.ShouldBeEmpty();
        descriptor.IndexesReported.ShouldBeTrue();
        descriptor.DuplicatedFields.ShouldBeEmpty();
        descriptor.DuplicatedFieldsReported.ShouldBeTrue();
    }

    [Fact]
    public async Task declared_indexes_are_reported_including_a_json_index()
    {
        await using var store = await TicketStoreAsync("desc_indexes", opts =>
        {
            opts.Schema.For<ExplainTicket>().Index(x => x.Severity);
            if (ConnectionSource.SupportsNativeJson)
            {
                opts.Schema.For<ExplainTicket>().JsonIndex(x => x.Title);
            }
        });

        var descriptor = await DescriptorForAsync(store, typeof(ExplainTicket));

        descriptor.IndexesReported.ShouldBeTrue();
        // Member paths in declaration casing, which is what a console matches a predicate's members against
        // (they were the rendered JSON path, "$.severity", which matched nothing).
        descriptor.Indexes.ShouldContain(x => x.Members.SequenceEqual(new[] { "Severity" }));
        if (ConnectionSource.SupportsNativeJson)
        {
            // The JSON index used to be missing from the list altogether, which made an incomplete list
            // read as a complete one the moment a store set the reported flag.
            descriptor.Indexes.Count.ShouldBe(2);
        }

        descriptor.DuplicatedFields.ShouldHaveSingleItem().MemberPath.ShouldBe("Severity");
        descriptor.DuplicatedFieldsReported.ShouldBeTrue();
    }

    [Fact]
    public async Task metadata_capture_is_reported_per_type()
    {
        await using var store = await TicketStoreAsync("desc_metadata", opts =>
            opts.Schema.For<ExplainTicket>().Metadata(m => m.CorrelationId.Enabled = true));

        var metadata = (await DescriptorForAsync(store, typeof(ExplainTicket))).Metadata;

        metadata.ShouldNotBeNull();
        metadata.StoreType.ShouldBe("Polecat");
        metadata.CorrelationId.ShouldBeTrue();
        metadata.CausationId.ShouldBeFalse();
        metadata.LastModifiedBy.ShouldBeFalse();
        metadata.SoftDelete.ShouldBeFalse();
        metadata.TenantId.ShouldBeFalse();
    }

    [Fact]
    public async Task a_custom_serializer_leaves_the_serializer_settings_unreported()
    {
        await using var store = await TicketStoreAsync("desc_custom_ser",
            opts => opts.Serializer = new OpaqueSerializer(new Polecat.Serialization.Serializer()));

        var usage = await ((IDocumentStoreUsageSource)store).TryCreateUsage(TestContext.Current.CancellationToken);

        usage!.EnumStorage.ShouldBeNullOrEmpty();
        usage.SerializerCasing.ShouldBeNullOrEmpty();
    }

    // ------------------------------------------------- live-aggregated render (jasperfx#1082)

    private static async Task<DocumentStore> LedgerStoreAsync(string schema)
    {
        await DropTablesAsync(schema, "pc_doc_ledger", "pc_doc_explainticket");
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            // Names, not numbers, so a client-side default STJ rendering would differ from the store's.
            opts.ConfigureSerialization(enumStorage: EnumStorage.AsString);
            opts.Projections.Snapshot<Ledger>(SnapshotLifecycle.Inline);
            opts.Schema.For<ExplainTicket>();
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        return store;
    }

    [Fact]
    public async Task the_rebuilt_json_equals_what_the_inline_projection_stored()
    {
        await using var store = await LedgerStoreAsync("agg_json_equal");
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Ledger>(id, new LedgerOpened("ann"), new LedgerCredited(12.5m),
                new LedgerCredited(3m), new LedgerClosed());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var diagnostics = (IDocumentStoreDiagnostics)store;
        var stored = await diagnostics.LoadDocumentAsync(typeof(Ledger).FullName!, id.ToString(), null,
            TestContext.Current.CancellationToken);
        var rebuilt = await diagnostics.AggregateDocumentJsonAsync(typeof(Ledger).FullName!, id.ToString(), null,
            TestContext.Current.CancellationToken);

        stored.ShouldNotBeNull();
        rebuilt.ShouldNotBeNull();
        rebuilt.ShouldContain("\"Closed\"");
        JsonNode.DeepEquals(JsonNode.Parse(rebuilt), JsonNode.Parse(stored.Json)).ShouldBeTrue(
            $"rebuilt {rebuilt} vs stored {stored.Json}");

        // Nothing was stored: the row is still the projection's.
        (await diagnostics.QueryDocumentsAsync(typeof(Ledger).FullName!, new DocumentQueryOptions(1, 10),
            TestContext.Current.CancellationToken)).TotalCount.ShouldBe(1);
    }

    [Fact]
    public async Task the_rebuilt_json_differs_from_a_tampered_row()
    {
        await using var store = await LedgerStoreAsync("agg_json_tampered");
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Ledger>(id, new LedgerOpened("bob"), new LedgerCredited(5m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The read model drifts from its stream.
        await using (var session = store.LightweightSession())
        {
            session.Store(new Ledger { Id = id, Owner = "mallory", Balance = 1_000_000m });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var diagnostics = (IDocumentStoreDiagnostics)store;
        var stored = await diagnostics.LoadDocumentAsync(typeof(Ledger).FullName!, id.ToString(), null,
            TestContext.Current.CancellationToken);
        var rebuilt = await diagnostics.AggregateDocumentJsonAsync(typeof(Ledger).FullName!, id.ToString(), null,
            TestContext.Current.CancellationToken);

        JsonNode.DeepEquals(JsonNode.Parse(rebuilt!), JsonNode.Parse(stored!.Json)).ShouldBeFalse();
        JsonNode.Parse(rebuilt!)!["owner"]!.GetValue<string>().ShouldBe("bob");
    }

    [Fact]
    public async Task no_stream_reads_as_null()
    {
        await using var store = await LedgerStoreAsync("agg_json_missing");

        (await ((IDocumentStoreDiagnostics)store).AggregateDocumentJsonAsync(typeof(Ledger).FullName!,
            Guid.NewGuid().ToString(), null, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task a_plain_document_type_is_refused_and_an_unknown_one_is_an_argument_error()
    {
        await using var store = await LedgerStoreAsync("agg_json_refusals");
        var diagnostics = (IDocumentStoreDiagnostics)store;

        var refused = await Should.ThrowAsync<InvalidOperationException>(() =>
            diagnostics.AggregateDocumentJsonAsync(typeof(ExplainTicket).FullName!, Guid.NewGuid().ToString(), null,
                TestContext.Current.CancellationToken));
        refused.Message.ShouldContain(typeof(ExplainTicket).FullName!);

        await Should.ThrowAsync<ArgumentException>(() =>
            diagnostics.AggregateDocumentJsonAsync("No.Such.Type", Guid.NewGuid().ToString(), null,
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task document_types_name_the_single_stream_projection_that_produces_them()
    {
        await using var store = await LedgerStoreAsync("agg_json_types");

        var types = await ((IDocumentStoreDiagnostics)store).DocumentTypesAsync(TestContext.Current.CancellationToken);

        types.Single(x => x.TypeName == typeof(Ledger).FullNameInCode()).SingleStreamProjection
            .ShouldNotBeNullOrWhiteSpace();
        types.Single(x => x.TypeName == typeof(ExplainTicket).FullNameInCode()).SingleStreamProjection
            .ShouldBeNull();
    }

    /// <summary>A custom serializer the descriptor cannot interrogate: it wraps the default one.</summary>
    private sealed class OpaqueSerializer(Polecat.Serialization.Serializer inner) : ISerializer
    {
        public EnumStorage EnumStorage => inner.EnumStorage;
        public Casing Casing => inner.Casing;
        public T FromJson<T>(string json) => inner.FromJson<T>(json);
        public object FromJson(Type type, string json) => inner.FromJson(type, json);
        public string ToJson(object? document) => inner.ToJson(document);
        public T FromJson<T>(System.IO.Stream stream) => inner.FromJson<T>(stream);
        public T FromJson<T>(System.Data.Common.DbDataReader reader, int index) => inner.FromJson<T>(reader, index);
        public ValueTask<T> FromJsonAsync<T>(System.IO.Stream stream, CancellationToken cancellationToken = default)
            => inner.FromJsonAsync<T>(stream, cancellationToken);
        public ValueTask<T> FromJsonAsync<T>(System.Data.Common.DbDataReader reader, int index,
            CancellationToken cancellationToken = default) => inner.FromJsonAsync<T>(reader, index, cancellationToken);
        public object FromJson(Type type, System.IO.Stream stream) => inner.FromJson(type, stream);
        public object FromJson(Type type, System.Data.Common.DbDataReader reader, int index)
            => inner.FromJson(type, reader, index);
        public ValueTask<object> FromJsonAsync(Type type, System.IO.Stream stream,
            CancellationToken cancellationToken = default) => inner.FromJsonAsync(type, stream, cancellationToken);
        public ValueTask<object> FromJsonAsync(Type type, System.Data.Common.DbDataReader reader, int index,
            CancellationToken cancellationToken = default)
            => inner.FromJsonAsync(type, reader, index, cancellationToken);
        public string ToCleanJson(object? document) => inner.ToCleanJson(document);
    }
}
