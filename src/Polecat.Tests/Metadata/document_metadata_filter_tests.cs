using System.Text.Json;
using JasperFx;
using JasperFx.Documents;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Metadata;

/// <summary>
/// #256: exact-match metadata filters on <see cref="IDocumentStoreDiagnostics.QueryDocumentsAsync"/>
/// (parity with marten#4791). Each of CorrelationId / CausationId / LastModifiedBy is honored when the
/// document mapping enables that opt-in column (#241/#243), and refused with
/// DocumentCriteriaNotSupportedException when it does not (jasperfx#1077) — it used to be dropped.
///
/// Seeding: 8 docs covering every combination of correlation ∈ {c0,c1} × causation ∈ {u0,u1} ×
/// last_modified_by ∈ {b0,b1}, indexed 0..7 by the (corr,caus,lmb) binary tuple.
/// </summary>
public class document_metadata_filter_tests
{
    public class DiagMetaDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static string NameForIndex(int i) => $"doc-{i}";

    private static string? NameOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        foreach (var p in doc.RootElement.EnumerateObject())
        {
            if (string.Equals(p.Name, "name", StringComparison.OrdinalIgnoreCase))
            {
                return p.Value.GetString();
            }
        }

        return null;
    }

    private static async Task<DocumentStore> CreateStoreAsync(
        string schema, bool enableCorr, bool enableCaus, bool enableLmb)
    {
        // Drop the doc table so each schema/metadata permutation starts clean across reruns.
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID('[{schema}].[pc_doc_diagmetadoc]','U') IS NOT NULL DROP TABLE [{schema}].[pc_doc_diagmetadoc];";
            await cmd.ExecuteNonQueryAsync();
        }

        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Schema.For<DiagMetaDoc>().Metadata(m =>
            {
                if (enableCorr) m.CorrelationId.Enabled = true;
                if (enableCaus) m.CausationId.Enabled = true;
                if (enableLmb) m.LastModifiedBy.Enabled = true;
            });
        });
    }

    private static async Task SeedMatrixAsync(DocumentStore store)
    {
        for (var i = 0; i < 8; i++)
        {
            await using var session = store.LightweightSession();
            session.CorrelationId = (i & 0b100) == 0 ? "c0" : "c1";
            session.CausationId = (i & 0b010) == 0 ? "u0" : "u1";
            session.LastModifiedBy = (i & 0b001) == 0 ? "b0" : "b1";
            session.Store(new DiagMetaDoc { Id = Guid.NewGuid(), Name = NameForIndex(i) });
            await session.SaveChangesAsync();
        }
    }

    private static Task<DocumentQueryResult> QueryAsync(DocumentStore store, DocumentQueryOptions options)
        => ((IDocumentStoreDiagnostics)store).QueryDocumentsAsync(typeof(DiagMetaDoc).FullName!, options, default);

    [Theory]
    // (corr, caus, lmb, expectedDocIndices) — every combo of the three filters on/off (2^3 = 8)
    [InlineData(null, null, null, new[] { 0, 1, 2, 3, 4, 5, 6, 7 })]
    [InlineData("c0", null, null, new[] { 0, 1, 2, 3 })]
    [InlineData(null, "u0", null, new[] { 0, 1, 4, 5 })]
    [InlineData(null, null, "b0", new[] { 0, 2, 4, 6 })]
    [InlineData("c0", "u0", null, new[] { 0, 1 })]
    [InlineData("c0", null, "b0", new[] { 0, 2 })]
    [InlineData(null, "u0", "b0", new[] { 0, 4 })]
    [InlineData("c0", "u0", "b0", new[] { 0 })]
    public async Task every_filter_combo_returns_the_expected_subset(
        string? corr, string? caus, string? lmb, int[] expectedIndices)
    {
        await using var store = await CreateStoreAsync("doc256_combo", true, true, true);
        await SeedMatrixAsync(store);

        var result = await QueryAsync(store, new DocumentQueryOptions(1, 100)
        {
            CorrelationId = corr,
            CausationId = caus,
            LastModifiedBy = lmb
        });

        result.TotalCount.ShouldBe(expectedIndices.Length);
        result.DocumentsJson.Select(NameOf).ShouldBe(expectedIndices.Select(NameForIndex), ignoreOrder: true);
    }

    [Fact]
    public async Task filter_on_unmatched_value_returns_zero()
    {
        await using var store = await CreateStoreAsync("doc256_unmatched", true, true, true);
        await SeedMatrixAsync(store);

        var result = await QueryAsync(store, new DocumentQueryOptions(1, 100) { CorrelationId = "nope" });

        result.TotalCount.ShouldBe(0);
        result.DocumentsJson.Count.ShouldBe(0);
    }

    /// <summary>
    /// jasperfx#1077: a metadata filter on a column this type does not capture used to be dropped, so the
    /// query returned the unfiltered page and total — indistinguishable from a filter that matched every
    /// row. It is refused now, naming the criterion, before any SQL runs.
    /// </summary>
    [Theory]
    [InlineData(false, true, true, "correlation", nameof(DocumentQueryOptions.CorrelationId))]
    [InlineData(true, false, true, "causation", nameof(DocumentQueryOptions.CausationId))]
    [InlineData(true, true, false, "lastmodifiedby", nameof(DocumentQueryOptions.LastModifiedBy))]
    public async Task filter_on_a_column_the_type_does_not_capture_is_refused(
        bool enableCorr, bool enableCaus, bool enableLmb, string which, string criterion)
    {
        await using var store = await CreateStoreAsync($"doc256_off_{which}", enableCorr, enableCaus, enableLmb);
        await SeedMatrixAsync(store);

        // Set ONLY the disabled filter to a value that would otherwise narrow to 4 docs.
        var options = which switch
        {
            "correlation" => new DocumentQueryOptions(1, 100) { CorrelationId = "c0" },
            "causation" => new DocumentQueryOptions(1, 100) { CausationId = "u0" },
            _ => new DocumentQueryOptions(1, 100) { LastModifiedBy = "b0" }
        };

        var ex = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(() => QueryAsync(store, options));
        ex.Criterion.ShouldBe(criterion);
        ex.Message.ShouldContain(typeof(DiagMetaDoc).Name);
    }

    [Fact]
    public async Task an_uncaptured_filter_is_refused_even_beside_captured_ones()
    {
        // causation disabled, correlation + lmb enabled. The two honored filters used to narrow the page
        // while the third was dropped without a word; the whole query is refused instead.
        await using var store = await CreateStoreAsync("doc256_mixed", true, false, true);
        await SeedMatrixAsync(store);

        var ex = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(() => QueryAsync(store,
            new DocumentQueryOptions(1, 100)
            {
                CorrelationId = "c0",
                CausationId = "u0",
                LastModifiedBy = "b0"
            }));
        ex.Criterion.ShouldBe(nameof(DocumentQueryOptions.CausationId));
    }

    [Fact]
    public async Task id_and_metadata_filters_compose_with_and()
    {
        await using var store = await CreateStoreAsync("doc256_id_meta", true, true, true);
        await SeedMatrixAsync(store);

        // doc 0 has corr=c0, caus=u0, lmb=b0. Find its id.
        var doc0Json = (await QueryAsync(store, new DocumentQueryOptions(1, 100)
        {
            CorrelationId = "c0", CausationId = "u0", LastModifiedBy = "b0"
        })).DocumentsJson.Single();
        using var d = JsonDocument.Parse(doc0Json);
        var id = d.RootElement.EnumerateObject()
            .First(p => string.Equals(p.Name, "id", StringComparison.OrdinalIgnoreCase)).Value.GetString();

        // id + all matching metadata → that one row.
        var hit = await QueryAsync(store, new DocumentQueryOptions(1, 100, IdEquals: id)
        {
            CorrelationId = "c0", CausationId = "u0", LastModifiedBy = "b0"
        });
        hit.TotalCount.ShouldBe(1);

        // same id but a non-matching metadata value → AND yields empty.
        var miss = await QueryAsync(store, new DocumentQueryOptions(1, 100, IdEquals: id)
        {
            CorrelationId = "c1"
        });
        miss.TotalCount.ShouldBe(0);
    }
}
