using System.Diagnostics.CodeAnalysis;
using JasperFx.Documents;
using JasperFx.Linq;
using Polecat.Linq;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Diagnostics;

/// <summary>
///     Two seeded stores for <see cref="document_diagnostics_criteria_shapes" />: the defaults (camelCase,
///     enums as integers) and the other end of both switches (member names as written, enums as names).
/// </summary>
public class CriteriaShapeStores : IAsyncLifetime
{
    public DocumentStore Defaults { get; private set; } = null!;
    public DocumentStore NamesAndPascal { get; private set; } = null!;
    public IReadOnlyList<CriteriaOrder> Documents { get; private set; } = [];

    public async ValueTask InitializeAsync()
    {
        Defaults = await CriteriaShapes.CreateStoreAsync("gh869_shapes", EnumStorage.AsInteger);
        NamesAndPascal = await CriteriaShapes.CreateStoreAsync("gh869_shapes_names", EnumStorage.AsString, Casing.Default);

        Documents = await CriteriaShapes.SeedAsync(Defaults);
        await CriteriaShapes.SeedAsync(NamesAndPascal);
    }

    public async ValueTask DisposeAsync()
    {
        await Defaults.DisposeAsync();
        await NamesAndPascal.DisposeAsync();
    }

    public DocumentStore For(string store) => store == "defaults" ? Defaults : NamesAndPascal;
}

/// <summary>
///     jasperfx#869: every Dynamic LINQ shape measured against Polecat, through the diagnostics surface,
///     each compared with LINQ to objects running the same text over the same documents.
/// </summary>
/// <remarks>
///     <para>
///         The verdicts on <see cref="CriteriaShapes.All" /> ARE the measurement. A shape either matches the
///         oracle, is refused because the provider cannot translate it, is refused by
///         <c>DiagnosticsCriteria</c>'s policy because the provider would answer it WRONGLY, or differs only
///         by SQL Server's case-insensitive collation.
///     </para>
///     <para>
///         ⚠️ For a policy refusal the test also runs the RAW provider, without the policy, and asserts it
///         is still wrong. When Polecat starts translating one of those shapes correctly, that assertion
///         fails — which is the signal to delete the rule, not to delete the assertion.
///     </para>
/// </remarks>
public class document_diagnostics_criteria_shapes : IClassFixture<CriteriaShapeStores>
{
    private readonly CriteriaShapeStores _stores;

    public document_diagnostics_criteria_shapes(CriteriaShapeStores stores)
    {
        _stores = stores;
    }

    public static TheoryData<string, string> Cases()
    {
        var data = new TheoryData<string, string>();
        foreach (var store in new[] { "defaults", "names_and_pascal" })
        foreach (var shape in CriteriaShapes.All)
        {
            data.Add(store, shape.Name);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task shape(string store, string shapeName)
    {
        var shape = CriteriaShapes.All.Single(x => x.Name == shapeName);
        var documentStore = _stores.For(store);
        var verdict = shape.VerdictFor(enumsAreNames: store != "defaults");

        var (oracle, oracleError) = CriteriaShapes.Oracle(_stores.Documents, shape);

        switch (verdict)
        {
            case ShapeVerdict.Matches:
                oracleError.ShouldBeNull();
                (await DiagnosticsRowsAsync(documentStore, shape)).ShouldBe(oracle!);
                break;

            case ShapeVerdict.CaseInsensitiveCollation:
                var rows = await DiagnosticsRowsAsync(documentStore, shape);
                rows.Select(CriteriaShapes.Label).ShouldBe(shape.CollationRows!);
                rows.ShouldNotBe(oracle!, "a collation shape that now agrees with the oracle is a plain match");
                break;

            case ShapeVerdict.ProviderRefuses:
                var refused = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(
                    () => DiagnosticsRowsAsync(documentStore, shape));
                refused.Criterion.ShouldBe(shape.Where is null ? "OrderBy" : "Where");
                break;

            case ShapeVerdict.PolicyRefuses:
                oracleError.ShouldBeNull();
                var byPolicy = await Should.ThrowAsync<DocumentCriteriaNotSupportedException>(
                    () => DiagnosticsRowsAsync(documentStore, shape));
                PolicyRefusals.ShouldContain(reason => byPolicy.Message.Contains(reason));

                // The rule is only justified while the provider is still wrong without it.
                var raw = await RawProviderRowsAsync(documentStore, shape);
                raw.ShouldNotBe(oracle!, $"Polecat now translates '{shape.Where ?? shape.OrderBy}' correctly: remove its rule from DiagnosticsCriteria");
                break;
        }
    }

    private static readonly string[] PolicyRefusals =
    [
        DiagnosticsCriteria.EnumRangeRefusal, DiagnosticsCriteria.EnumOrderingRefusal,

        // DynamicQueryShapeRules.SqlNullSemantics(), JasperFx's own text.
        "the database leaves rows where it is null OUT"
    ];

    private static async Task<IReadOnlyList<Guid>> DiagnosticsRowsAsync(DocumentStore store, CriteriaShape shape)
    {
        var result = await ((IDocumentStoreDiagnostics)store).QueryDocumentsAsync(typeof(CriteriaOrder).FullName!,
            new DocumentQueryOptions(1, 100) { Where = shape.Where, OrderBy = shape.OrderBy, Arguments = shape.Args },
            TestContext.Current.CancellationToken);

        result.TotalCount.ShouldBe(result.Documents.Count);
        var ids = result.Documents.Select(x => Guid.Parse(x.Id)).ToList();
        return shape.OrderBy is null ? ids.OrderBy(x => x).ToList() : ids;
    }

    [UnconditionalSuppressMessage("AOT", "IL3050")]
    [UnconditionalSuppressMessage("Trimming", "IL2026")]
    private static async Task<IReadOnlyList<Guid>> RawProviderRowsAsync(DocumentStore store, CriteriaShape shape)
    {
        await using var session = store.QuerySession();
        var queryable = DynamicQuery.Apply(session.Query<CriteriaOrder>(),
            new DynamicQueryText(shape.Where, shape.OrderBy is null ? null : shape.OrderBy + ", Id", shape.Args));

        var rows = await queryable.ToListAsync(TestContext.Current.CancellationToken);
        var ids = rows.Select(x => x.Id).ToList();
        return shape.OrderBy is null ? ids.OrderBy(x => x).ToList() : ids;
    }
}
