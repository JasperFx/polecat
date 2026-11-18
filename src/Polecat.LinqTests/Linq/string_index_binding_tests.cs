using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #877: the #363 rule on the LINQ path. A default index on a string member is <c>varchar(250)</c>, and
///     a string binds as <c>nvarchar</c>, so #223's seek never happened: under a SQL collation the
///     conversion lands on the column. And the column cannot hold what its code page cannot, so a value
///     outside it was compared with the <c>??</c> that was stored and found nothing.
/// </summary>
public class string_index_binding_tests: OneOffConfigurationsContext
{
    public class CodedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Code { get; set; }
    }

    public class KeyedDoc
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    private const string Tokyo = "東京";
    private const string Osaka = "大阪";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<CodedDoc>().Index(x => x.Code);
        });

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < 2000; i++)
            {
                session.Store(new CodedDoc { Id = Guid.NewGuid(), Name = $"n{i}", Code = $"code-{i}" });
            }

            session.Store(new CodedDoc { Id = Guid.NewGuid(), Name = "tokyo", Code = Tokyo });
            session.Store(new CodedDoc { Id = Guid.NewGuid(), Name = "osaka", Code = Osaka });
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    private static void shouldSeek(IReadOnlyList<PlanAccess> accesses, string table)
    {
        accesses.ShouldContain(x => x.Operator.EndsWith("Index Seek") && x.Table == table);
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan") && x.Table == table);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_ascii_value_seeks_a_default_string_index(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var queryable = query.Query<CodedDoc>().Where(x => x.Code == "code-42");

        (await queryable.ToListAsync(Ct)).Single().Name.ShouldBe("n42");
        shouldSeek(await QueryPlanProbe.AccessesAsync(query, queryable, Ct), "pc_doc_codeddoc");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_value_outside_the_code_page_is_found_in_the_document(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<CodedDoc>().Where(x => x.Code == Tokyo).ToListAsync(Ct)).Single().Name.ShouldBe("tokyo");

        // Same length, so through the column these are the same two question marks.
        (await query.Query<CodedDoc>().Where(x => x.Code == Osaka).ToListAsync(Ct)).Single().Name.ShouldBe("osaka");
        (await query.Query<CodedDoc>().Where(x => x.Code.IsOneOf(Tokyo, "code-1")).ToListAsync(Ct)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_string_id_seeks_its_primary_key()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < 2000; i++)
            {
                session.Store(new KeyedDoc { Id = $"key-{i}", Name = $"n{i}" });
            }

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var queryable = query.Query<KeyedDoc>().Where(x => x.Id == "key-42");

        (await queryable.ToListAsync(Ct)).Single().Name.ShouldBe("n42");
        shouldSeek(await QueryPlanProbe.AccessesAsync(query, queryable, Ct), "pc_doc_keyeddoc");
    }
}
