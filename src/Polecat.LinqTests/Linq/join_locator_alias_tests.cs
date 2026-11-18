using Polecat.Linq;
using Polecat.Linq.Joins;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #875: both sides of a join have an <c>id</c>, so a locator has to name its table. Only the literal
///     <c>id</c> was aliased, and anything built around it was left bare, which is Msg 209 from the server:
///     <c>x.Id.Length</c> is <c>DATALENGTH(CAST(id AS ...))</c> and <c>x.Id != null</c> is
///     <c>id IS NOT NULL</c>.
/// </summary>
public class join_locator_alias_tests: OneOffConfigurationsContext
{
    public class StringKeyedDoc
    {
        public string Id { get; set; } = string.Empty;
        public string Owner { get; set; } = string.Empty;
    }

    public class OwnerDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(new StringKeyedDoc { Id = "abc", Owner = "ann" }, new StringKeyedDoc { Id = "abcd", Owner = "bob" });
        session.Store(new OwnerDoc { Id = Guid.NewGuid(), Name = "ann", Age = 30 },
            new OwnerDoc { Id = Guid.NewGuid(), Name = "bob", Age = 40 });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<string[]> joinedAsync(IQuerySession query, IQueryable<StringKeyedDoc> outer)
    {
        var rows = await outer
            .GroupJoin(query.Query<OwnerDoc>(), o => o.Owner, i => i.Name, (o, i) => new { o, i })
            .SelectMany(t => t.i, (t, i) => new { t.o.Id, i.Age })
            .ToListAsync(Ct);

        return rows.Select(x => x.Id).OrderBy(x => x).ToArray();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_computed_from_the_id_is_aliased(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await joinedAsync(query, query.Query<StringKeyedDoc>().Where(x => x.Id.Length == 3))).ShouldBe(["abc"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_transform_of_the_id_is_aliased(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await joinedAsync(query, query.Query<StringKeyedDoc>().Where(x => x.Id.ToLower() == "abcd")))
            .ShouldBe(["abcd"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_check_on_the_id_is_aliased(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await joinedAsync(query, query.Query<StringKeyedDoc>().Where(x => x.Id != null)))
            .ShouldBe(["abc", "abcd"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_join_key_computed_from_the_id_is_aliased(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<StringKeyedDoc>()
            .GroupJoin(query.Query<OwnerDoc>(), o => o.Id.Length, i => i.Name.Length, (o, i) => new { o, i })
            .SelectMany(t => t.i, (t, i) => new { t.o.Id, i.Name })
            .ToListAsync(Ct);

        rows.Select(x => x.Id).Distinct().ShouldBe(["abc"]);
    }

    [Theory]
    [InlineData("id", "t.id")]
    [InlineData("LOWER(id)", "LOWER(t.id)")]
    [InlineData("id IS NOT NULL", "t.id IS NOT NULL")]
    [InlineData("CAST(DATALENGTH(CAST(id AS nvarchar(max))) / 2 AS int)", "CAST(DATALENGTH(CAST(t.id AS nvarchar(max))) / 2 AS int)")]
    [InlineData("JSON_VALUE(data, '$.id')", "JSON_VALUE(t.data, '$.id')")]
    [InlineData("JSON_VALUE(data, '$.data.id')", "JSON_VALUE(t.data, '$.data.id')")]
    [InlineData("OPENJSON(data) WITH (pc_value nvarchar(max) '$.id')", "OPENJSON(t.data) WITH (pc_value nvarchar(max) '$.id')")]
    [InlineData("tenant_id = @p0", "tenant_id = @p0")]
    [InlineData("t.id", "t.id")]
    [InlineData("@id", "@id")]
    [InlineData("JSON_VALUE(data, '$.it''s id')", "JSON_VALUE(t.data, '$.it''s id')")]
    public void only_a_bare_column_outside_a_literal_is_aliased(string locator, string expected)
    {
        JoinStatement.AliasLocator(locator, "t").ShouldBe(expected);
    }
}
