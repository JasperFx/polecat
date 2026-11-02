using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #877: each string operator cast its receiver to a member, so anything chained onto another one
///     threw <see cref="InvalidCastException" /> from inside the translator instead of a refusal.
/// </summary>
public class chained_string_method_tests: OneOffConfigurationsContext
{
    public class ChainedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Text { get; set; }
        public string Other { get; set; } = string.Empty;
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
        session.Store(new ChainedDoc { Id = Guid.NewGuid(), Name = "padded", Text = "  Hello  ", Other = "x" });
        session.Store(new ChainedDoc { Id = Guid.NewGuid(), Name = "plain", Text = "hello", Other = "hello" });
        session.Store(new ChainedDoc { Id = Guid.NewGuid(), Name = "other", Text = "world", Other = "x" });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<string[]> namesAsync(IQueryable<ChainedDoc> query)
        => (await query.OrderBy(x => x.Name).ToListAsync(Ct)).Select(x => x.Name).ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task transforms_compose(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.Trim().ToLower() == "hello")))
            .ShouldBe(["padded", "plain"]);
        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.ToLower().Trim() == "hello")))
            .ShouldBe(["padded", "plain"]);
        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.ToUpper().TrimEnd() == "  HELLO")))
            .ShouldBe(["padded"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_text_operator_reads_a_transform(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.Trim().Contains("ell"))))
            .ShouldBe(["padded", "plain"]);
        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.Trim().ToLower().StartsWith("hel"))))
            .ShouldBe(["padded", "plain"]);
        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text!.Trim().ToUpper().EndsWith("LLO"))))
            .ShouldBe(["padded", "plain"]);
        (await namesAsync(query.Query<ChainedDoc>()
                .Where(x => x.Text!.Trim().Equals("HELLO", StringComparison.OrdinalIgnoreCase))))
            .ShouldBe(["padded", "plain"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_string_method_on_the_value_side_is_just_a_value(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var input = " WORLD ";

        // There is no document in it to transform, and it used to be sent looking for one.
        (await namesAsync(query.Query<ChainedDoc>().Where(x => x.Text == input.Trim().ToLower())))
            .ShouldBe(["other"]);
    }

    [Fact]
    public async Task a_transform_of_something_with_no_translation_is_refused()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ChainedDoc>().Where(x => x.Name.Substring(1).ToLower() == "a").ToListAsync(Ct));

        ex.Message.ShouldContain("Substring");
    }

    [Fact]
    public async Task comparing_two_members_is_refused()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        // These reached the expression compiler, which said a variable was not defined in its scope.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ChainedDoc>().Where(x => x.Text == x.Other).ToListAsync(Ct));

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ChainedDoc>().Where(x => x.Text!.ToLower() == x.Other).ToListAsync(Ct));
    }
}
