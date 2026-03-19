using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class StringMethodDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
}

[Collection("integration")]
public class linq_string_methods_tests : IntegrationContext
{
    public linq_string_methods_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task SeedData(string schema)
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = schema);

        // Clean any existing data from previous test runs
        await theStore.Advanced.CleanAllDocumentsAsync();

        theSession.Store(new StringMethodDoc { Id = Guid.NewGuid(), Name = "Alice", Code = "  ABC  " });
        theSession.Store(new StringMethodDoc { Id = Guid.NewGuid(), Name = "Bob", Code = "  DEF  " });
        theSession.Store(new StringMethodDoc { Id = Guid.NewGuid(), Name = "CAROL", Code = "GHI" });
        await theSession.SaveChangesAsync();
    }

    [Fact]
    public async Task where_to_lower_equals()
    {
        await SeedData("linq_tolower");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Name.ToLower() == "alice")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task where_to_lower_invariant_equals()
    {
        await SeedData("linq_tolowerinv");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Name.ToLowerInvariant() == "bob")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task where_to_upper_equals()
    {
        await SeedData("linq_toupper");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Name.ToUpper() == "ALICE")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task where_trim_equals()
    {
        await SeedData("linq_trim");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Code.Trim() == "ABC")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task where_trim_start_equals()
    {
        await SeedData("linq_trimstart");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Code.TrimStart() == "ABC  ")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task where_trim_end_equals()
    {
        await SeedData("linq_trimend");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Code.TrimEnd() == "  ABC")
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task where_string_length_equals()
    {
        await SeedData("linq_strlen");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Name.Length == 3)
            .ToListAsync();

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task where_string_length_greater_than()
    {
        await SeedData("linq_strlen_gt");

        var results = await theSession.Query<StringMethodDoc>()
            .Where(x => x.Name.Length > 3)
            .ToListAsync();

        results.Count.ShouldBe(2);
    }
}
