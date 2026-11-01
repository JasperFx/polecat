using Polecat.Internal;
using Polecat.Storage;
using Polecat.Storage.FullText;

namespace Polecat.Tests.Storage;

/// <summary>
///     The constants the string predicates and the full-text tokenizers share. Each of these fails as a
///     wrong answer rather than an error, so they are held to their sources rather than read.
/// </summary>
public class sql_text_tests
{
    [Fact]
    public void the_whitespace_list_is_what_the_runtime_calls_whitespace()
    {
        var runtime = Enumerable.Range(0, char.MaxValue + 1)
            .Select(c => (char)c)
            .Where(char.IsWhiteSpace);

        // A character missing here is a value IsNullOrWhiteSpace answers differently in SQL and in C#,
        // and two tokenizers that stop agreeing.
        SqlText.Whitespace.ToCharArray().ShouldBe(runtime);
    }

    [Fact]
    public void the_whitespace_expression_names_every_character_once()
    {
        foreach (var c in SqlText.Whitespace)
        {
            SqlText.WhitespaceExpression.Split('+').ShouldContain($"NCHAR({(int)c})");
        }

        SqlText.WhitespaceExpression.Split('+').Length.ShouldBe(SqlText.Whitespace.Length);
    }

    [Fact]
    public void whitespace_and_punctuation_are_disjoint()
    {
        // The two are concatenated for TRANSLATE, whose from and to have to be the same length.
        FullTextIndex.Punctuation.Intersect(SqlText.Whitespace).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_clip_is_the_width_the_token_table_declares()
    {
        var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
        options.Schema.For<ClipDoc>().FullTextIndex(x => x.Body);

        await using var store = new DocumentStore(options);
        var mapping = store.Options.Providers.GetProvider(typeof(ClipDoc)).Mapping;

        new FullTextTokenTable(mapping).Columns.Single(x => x.Name == "term").Type
            .ShouldBe($"varchar({FullTextIndex.MaxTermLength})");
    }

    [Fact]
    public void a_term_is_clipped_on_the_query_side()
    {
        var wide = new string('a', FullTextIndex.MaxTermLength + 10);

        FullTextIndex.Tokenize(wide).Single().Length.ShouldBe(FullTextIndex.MaxTermLength);
    }

    [Theory]
    [InlineData("varchar(250)", 250)]
    [InlineData("nvarchar(300)", 300)]
    [InlineData("char(10)", 10)]
    [InlineData("nvarchar(max)", int.MaxValue)]
    [InlineData("uniqueidentifier", int.MaxValue)]
    public void text_capacity_reads_the_declared_width(string sqlType, int expected)
    {
        SqlTypeMap.TextCapacity(sqlType).ShouldBe(expected);
    }

    public class ClipDoc
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }
}
