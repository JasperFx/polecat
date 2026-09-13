using Microsoft.Data.SqlClient;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     The schema half of gh-611: the token table, the trigger that keeps it in step with the
///     document table, and the backfill that makes a newly declared index cover rows that predate it.
/// </summary>
/// <remarks>
///     These read the token table directly rather than through LINQ, because the search operators are
///     the next increment. That is deliberate: the trigger and the backfill are where this design can
///     fail silently, and a fact that goes through a query cannot tell a missing token from a
///     mistranslated <c>Where</c>.
/// </remarks>
public class full_text_index_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    private async Task<List<(string Member, string Term, int Pos)>> tokensForAsync(Guid id)
    {
        var mapping = theStore.Options.Providers.GetProvider(typeof(Article)).Mapping;
        var table = FullTextIndex.TableNameFor(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT member, term, pos FROM [{mapping.DatabaseSchemaName}].[{table}] WHERE doc_id = @id ORDER BY member, pos";
        cmd.Parameters.AddWithValue("@id", id);

        var results = new List<(string, string, int)>();
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return results;
    }

    [Fact]
    public async Task storing_a_document_tokenizes_the_declared_member()
    {
        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Body));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = "The quick, brown fox jumps!" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var tokens = await tokensForAsync(id);

        // Lowercased, punctuation split rather than glued to its neighbour, and positions recorded in
        // source order — which is what makes a phrase search possible later without a migration.
        tokens.Select(x => x.Term).ShouldBe(["the", "quick", "brown", "fox", "jumps"]);
        tokens.Select(x => x.Pos).ShouldBe(tokens.Select(x => x.Pos).OrderBy(x => x));
        tokens.ShouldAllBe(x => x.Member == "Body");
    }

    [Fact]
    public async Task updating_a_document_replaces_its_tokens_and_deleting_removes_them()
    {
        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Body));
        var token = TestContext.Current.CancellationToken;
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = "jumps over the lazy dog" });
            await session.SaveChangesAsync(token);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Article { Id = id, Body = "the fox is gone" });
            await session.SaveChangesAsync(token);
        }

        var afterUpdate = await tokensForAsync(id);
        afterUpdate.Select(x => x.Term).ShouldNotContain("jumps");
        afterUpdate.Select(x => x.Term).ShouldContain("gone");

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Article>(id);
            await session.SaveChangesAsync(token);
        }

        (await tokensForAsync(id)).ShouldBeEmpty();
    }

    [Fact]
    public async Task declaring_an_index_backfills_rows_that_predate_it()
    {
        // The failure this exists to catch is silent: a trigger only fires on writes made after it
        // exists, so without a backfill this document would be invisible to search with no error
        // anywhere. Fisher met the same thing and answered it with FTS5's `rebuild`.
        var token = TestContext.Current.CancellationToken;
        var before = Guid.NewGuid();

        ConfigureStore(_ => { });
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Article { Id = before, Body = "written before the declaration existed" });
            await session.SaveChangesAsync(token);
        }

        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Body));

        (await tokensForAsync(before)).Select(x => x.Term).ShouldContain("declaration");
    }

    [Fact]
    public async Task two_declared_members_are_both_maintained()
    {
        // One trigger serves the table, so a per-index rendering would have the second member's
        // CREATE OR ALTER replace the first member's trigger and leave it silently unmaintained.
        ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Title, x => x.Body));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Article { Id = id, Title = "headline here", Body = "body text here" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var tokens = await tokensForAsync(id);

        tokens.Where(x => x.Member == "Title").Select(x => x.Term).ShouldBe(["headline", "here"]);
        tokens.Where(x => x.Member == "Body").Select(x => x.Term).ShouldBe(["body", "text", "here"]);
    }

    [Fact]
    public async Task declaring_a_full_text_index_on_a_non_string_member_is_refused()
    {
        var ex = Should.Throw<InvalidOperationException>(() =>
            ConfigureStore(opts => opts.Schema.For<Article>().FullTextIndex(x => x.Id)));

        ex.Message.ShouldContain("has no text to index");
        await Task.CompletedTask;
    }
}
