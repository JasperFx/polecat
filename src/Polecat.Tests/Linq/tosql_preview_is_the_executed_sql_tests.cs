using System.Text.Json.Serialization;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class PreviewDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>A document with a metadata read column, which execution adds to the select list.</summary>
public class PreviewVersionedDoc : IVersioned
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";

    [JsonIgnore]
    public Guid Version { get; set; }
}

/// <summary>
///     gh-1002 (ported from marten#5916): <c>ToSql()</c> rendered the statement before the preparation
///     execution runs, so for a document with metadata read columns it previewed <c>SELECT data ...</c>
///     while <c>SELECT data, guid_version ...</c> executed. Each fact compares the preview with the text
///     SQL Server actually ran, read back through <see cref="ExecutedStatements" />.
/// </summary>
public class tosql_preview_is_the_executed_sql_tests : OneOffConfigurationsContext
{
    private const string Schema = "tosql_preview_is_the_executed_sql_tests";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public tosql_preview_is_the_executed_sql_tests()
    {
        // ExecutedStatements needs it: a pooled connection's re-run of a cached statement can stay out of the
        // query stats
        ConfigureStore(opts => opts.ConnectionString =
            new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Pooling = false }.ConnectionString);
    }

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        for (var i = 1; i <= 5; i++)
        {
            session.Store(new PreviewDoc { Id = Guid.NewGuid(), Number = i, Name = "n" + i });
            session.Store(new PreviewVersionedDoc { Id = Guid.NewGuid(), Number = i, Name = "n" + i });
        }

        await session.SaveChangesAsync(Ct);
    }

    private async Task previewShouldBeWhatExecutes<T>(string tableTypeName, Func<IQuerySession, IQueryable<T>> query)
        where T : class
    {
        await SeedAsync();

        string previewed;
        await using (var session = theStore.QuerySession())
        {
            previewed = ExecutedStatements.Normalize(session.ToSql(query(session)));
        }

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);

        // #1046's fix for the sibling class, which this one missed: the old clock comparison could miss the
        // query and read back the seeding MERGE instead
        var snapshot = await ExecutedStatements.SnapshotAsync(conn, $"%{Schema}%pc_doc_{tableTypeName}%");

        await using (var session = theStore.QuerySession())
        {
            await query(session).ToListAsync(Ct);
        }

        var executed = await snapshot.ExecutedSinceAsync(x => x.Contains(previewed));

        executed.ShouldContain(previewed,
            $"ToSql() previewed:{Environment.NewLine}  {previewed}{Environment.NewLine}"
            + $"statements executed against the table since the query started:{Environment.NewLine}  "
            + string.Join(Environment.NewLine + "  ", executed));
    }

    [Fact]
    public Task a_plain_document_query() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 2));

    // gh-1002: the defect
    [Fact]
    public Task a_document_with_metadata_read_columns() =>
        previewShouldBeWhatExecutes("previewversioneddoc",
            s => s.Query<PreviewVersionedDoc>().Where(x => x.Number > 2));

    // gh-1002: the defect, paged
    [Fact]
    public Task a_paged_document_with_metadata_read_columns() =>
        previewShouldBeWhatExecutes("previewversioneddoc",
            s => s.Query<PreviewVersionedDoc>().Where(x => x.Number > 0).OrderBy(x => x.Number).Take(3));

    [Fact]
    public Task a_projection() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => new { x.Id, x.Number }));

    [Fact]
    public Task a_projection_of_a_document_with_metadata_read_columns() =>
        previewShouldBeWhatExecutes("previewversioneddoc",
            s => s.Query<PreviewVersionedDoc>().Where(x => x.Number > 1).Select(x => new { x.Id, x.Number }));

    [Fact]
    public Task a_scalar_select() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => x.Name));

    [Fact]
    public Task a_distinct() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 1).Select(x => x.Name).Distinct());

    [Fact]
    public Task a_paged_query() =>
        previewShouldBeWhatExecutes("previewdoc",
            s => s.Query<PreviewDoc>().Where(x => x.Number > 0).OrderByDescending(x => x.Number).Skip(1).Take(2));

    [Fact]
    public async Task a_group_join_is_refused()
    {
        await using var session = theStore.QuerySession();

        var joined = session.Query<PreviewDoc>()
            .GroupJoin(session.Query<PreviewVersionedDoc>(), d => d.Number, v => v.Number, (d, vs) => new { d, vs })
            .SelectMany(x => x.vs, (x, v) => new { x.d.Name, v.Number });

        Should.Throw<BadLinqExpressionException>(() => session.ToSql(joined));
    }
}
