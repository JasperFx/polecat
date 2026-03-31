using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Reading;

public class AdvSqlDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class AdvSqlVersionedDoc : IVersioned
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    [JsonIgnore]
    public Guid Version { get; set; }
}

public class FooJson
{
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class advanced_sql_query : IntegrationContext
{
    public advanced_sql_query(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_query_scalar()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_scalar");
        await theStore.Advanced.CleanAllDocumentsAsync();

        var doc = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Max" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var name = (await query.AdvancedSql.QueryAsync<string>(
            "SELECT JSON_VALUE(data, '$.name') FROM advsql_scalar.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*'",
            CancellationToken.None)).First();

        name.ShouldBe("Max");
    }

    [Fact]
    public async Task can_query_multiple_scalars()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_multi_scalar");

        await using var query = theStore.QuerySession();
        var (number, text) = (await query.AdvancedSql.QueryAsync<int, string>(
            "SELECT 5, 'foo'",
            CancellationToken.None)).First();

        number.ShouldBe(5);
        text.ShouldBe("foo");
    }

    [Fact]
    public async Task can_query_three_scalars()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_three_scalar");

        await using var query = theStore.QuerySession();
        var (number, text, flag) = (await query.AdvancedSql.QueryAsync<int, string, int>(
            "SELECT 5, 'foo', 1",
            CancellationToken.None)).First();

        number.ShouldBe(5);
        text.ShouldBe("foo");
        flag.ShouldBe(1);
    }

    [Fact]
    public async Task can_query_json_objects()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_json");

        await using var query = theStore.QuerySession();
        var results = await query.AdvancedSql.QueryAsync<FooJson>(
            "SELECT '{\"name\": \"hello\"}'",
            CancellationToken.None);

        results.Count.ShouldBe(1);
        results[0].Name.ShouldBe("hello");
    }

    [Fact]
    public async Task can_query_documents()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_docs");
        await theStore.Advanced.CleanAllDocumentsAsync();

        var doc1 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Anne" };
        var doc2 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Max" };
        theSession.Store(doc1);
        theSession.Store(doc2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var docs = await query.AdvancedSql.QueryAsync<AdvSqlDoc>(
            "SELECT id, data FROM advsql_docs.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*' ORDER BY JSON_VALUE(data, '$.name')",
            CancellationToken.None);

        docs.Count.ShouldBe(2);
        docs[0].Name.ShouldBe("Anne");
        docs[1].Name.ShouldBe("Max");
    }

    [Fact]
    public async Task can_query_with_parameters()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_params");
        await theStore.Advanced.CleanAllDocumentsAsync();

        var doc = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Max" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();

        // Default placeholder '?'
        var name = (await query.AdvancedSql.QueryAsync<string>(
            "SELECT JSON_VALUE(data, ?) FROM advsql_params.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*'",
            CancellationToken.None,
            "$.name")).First();

        name.ShouldBe("Max");

        // Custom placeholder '^'
        var name2 = (await query.AdvancedSql.QueryAsync<string>(
            '^',
            "SELECT JSON_VALUE(data, ^) FROM advsql_params.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*'",
            CancellationToken.None,
            "$.name")).First();

        name2.ShouldBe("Max");
    }

    [Fact]
    public async Task can_stream_results()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_stream");
        await theStore.Advanced.CleanAllDocumentsAsync();

        var doc1 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Alpha" };
        var doc2 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Beta" };
        var doc3 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Gamma" };
        theSession.Store(doc1);
        theSession.Store(doc2);
        theSession.Store(doc3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var names = new List<string>();
        await foreach (var name in query.AdvancedSql.StreamAsync<string>(
            "SELECT JSON_VALUE(data, '$.name') FROM advsql_stream.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*' ORDER BY JSON_VALUE(data, '$.name')",
            CancellationToken.None))
        {
            names.Add(name);
        }

        names.Count.ShouldBe(3);
        names[0].ShouldBe("Alpha");
        names[1].ShouldBe("Beta");
        names[2].ShouldBe("Gamma");
    }

    [Fact]
    public async Task can_stream_tuple_results()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_stream_tuple");

        await using var query = theStore.QuerySession();
        var results = new List<(int, string)>();
        await foreach (var item in query.AdvancedSql.StreamAsync<int, string>(
            "SELECT value, CONCAT('item_', CAST(value AS varchar)) FROM (VALUES (1),(2),(3)) AS s(value)",
            CancellationToken.None))
        {
            results.Add(item);
        }

        results.Count.ShouldBe(3);
        results[0].ShouldBe((1, "item_1"));
        results[1].ShouldBe((2, "item_2"));
        results[2].ShouldBe((3, "item_3"));
    }

    [Fact]
    public async Task can_query_document_and_scalar()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_doc_scalar");
        await theStore.Advanced.CleanAllDocumentsAsync();

        var doc1 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Anne" };
        var doc2 = new AdvSqlDoc { Id = Guid.NewGuid(), Name = "Max" };
        theSession.Store(doc1);
        theSession.Store(doc2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.AdvancedSql.QueryAsync<AdvSqlDoc, long>(
            "SELECT id, data, COUNT(*) OVER() FROM advsql_doc_scalar.pc_doc_advsqldoc WHERE tenant_id = '*DEFAULT*' ORDER BY JSON_VALUE(data, '$.name')",
            CancellationToken.None);

        results.Count.ShouldBe(2);
        results[0].Item1.Name.ShouldBe("Anne");
        results[0].Item2.ShouldBe(2);
        results[1].Item1.Name.ShouldBe("Max");
        results[1].Item2.ShouldBe(2);
    }

    [Fact]
    public async Task wrong_parameter_count_throws()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "advsql_bad_params");

        await using var query = theStore.QuerySession();

        // SQL Server will reject the query with an unreplaced placeholder
        await Should.ThrowAsync<SqlException>(async () =>
        {
            await query.AdvancedSql.QueryAsync<string>(
                "SELECT ? + ?",
                CancellationToken.None,
                "only_one_param");
        });
    }
}
