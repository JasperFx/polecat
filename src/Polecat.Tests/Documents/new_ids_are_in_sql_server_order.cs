using System.Data.SqlTypes;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.Tests.StrongTypedId.GeneratedIds;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Documents;

/// <summary>
///     Document and stream tables are keyed by a clustered <c>uniqueidentifier</c>, so the Guid ids Polecat makes
///     up come from <see cref="SequentialGuid.SqlServerOrder" />: each new id sorts after the last one the way SQL
///     Server compares them (which <see cref="SqlGuid" /> models), and new rows go in at the end of the index.
/// </summary>
[Collection("integration")]
public class new_ids_are_in_sql_server_order : IntegrationContext
{
    private const string Schema = "sqlserver_order_ids";

    public new_ids_are_in_sql_server_order(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = Schema; });
    }

    private static void shouldBeInSqlServerOrder(IReadOnlyList<Guid> ids)
    {
        ids.Distinct().Count().ShouldBe(ids.Count);

        for (var i = 1; i < ids.Count; i++)
        {
            new SqlGuid(ids[i - 1]).CompareTo(new SqlGuid(ids[i])).ShouldBeLessThan(0,
                $"id {i} ({ids[i]}) sorts before id {i - 1} ({ids[i - 1]}) in SQL Server");
        }
    }

    // Our ids in the order SQL Server sorts them. The filter keeps out rows another run left behind
    private static async Task<List<Guid>> orderedByTheServerAsync(string table, IReadOnlyList<Guid> ids)
    {
        var ours = ids.ToHashSet();
        return (await idsOrderedByTheServerAsync(table)).Where(ours.Contains).ToList();
    }

    private static async Task<List<Guid>> idsOrderedByTheServerAsync(string table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand($"select id from [{Schema}].[{table}] order by id", conn);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        var ids = new List<Guid>();
        while (await reader.ReadAsync(Ct))
        {
            ids.Add(reader.GetGuid(0));
        }

        return ids;
    }

    [Fact]
    public void the_event_store_makes_up_stream_ids_in_sql_server_order()
    {
        theStore.Options.EventGraph.StreamIdGenerator.ShouldBeSameAs(SequentialGuid.SqlServerOrder);
    }

    [Fact]
    public async Task stored_documents_get_ids_in_sql_server_order()
    {
        var users = Enumerable.Range(0, 100).Select(i => new User { FirstName = $"user-{i}" }).ToList();

        await using var session = theStore.LightweightSession();
        foreach (var user in users)
        {
            session.Store(user);
        }

        await session.SaveChangesAsync(Ct);

        var assigned = users.Select(x => x.Id).ToList();
        shouldBeInSqlServerOrder(assigned);
        (await orderedByTheServerAsync("pc_doc_user", assigned)).ShouldBe(assigned);
    }

    [Fact]
    public async Task bulk_inserted_documents_get_ids_in_sql_server_order()
    {
        var users = Enumerable.Range(0, 100).Select(i => new User { FirstName = $"user-{i}" }).ToList();

        await theStore.Advanced.BulkInsertAsync(users, Ct);

        var assigned = users.Select(x => x.Id).ToList();
        shouldBeInSqlServerOrder(assigned);
        (await orderedByTheServerAsync("pc_doc_user", assigned)).ShouldBe(assigned);
    }

    [Fact]
    public async Task strong_typed_guid_ids_are_in_sql_server_order()
    {
        var invoices = Enumerable.Range(0, 100).Select(i => new GeneratedInvoice { Name = $"invoice-{i}" }).ToList();

        await using var session = theStore.LightweightSession();
        foreach (var invoice in invoices)
        {
            session.Store(invoice);
        }

        await session.SaveChangesAsync(Ct);

        var assigned = invoices.Select(x => x.Id.Value).ToList();
        shouldBeInSqlServerOrder(assigned);
        (await orderedByTheServerAsync("pc_doc_generatedinvoice", assigned)).ShouldBe(assigned);
    }

    [Fact]
    public async Task streams_started_without_an_id_are_in_sql_server_order()
    {
        await using var session = theStore.LightweightSession();
        var streamIds = Enumerable.Range(0, 100)
            .Select(i => session.Events.StartStream(new QuestStarted($"quest-{i}")).Id)
            .ToList();

        await session.SaveChangesAsync(Ct);

        shouldBeInSqlServerOrder(streamIds);
        (await orderedByTheServerAsync("pc_streams", streamIds)).ShouldBe(streamIds);
    }

    [Fact]
    public async Task typed_streams_started_without_an_id_are_in_sql_server_order()
    {
        await using var session = theStore.LightweightSession();
        var streamIds = Enumerable.Range(0, 100)
            .Select(i => session.Events.StartStream<QuestParty>(new QuestStarted($"quest-{i}")).Id)
            .ToList();

        await session.SaveChangesAsync(Ct);

        shouldBeInSqlServerOrder(streamIds);
    }
}
