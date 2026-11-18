using System.Text.Json;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class StreamedDoc
{
    public Guid Id { get; set; }
    public int Order { get; set; }
    public string Name { get; set; } = "";
    public string Color { get; set; } = "";
    public int Count { get; set; }
}

public class StreamedOwner
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class StreamedPet
{
    public Guid Id { get; set; }
    public Guid OwnerId { get; set; }
    public string Name { get; set; } = "";
}

public record StreamedStarted(string Name);

/// <summary>
///     #801: <see cref="PolecatQueryableExtensions.ToAsyncEnumerable{T}" /> hands a query's rows out as
///     they are read. Every shape is checked against <c>ToListAsync()</c> over the same query, so the two
///     cannot drift apart.
/// </summary>
/// <remarks>
///     These files import <c>System.Linq</c> through implicit usings, where .NET 10's
///     <c>AsyncEnumerable.ToAsyncEnumerable(IEnumerable&lt;T&gt;)</c> is also in scope. Every call below
///     binding Polecat's overload, rather than that one, is part of what passes here: the BCL's would
///     enumerate synchronously, which Polecat refuses.
/// </remarks>
public class to_async_enumerable_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly string[] Colors = ["red", "green", "blue"];

    private async Task StoreDocsAsync(int count = 9)
    {
        await using var session = theStore.LightweightSession();
        for (var i = 1; i <= count; i++)
        {
            session.Store(new StreamedDoc
            {
                Id = Guid.NewGuid(), Order = i, Name = $"doc-{i}", Color = Colors[i % 3], Count = i * 10
            });
        }

        await session.SaveChangesAsync(Ct);
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> rows)
    {
        var list = new List<T>();
        await foreach (var row in rows)
        {
            list.Add(row);
        }

        return list;
    }

    [Fact]
    public async Task whole_documents_stream_in_query_order()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().Where(x => x.Order > 2).OrderByDescending(x => x.Order);

        var streamed = await CollectAsync(query.ToAsyncEnumerable(Ct));

        streamed.Select(x => x.Order).ShouldBe([9, 8, 7, 6, 5, 4, 3]);
        streamed.Select(x => x.Id).ShouldBe((await query.ToListAsync(Ct)).Select(x => x.Id));
    }

    [Fact]
    public async Task skip_and_take_are_honored()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();

        var streamed = await CollectAsync(session.Query<StreamedDoc>().OrderBy(x => x.Order).Skip(2).Take(3)
            .ToAsyncEnumerable(Ct));

        streamed.Select(x => x.Order).ShouldBe([3, 4, 5]);
    }

    [Fact]
    public async Task an_empty_result_yields_nothing()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();

        (await CollectAsync(session.Query<StreamedDoc>().Where(x => x.Order > 100).ToAsyncEnumerable(Ct)))
            .ShouldBeEmpty();
    }

    /// <summary>
    ///     The proof that rows are handed out as they are read rather than gathered first: a row that cannot
    ///     be deserialized part-way through. <c>ToListAsync()</c> returns nothing at all, while the stream
    ///     has already handed out every row before it.
    /// </summary>
    [Fact]
    public async Task rows_before_a_bad_one_have_already_been_handed_out()
    {
        await StoreDocsAsync(6);
        var conn = await OpenConnectionAsync();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"""
                UPDATE [{theStore.Options.DatabaseSchemaName}].[pc_doc_streameddoc]
                SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.count', 'not a number')
                WHERE CAST(JSON_VALUE(data, '$.order') AS int) = 4
                """;
            (await cmd.ExecuteNonQueryAsync(Ct)).ShouldBe(1);
        }

        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().OrderBy(x => x.Order);

        await Should.ThrowAsync<JsonException>(() => query.ToListAsync(Ct));

        var handedOut = new List<int>();
        await Should.ThrowAsync<JsonException>(async () =>
        {
            await foreach (var doc in query.ToAsyncEnumerable(Ct))
            {
                handedOut.Add(doc.Order);
            }
        });

        handedOut.ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task breaking_out_early_frees_the_session_for_the_next_query()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();

        await foreach (var doc in session.Query<StreamedDoc>().OrderBy(x => x.Order).ToAsyncEnumerable(Ct))
        {
            if (doc.Order == 2) break;
        }

        (await session.Query<StreamedDoc>().CountAsync(Ct)).ShouldBe(9);
    }

    [Fact]
    public async Task a_scalar_select_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().OrderBy(x => x.Order).Select(x => x.Count);

        var streamed = await CollectAsync(query.ToAsyncEnumerable(Ct));

        streamed.ShouldBe([10, 20, 30, 40, 50, 60, 70, 80, 90]);
        streamed.ShouldBe(await query.ToListAsync(Ct));
    }

    [Fact]
    public async Task a_value_type_id_select_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().OrderBy(x => x.Order).Select(x => x.Id);

        (await CollectAsync(query.ToAsyncEnumerable(Ct))).ShouldBe(await query.ToListAsync(Ct));
    }

    [Fact]
    public async Task a_projected_select_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().Where(x => x.Order <= 3).OrderBy(x => x.Order)
            .Select(x => new { x.Name, Doubled = x.Count * 2 });

        var streamed = await CollectAsync(query.ToAsyncEnumerable(Ct));

        streamed.Select(x => x.Name).ShouldBe(["doc-1", "doc-2", "doc-3"]);
        streamed.Select(x => x.Doubled).ShouldBe([20, 40, 60]);
        streamed.ShouldBe(await query.ToListAsync(Ct));
    }

    [Fact]
    public async Task a_distinct_select_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().Select(x => x.Color).Distinct();

        var streamed = await CollectAsync(query.ToAsyncEnumerable(Ct));

        streamed.OrderBy(x => x).ShouldBe(["blue", "green", "red"]);
        streamed.OrderBy(x => x).ShouldBe((await query.ToListAsync(Ct)).OrderBy(x => x));
    }

    [Fact]
    public async Task a_group_by_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>()
            .GroupBy(x => x.Color)
            .Select(g => new { Color = g.Key, Total = g.Sum(x => x.Count) });

        var streamed = (await CollectAsync(query.ToAsyncEnumerable(Ct))).OrderBy(x => x.Color).ToList();

        streamed.Select(x => x.Color).ShouldBe(["blue", "green", "red"]);
        streamed.ShouldBe((await query.ToListAsync(Ct)).OrderBy(x => x.Color));
    }

    [Fact]
    public async Task a_group_by_key_select_streams()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedDoc>().GroupBy(x => x.Color).Select(g => g.Key);

        (await CollectAsync(query.ToAsyncEnumerable(Ct))).OrderBy(x => x).ShouldBe(["blue", "green", "red"]);
    }

    [Fact]
    public async Task a_group_join_reads_its_rows_first_and_gives_the_same_answer()
    {
        var alice = Guid.NewGuid();
        var bob = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new StreamedOwner { Id = alice, Name = "Alice" }, new StreamedOwner { Id = bob, Name = "Bob" });
            write.Store(
                new StreamedPet { Id = Guid.NewGuid(), OwnerId = alice, Name = "Rex" },
                new StreamedPet { Id = Guid.NewGuid(), OwnerId = alice, Name = "Tom" },
                new StreamedPet { Id = Guid.NewGuid(), OwnerId = bob, Name = "Kit" });
            await write.SaveChangesAsync(Ct);
        }

        await using var session = theStore.QuerySession();
        var query = session.Query<StreamedOwner>()
            .GroupJoin(session.Query<StreamedPet>(), o => o.Id, p => p.OwnerId, (o, pets) => new { o, pets })
            .SelectMany(x => x.pets, (x, p) => new { Owner = x.o.Name, Pet = p.Name });

        var streamed = (await CollectAsync(query.ToAsyncEnumerable(Ct))).OrderBy(x => x.Pet).ToList();

        streamed.Select(x => $"{x.Owner}/{x.Pet}").ShouldBe(["Bob/Kit", "Alice/Rex", "Alice/Tom"]);
        streamed.ShouldBe((await query.ToListAsync(Ct)).OrderBy(x => x.Pet));
    }

    [Fact]
    public async Task raw_events_are_handed_out_through_the_default()
    {
        await using (var write = theStore.LightweightSession())
        {
            write.Events.StartStream(Guid.NewGuid(), new StreamedStarted("one"), new StreamedStarted("two"));
            await write.SaveChangesAsync(Ct);
        }

        await using var session = theStore.QuerySession();

        var events = await CollectAsync(session.Events.QueryAllRawEvents().ToAsyncEnumerable(Ct));

        events.Select(x => ((StreamedStarted)x.Data).Name).ShouldBe(["one", "two"]);
    }

    [Fact]
    public async Task a_cancelled_token_stops_the_query()
    {
        await StoreDocsAsync();
        await using var session = theStore.QuerySession();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in session.Query<StreamedDoc>().ToAsyncEnumerable(cancelled.Token))
            {
            }
        });
    }

    [Fact]
    public void a_queryable_that_is_not_polecats_is_refused()
    {
        Should.Throw<InvalidOperationException>(() =>
            new[] { 1, 2 }.AsQueryable().ToAsyncEnumerable(Ct));
    }
}
