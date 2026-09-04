using System.Text.Json;
using JasperFx;
using JasperFx.Events.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.CommandLine;

/// <summary>
/// #532 (jasperfx#737): end-to-end coverage of the <c>event-query</c> CLI command against a REAL
/// Polecat store — upstream carries only input-mapping unit tests, so until this class nothing
/// anywhere executed <see cref="EventQueryCommand"/> against a live event store. The command is
/// driven exactly as the CLI runtime drives it: a real <see cref="EventQueryInput"/> with its
/// <c>HostBuilder</c> set (the command builds and disposes the host itself, resolves the store
/// from DI via <c>GetServices&lt;IEventStore&gt;()</c> — the registration AddPolecat makes), stdout
/// captured and parsed as the JSON report agents and scripts consume.
///
/// Three facts: a filtered query returning exact expected events + totalCount; the honesty case
/// where a filter matches nothing (totalCount 0 printed as a REAL answer, success return); and the
/// guard-rail case where a supplied filter the store does not capture is refused with a report
/// naming the field and a failure return — never an unfiltered result that reads as filtered.
/// </summary>
public class event_query_command_end_to_end
{
    private const string Schema = "event_query_cli";

    public record OrderPlaced(int OrderId);

    public record OrderShipped(int OrderId);

    private static StoreOptions ConfigureOptions(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        // Deliberately NOT enabling the metadata columns (EnableUserName etc.) — the refusal fact
        // depends on the store genuinely not capturing user_name.
        return opts;
    }

    private static async Task DropSchemaAsync()
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('[{Schema}].[pc_events]','U') IS NOT NULL DROP TABLE [{Schema}].[pc_events];
            IF OBJECT_ID('[{Schema}].[pc_streams]','U') IS NOT NULL DROP TABLE [{Schema}].[pc_streams];
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Five events over two streams, one save each so the global sequence is deterministic:
    /// Placed(1), Shipped(1), Placed(2), Shipped(2), Placed(3).
    /// </summary>
    private static async Task SeedAsync()
    {
        await DropSchemaAsync();

        await using var store = DocumentStore.For(opts => ConfigureOptions(opts));

        var streamOne = Guid.NewGuid();
        var streamTwo = Guid.NewGuid();
        foreach (var @event in new object[]
                 {
                     new OrderPlaced(1), new OrderShipped(1), new OrderPlaced(2),
                     new OrderShipped(2), new OrderPlaced(3)
                 })
        {
            await using var session = store.LightweightSession();
            session.Events.Append(@event is OrderPlaced ? streamOne : streamTwo, @event);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Run the command the way the CLI runtime does — <c>Execute(input)</c> against a host built
    /// from the input's own HostBuilder — capturing stdout and parsing the JSON report.
    /// </summary>
    private static async Task<(bool Success, JsonElement Report)> ExecuteAsync(EventQueryInput input)
    {
        input.HostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts => ConfigureOptions(opts)));

        var original = Console.Out;
        var writer = new StringWriter();
        Console.SetOut(writer);
        bool success;
        try
        {
            success = await new EventQueryCommand().Execute(input);
        }
        finally
        {
            Console.SetOut(original);
        }

        var output = writer.ToString();
        var start = output.IndexOf('{');
        start.ShouldBeGreaterThanOrEqualTo(0, $"expected a JSON report on stdout, got: {output}");

        using var doc = JsonDocument.Parse(output[start..]);
        return (success, doc.RootElement.Clone());
    }

    [Fact]
    public async Task a_filtered_query_returns_the_exact_events_and_total()
    {
        await SeedAsync();

        var (success, report) = await ExecuteAsync(new EventQueryInput
        {
            EventTypeFlag = "order_placed",
            PageSizeFlag = 50
        });

        success.ShouldBeTrue();
        report.GetProperty("totalCount").GetInt32().ShouldBe(3);
        report.GetProperty("pageNumber").GetInt32().ShouldBe(1);
        report.GetProperty("pageSize").GetInt32().ShouldBe(50);
        report.GetProperty("hasMore").GetBoolean().ShouldBeFalse();
        report.TryGetProperty("error", out _).ShouldBeFalse();

        var events = report.GetProperty("events").EnumerateArray().ToList();
        events.Count.ShouldBe(3);
        events.ShouldAllBe(e => e.GetProperty("eventType").GetString() == "order_placed");

        // Exact membership in seed order — the filter demonstrably filtered (the two
        // order_shipped events are absent) and the ordering is the store-global sequence.
        events.Select(e => e.GetProperty("data").GetProperty("OrderId").GetInt32())
            .ShouldBe([1, 2, 3]);
        var sequences = events.Select(e => e.GetProperty("sequence").GetInt64()).ToList();
        sequences.ShouldBe(sequences.OrderBy(x => x).ToList());
        sequences.Distinct().Count().ShouldBe(3);
    }

    [Fact]
    public async Task a_filter_matching_nothing_is_a_real_answer_not_a_failure()
    {
        await SeedAsync();

        var (success, report) = await ExecuteAsync(new EventQueryInput
        {
            EventTypeFlag = "no_such_event_type",
            PageSizeFlag = 50
        });

        // totalCount 0 with no error, and a success return: "nothing matched" must stay
        // distinguishable from "the run failed".
        success.ShouldBeTrue();
        report.GetProperty("totalCount").GetInt32().ShouldBe(0);
        report.GetProperty("events").GetArrayLength().ShouldBe(0);
        report.TryGetProperty("error", out _).ShouldBeFalse();
    }

    [Fact]
    public async Task a_filter_the_store_does_not_capture_is_refused_by_name()
    {
        await SeedAsync();

        var (success, report) = await ExecuteAsync(new EventQueryInput
        {
            UserNameFlag = "whoever",
            PageSizeFlag = 50
        });

        // The jasperfx#737 guard rail surfaced through the CLI: the store does not capture
        // user_name, so the run FAILS with a report naming the refused field — it must never
        // return unfiltered events that read as filtered.
        success.ShouldBeFalse();
        var error = report.GetProperty("error").GetString();
        error.ShouldNotBeNull();
        error.ShouldContain("EventQuery.UserName");
        report.GetProperty("totalCount").GetInt32().ShouldBe(0);
        report.GetProperty("events").GetArrayLength().ShouldBe(0);
    }
}
