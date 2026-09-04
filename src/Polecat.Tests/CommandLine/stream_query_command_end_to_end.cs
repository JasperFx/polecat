using System.Text.Json;
using JasperFx;
using JasperFx.Events.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.CommandLine;

#region CLI stream query aggregate

public record CliShipLaunched(string Name);

public record CliShipDocked(string Port);

/// <summary>
/// Top-level and uniquely named on purpose: <c>StreamQueryInput.ResolveAggregateType</c> scans the
/// loaded assemblies by simple name, so a nested or generic type would not resolve and a common
/// name could be ambiguous across test assemblies.
/// </summary>
public partial class CliCargoShip
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Dockings { get; set; }

    public static CliCargoShip Create(CliShipLaunched e) => new() { Name = e.Name };

    public void Apply(CliShipDocked _) => Dockings++;
}

#endregion

/// <summary>
/// #534 (jasperfx#740): end-to-end coverage of the <c>stream-query</c> CLI command against a REAL
/// Polecat store, the pattern the event-query wave established (see
/// <c>event_query_command_end_to_end</c>) — a real <see cref="StreamQueryInput"/> with its
/// <c>HostBuilder</c> set, <see cref="StreamQueryCommand.Execute"/> driven directly, stdout
/// captured and the JSON report parsed. The happy path pins the compaction-policy question the
/// command exists to answer — aggregate type + un-compacted growth — including the exact
/// <c>versionsSinceCompaction</c> arithmetic off the watermark CompactStreamAsync wrote; the two
/// honesty facts pin the truthful empty answer and the tenant refusal naming the problem.
/// </summary>
public class stream_query_command_end_to_end
{
    private const string Schema = "stream_query_cli";

    private static StoreOptions ConfigureOptions(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        // Deliberately NOT conjoined — the tenant-refusal fact depends on it.
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
    /// Three streams: an overgrown compacted ship (version 9, compacted through 5 → growth 4), a
    /// small never-compacted ship (version 2, growth 2), and an untyped stream — the decoy for a
    /// dropped aggregate-type filter.
    /// </summary>
    private static async Task<(Guid Overgrown, Guid Small)> SeedAsync()
    {
        await DropSchemaAsync();

        await using var store = DocumentStore.For(opts => ConfigureOptions(opts));

        var overgrown = Guid.NewGuid();
        var small = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            var events = new object[] { new CliShipLaunched("Overgrown") }
                .Concat(Enumerable.Range(0, 8).Select(object (i) => new CliShipDocked($"Port {i}")))
                .ToArray();
            session.Events.StartStream<CliCargoShip>(overgrown, events);
            session.Events.StartStream<CliCargoShip>(small, new CliShipLaunched("Small"), new CliShipDocked("Kiel"));
            session.Events.StartStream(Guid.NewGuid(), new CliShipLaunched("Untyped"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            await session.Events.CompactStreamAsync<CliCargoShip>(overgrown, x => x.Version = 5);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        return (overgrown, small);
    }

    private static async Task<(bool Success, JsonElement Report)> ExecuteAsync(StreamQueryInput input)
    {
        input.HostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts => ConfigureOptions(opts)));

        var original = Console.Out;
        var writer = new StringWriter();
        Console.SetOut(writer);
        bool success;
        try
        {
            success = await new StreamQueryCommand().Execute(input);
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
    public async Task the_compaction_policy_question_answers_with_exact_streams()
    {
        var (overgrown, _) = await SeedAsync();

        var (success, report) = await ExecuteAsync(new StreamQueryInput
        {
            AggregateTypeFlag = nameof(CliCargoShip),
            VersionAboveCompactedFlag = 3
        });

        // The small ship (growth 2) fails the growth filter, the untyped stream fails the type
        // filter — only the overgrown ship answers, with the watermark arithmetic exact.
        success.ShouldBeTrue(report.ToString());
        report.GetProperty("totalCount").GetInt32().ShouldBe(1);
        report.GetProperty("hasMore").GetBoolean().ShouldBeFalse();
        report.TryGetProperty("error", out _).ShouldBeFalse();

        var stream = report.GetProperty("streams").EnumerateArray().ShouldHaveSingleItem();
        stream.GetProperty("streamId").GetString().ShouldBe(overgrown.ToString());
        stream.GetProperty("version").GetInt64().ShouldBe(9);
        stream.GetProperty("compactedVersion").GetInt64().ShouldBe(5);
        stream.GetProperty("versionsSinceCompaction").GetInt64().ShouldBe(4);
        stream.GetProperty("aggregateType").GetString().ShouldBe(typeof(CliCargoShip).FullName);
        stream.GetProperty("isArchived").GetBoolean().ShouldBeFalse();
    }

    [Fact]
    public async Task a_filter_matching_nothing_is_a_real_answer_not_a_failure()
    {
        await SeedAsync();

        var (success, report) = await ExecuteAsync(new StreamQueryInput
        {
            AggregateTypeFlag = nameof(CliCargoShip),
            MinVersionFlag = 1000
        });

        success.ShouldBeTrue(report.ToString());
        report.GetProperty("totalCount").GetInt32().ShouldBe(0);
        report.GetProperty("streams").GetArrayLength().ShouldBe(0);
        report.TryGetProperty("error", out _).ShouldBeFalse();
    }

    [Fact]
    public async Task a_tenant_on_a_tenantless_store_is_refused_by_the_report()
    {
        await SeedAsync();

        var (success, report) = await ExecuteAsync(new StreamQueryInput
        {
            TenantFlag = "tenant-a"
        });

        // The jasperfx#740 refusal surfaced through the CLI: this store has no tenant dimension,
        // so the run FAILS with a report saying so — never unscoped rows dressed as a tenant's.
        success.ShouldBeFalse();
        var error = report.GetProperty("error").GetString();
        error.ShouldNotBeNull();
        error.ShouldContain("tenant");
        report.GetProperty("streams").GetArrayLength().ShouldBe(0);
    }
}
