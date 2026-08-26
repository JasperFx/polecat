using JasperFx.Descriptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;

namespace Polecat.Tests.DependencyInjection;

/// <summary>
///     #501: Weasel's <c>db-apply</c> / <c>db-assert</c> / <c>db-dump</c> resolve
///     <see cref="IDatabaseSource" /> out of the container, which Polecat never registered — so all
///     three failed with "No Weasel databases were registered in this application" on a stock
///     <c>AddPolecat</c> host, while <c>resources setup</c> worked. That reads as a misconfigured
///     host rather than an unsupported command.
///     These drive <see cref="WeaselInput" />'s real discovery path rather than merely asserting a
///     registration exists, because the registration is only worth having if that path finds it.
/// </summary>
public class database_source_registration_tests
{
    [Fact]
    public async Task weasel_cli_discovers_the_polecat_database()
    {
        using var host = BuildHost();

        // AllDatabases is what FilterDatabases calls, and FilterDatabases is what threw. Before the
        // fix this returned an empty list — the "Found 0 databases in 0.0s" from the report.
        var databases = await new WeaselInput().AllDatabases(host);

        databases.ShouldNotBeEmpty();

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        databases.ShouldContain(x => ReferenceEquals(x, store.Database));
    }

    [Fact]
    public async Task filter_databases_no_longer_throws()
    {
        using var host = BuildHost();

        // The exact call in the stack trace on the issue.
        var databases = await new WeaselInput().FilterDatabases(host);

        databases.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task describes_a_single_database_store_as_single_cardinality()
    {
        using var host = BuildHost();

        var source = host.Services.GetServices<IDatabaseSource>().OfType<PolecatDatabaseSource>().Single();

        source.Cardinality.ShouldBe(DatabaseCardinality.Single);

        var usage = await source.DescribeDatabasesAsync(TestContext.Current.CancellationToken);

        usage.Cardinality.ShouldBe(DatabaseCardinality.Single);
        usage.MainDatabase.ShouldNotBeNull();
    }

    private static IHost BuildHost()
    {
        return new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                {
                    opts.ConnectionString = ConnectionSource.ConnectionString;
                    opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                    opts.DatabaseSchemaName = "database_source_501";
                });
            })
            .Build();
    }
}
