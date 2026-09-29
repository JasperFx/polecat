using JasperFx.Events.Daemon;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events;
using Polecat.Events.Daemon;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     The high-water detector's database identity. It used to compose its own URI from the connection
///     string, which threw <see cref="UriFormatException" /> out of the CONSTRUCTOR for server names
///     ordinary SQL Server deployments use — taking the async daemon down at startup rather than at
///     first use. jasperfx#918 is the same bug upstream, and its history is the reason this delegates
///     to <c>DatabaseDescriptor.DatabaseUri()</c> instead of adding the missing characters: that method
///     had already been patched three times, once per character class somebody hit in production.
/// </summary>
public class high_water_detector_database_uri_tests
{
    private static Uri UriFor(string dataSource)
    {
        var store = DocumentStore.For(opts => opts.ConnectionString = ConnectionSource.ConnectionString);
        var detector = new PolecatHighWaterDetector(
            store.Options.EventGraph,
            $"Server={dataSource};Initial Catalog=appdb;User Id=sa;Password=x;Encrypt=False",
            store.Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance,
            store.Options.ResiliencePipeline);
        return detector.DatabaseUri;
    }

    /// <summary>
    ///     The four shapes measured as throwing before the fix. A named instance is the common one —
    ///     any on-premises SQL Server installed as anything other than the default instance.
    /// </summary>
    [Theory]
    [InlineData(@"db-host\MSSQL2017")]
    [InlineData(@".\SQLEXPRESS")]
    [InlineData(@"(localdb)\MSSQLLocalDB")]
    [InlineData("tcp:my.server.net,1433")]
    public void a_server_name_that_is_not_a_bare_hostname_still_yields_a_uri(string dataSource)
    {
        var uri = Should.NotThrow(() => UriFor(dataSource));
        uri.ShouldNotBeNull();
        uri.Scheme.ShouldBe("sqlserver");
        uri.ToString().ShouldContain("appdb", Case.Insensitive);
    }

    /// <summary>
    ///     Distinct server names must not collapse onto one identity. DatabaseUri is load-bearing as an
    ///     identity — agent URIs, database ids — so a sanitizer that mapped every awkward name to the
    ///     same string would trade a startup crash for two databases silently sharing a name.
    /// </summary>
    [Fact]
    public void different_named_instances_keep_different_identities()
    {
        UriFor(@"db-host\MSSQL2017").ShouldNotBe(UriFor(@"db-host\MSSQL2019"));
    }

    /// <summary>
    ///     The ordinary case is unchanged. The bar on this fix is that nothing already deployed gets
    ///     renamed: a server name whose characters were all legal reached the host untouched before and
    ///     has to still.
    /// </summary>
    [Fact]
    public void an_ordinary_host_and_port_is_unchanged()
    {
        var uri = UriFor("localhost,11433");
        uri.Host.ShouldBe("localhost");
        uri.ToString().ShouldContain("appdb", Case.Insensitive);
    }
}
