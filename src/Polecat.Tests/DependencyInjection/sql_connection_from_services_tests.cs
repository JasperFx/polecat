using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace Polecat.Tests.DependencyInjection;

// polecat#810: Aspire's AddSqlServerClient registers a SqlConnection; the store takes its connection string.
public class sql_connection_from_services_tests
{
    private const string Cs = "Server=tcp:somewhere,1433;Initial Catalog=aspire_db;User Id=u;Password=p;Encrypt=False";

    [Fact]
    public void connection_overload_reads_the_connection_string_without_opening()
    {
        using var connection = new SqlConnection(Cs);
        var options = new StoreOptions();

        options.Connection(connection);

        options.ConnectionString.ShouldBe(Cs);
    }

    [Fact]
    public void store_options_take_the_registered_sql_connection()
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => new SqlConnection(Cs));
        services.AddPolecat(_ => { }).UseSqlConnectionFromServices();

        using var provider = services.BuildServiceProvider();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();

        store.Options.ConnectionString.ShouldBe(Cs);
    }

    [Fact]
    public void keyed_connection_is_resolved_by_key()
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton("orders", (_, _) => new SqlConnection(Cs));
        services.AddPolecat(_ => { }).UseSqlConnectionFromServices("orders");

        using var provider = services.BuildServiceProvider();
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();

        store.Options.ConnectionString.ShouldBe(Cs);
    }
}
