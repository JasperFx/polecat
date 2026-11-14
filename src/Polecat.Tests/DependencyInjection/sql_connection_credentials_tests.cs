using System.Security;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.DependencyInjection;

// StoreOptions.Connection(SqlConnection) — and UseSqlConnectionFromServices(), which calls it — keep only
// the connection's ConnectionString. That string is not always one the store can log in with:
//
//   * SqlClient removes the password from SqlConnection.ConnectionString once the connection has been
//     opened, unless the string says Persist Security Info=True.
//   * A SqlCredential, AccessToken or AccessTokenCallback lives on the connection, never in the string.
//
// Polecat builds every connection it opens (and Weasel builds the migration ones) from the string alone,
// so in all four cases the store used to come up looking healthy and then fail its first query with
// "Login failed". It now refuses at configuration time and says why. Same failure shape as
// JasperFx/marten#5898 ("No password has been provided"), from a different cause.
public class sql_connection_credentials_tests
{
    private const string NoCredentials = "Server=tcp:somewhere,1433;Initial Catalog=aspire_db;Encrypt=False";

    public record CredentialProbeDoc(Guid Id);

    [Fact]
    public async Task a_connection_that_has_been_opened_is_refused_rather_than_failing_to_log_in()
    {
        var ct = TestContext.Current.CancellationToken;
        Assert.SkipWhen(string.IsNullOrEmpty(new SqlConnectionStringBuilder(ConnectionSource.ConnectionString).Password),
            "The test database is not reached with a SQL login and password.");

        await using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(ct);
        await connection.CloseAsync();

        // This is SqlClient's doing, not Polecat's: the string no longer carries the password.
        new SqlConnectionStringBuilder(connection.ConnectionString).Password.ShouldBeEmpty();

        var ex = await Should.ThrowAsync<ArgumentException>(async () =>
        {
            await using var store = DocumentStore.For(opts =>
            {
                opts.Connection(connection);
                opts.DatabaseSchemaName = "sql_connection_credentials";
            });

            await using var session = store.QuerySession();
            await session.Query<CredentialProbeDoc>().CountAsync(ct);
        });

        ex.Message.ShouldContain("Persist Security Info");
    }

    [Fact]
    public async Task an_opened_connection_that_persists_its_security_info_still_works()
    {
        var ct = TestContext.Current.CancellationToken;

        var cs = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { PersistSecurityInfo = true }
            .ConnectionString;

        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync(ct);
        await connection.CloseAsync();

        await using var store = DocumentStore.For(opts =>
        {
            opts.Connection(connection);
            opts.DatabaseSchemaName = "sql_connection_credentials";
        });

        await using var session = store.QuerySession();
        (await session.Query<CredentialProbeDoc>().CountAsync(ct)).ShouldBeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public void a_sql_credential_is_refused()
    {
        var password = new SecureString();
        foreach (var c in "p") password.AppendChar(c);
        password.MakeReadOnly();

        using var connection = new SqlConnection(NoCredentials, new SqlCredential("u", password));

        var ex = Should.Throw<ArgumentException>(() => new StoreOptions().Connection(connection));
        ex.Message.ShouldContain("SqlCredential");
    }

    [Fact]
    public void an_access_token_is_refused()
    {
        using var connection = new SqlConnection(NoCredentials) { AccessToken = "token" };

        var ex = Should.Throw<ArgumentException>(() => new StoreOptions().Connection(connection));
        ex.Message.ShouldContain("AccessToken");
    }

    [Fact]
    public void an_access_token_callback_is_refused()
    {
        using var connection = new SqlConnection(NoCredentials)
        {
            AccessTokenCallback = (_, _) => Task.FromResult(new SqlAuthenticationToken("token", DateTimeOffset.MaxValue))
        };

        var ex = Should.Throw<ArgumentException>(() => new StoreOptions().Connection(connection));
        ex.Message.ShouldContain("AccessTokenCallback");
    }

    [Theory]
    // Credentials the string itself carries, or that SqlClient acquires on its own from the string.
    [InlineData(NoCredentials + ";Integrated Security=True")]
    [InlineData(NoCredentials + ";Authentication=Active Directory Default")]
    [InlineData(NoCredentials + ";Authentication=Active Directory Managed Identity;User Id=client-id")]
    [InlineData(NoCredentials + ";User Id=u;Password=p")]
    public void a_connection_string_that_can_log_in_by_itself_is_taken(string cs)
    {
        using var connection = new SqlConnection(cs);
        var options = new StoreOptions();

        options.Connection(connection);

        options.ConnectionString.ShouldBe(cs);
    }

    [Theory]
    [InlineData(NoCredentials + ";User Id=u")]
    [InlineData(NoCredentials + ";User Id=u;Authentication=Sql Password")]
    [InlineData(NoCredentials + ";User Id=app-id;Authentication=Active Directory Service Principal")]
    public void a_user_id_without_the_password_it_needs_is_refused(string cs)
    {
        using var connection = new SqlConnection(cs);

        var ex = Should.Throw<ArgumentException>(() => new StoreOptions().Connection(connection));
        ex.Message.ShouldContain("Persist Security Info");
    }
}
