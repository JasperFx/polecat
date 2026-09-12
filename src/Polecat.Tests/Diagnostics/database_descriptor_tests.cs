using JasperFx.Descriptors;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.SqlServer;

namespace Polecat.Tests.Diagnostics;

/// <summary>
///     #583 — what <c>PolecatDatabase.Describe()</c> reports, which is what JasperFx's <c>db-list</c>
///     command renders and what monitoring tools key on.
/// </summary>
/// <remarks>
///     <para>
///         There was no coverage of this method at all, which is how two properties stayed at their
///         <see cref="DatabaseDescriptor" /> defaults through several releases: <c>SubjectUri</c> was
///         reported as the literal <c>database://unknown</c>, and <c>SchemaOrNamespace</c> was empty,
///         which silently dropped the schema segment from <c>DatabaseUri()</c>.
///     </para>
///     <para>
///         Both are assertions about the CONFIGURED values rather than about any constant, so a
///         descriptor that hardcoded an answer would not satisfy them.
///     </para>
/// </remarks>
public class database_descriptor_tests
{
    private static PolecatDatabase DatabaseFor(string schemaName) =>
        new(new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            DatabaseSchemaName = schemaName
        });

    [Fact]
    public void reports_the_polecat_store_as_its_subject()
    {
        var descriptor = DatabaseFor("dbo").Describe();

        // Was "database://unknown" — a Polecat store that db-list attributed to nothing, where the
        // equivalent Marten app reports "marten://store".
        descriptor.SubjectUri.ShouldBe(PolecatSystemPart.PolecatStoreUri);
        descriptor.SubjectUri.ToString().ShouldStartWith("polecat://");
    }

    [Fact]
    public void reports_the_configured_schema_as_its_namespace()
    {
        // Not "dbo": the default would pass against an implementation that hardcoded one.
        DatabaseFor("descriptor_schema").Describe()
            .SchemaOrNamespace.ShouldBe("descriptor_schema");
    }

    [Fact]
    public void the_database_uri_carries_the_schema_segment()
    {
        var uri = DatabaseFor("descriptor_schema").Describe().DatabaseUri().ToString();

        // DatabaseUri() drops empty segments, so an unset SchemaOrNamespace rendered
        // "sqlserver://localhost/" while the Wolverine database beside it in the same db-list table
        // rendered "sqlserver://localhost/dbo".
        uri.ShouldEndWith("/descriptor_schema");
    }

    [Fact]
    public void still_reports_the_engine_and_server_it_always_did()
    {
        var descriptor = DatabaseFor("dbo").Describe();

        descriptor.Engine.ShouldBe(SqlServerProvider.EngineName);
        descriptor.ServerName.ShouldNotBeEmpty();
    }
}
