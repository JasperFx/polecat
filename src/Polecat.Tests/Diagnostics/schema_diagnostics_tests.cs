using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Diagnostics;

[Collection("integration")]
public class schema_diagnostics_tests : IntegrationContext
{
    public schema_diagnostics_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void to_database_script_includes_event_store_tables()
    {
        var script = theStore.Advanced.ToDatabaseScript();

        script.ShouldContain("pc_streams");
        script.ShouldContain("pc_events");
        script.ShouldContain("pc_event_progression");
    }

    [Fact]
    public void to_database_script_includes_registered_document_tables()
    {
        // Trigger provider registration by accessing a document type
        _ = theStore.QuerySession();
        theStore.Options.Providers.GetProvider<User>();

        var script = theStore.Advanced.ToDatabaseScript();

        script.ShouldContain("pc_doc_user");
    }

    [Fact]
    public async Task write_creation_script_to_file()
    {
        var path = Path.Combine(Path.GetTempPath(), $"polecat_schema_{Guid.NewGuid()}.sql");

        try
        {
            await theStore.Advanced.WriteCreationScriptToFileAsync(path);

            File.Exists(path).ShouldBeTrue();
            var content = await File.ReadAllTextAsync(path);
            content.ShouldContain("pc_streams");
            content.ShouldContain("pc_events");
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
}
