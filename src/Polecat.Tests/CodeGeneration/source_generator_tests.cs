using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Polecat.CodeGeneration;

namespace Polecat.Tests.CodeGeneration;

public class source_generator_tests
{
    private static GeneratorDriverRunResult RunGenerator(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source);

        // Reference assemblies needed for compilation
        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Attribute).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Polecat.Attributes.DocumentAttribute).Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location)
        };

        var compilation = CSharpCompilation.Create(
            "TestAssembly",
            new[] { syntaxTree },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new PolecatGenerator();
        var driver = CSharpGeneratorDriver.Create(generator);

        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(
            compilation, out var outputCompilation, out var diagnostics);

        return driver.GetRunResult();
    }

    [Fact]
    public void generator_finds_document_attribute_types()
    {
        var source = @"
using Polecat.Attributes;

namespace TestApp
{
    [Document]
    public class Invoice
    {
        public System.Guid Id { get; set; }
        public string Number { get; set; }
    }
}";

        var result = RunGenerator(source);

        result.GeneratedTrees.Length.ShouldBe(1);
        result.Results[0].GeneratedSources.Length.ShouldBe(1);
        result.Results[0].GeneratedSources[0].HintName.ShouldBe("InvoiceDocumentProvider.g.cs");
    }

    [Fact]
    public void generator_emits_valid_csharp()
    {
        var source = @"
using Polecat.Attributes;

namespace TestApp
{
    [Document]
    public class Customer
    {
        public System.Guid Id { get; set; }
        public string Name { get; set; }
    }
}";

        var result = RunGenerator(source);

        var generatedSource = result.Results[0].GeneratedSources[0].SourceText.ToString();

        // Should contain the class declaration
        generatedSource.ShouldContain("public static class CustomerDocumentProvider");

        // Should contain the namespace
        generatedSource.ShouldContain("namespace TestApp;");

        // Should contain all expected SQL members
        generatedSource.ShouldContain("TableName");
        generatedSource.ShouldContain("SelectSql");
        generatedSource.ShouldContain("LoadSql");
        generatedSource.ShouldContain("InsertSql");
        generatedSource.ShouldContain("UpsertSql");
        generatedSource.ShouldContain("DeleteSql");

        // Should use the correct table name
        generatedSource.ShouldContain("pc_doc_customer");

        // Should use uniqueidentifier for Guid id type
        generatedSource.ShouldContain("uniqueidentifier");
    }

    [Fact]
    public void generated_sql_matches_expected_patterns()
    {
        var source = @"
using Polecat.Attributes;

namespace TestApp
{
    [Document]
    public class Order
    {
        public System.Guid Id { get; set; }
        public decimal Total { get; set; }
    }
}";

        var result = RunGenerator(source);
        var generatedSource = result.Results[0].GeneratedSources[0].SourceText.ToString();

        // Select SQL should query all standard columns
        generatedSource.ShouldContain(
            "SELECT id, data, version, last_modified, dotnet_type, tenant_id FROM [dbo].[pc_doc_order]");

        // Load SQL should filter by id and tenant_id
        generatedSource.ShouldContain("WHERE id = @id AND tenant_id = @tenant_id");

        // Insert SQL should use VALUES with version=1
        generatedSource.ShouldContain("INSERT INTO [dbo].[pc_doc_order]");
        generatedSource.ShouldContain("VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id)");

        // Upsert SQL should use MERGE
        generatedSource.ShouldContain("MERGE [dbo].[pc_doc_order]");
        generatedSource.ShouldContain("WHEN MATCHED THEN UPDATE");
        generatedSource.ShouldContain("WHEN NOT MATCHED THEN INSERT");

        // Delete SQL should filter by id and tenant_id
        generatedSource.ShouldContain("DELETE FROM [dbo].[pc_doc_order] WHERE id = @id AND tenant_id = @tenant_id");
    }

    [Fact]
    public void generator_handles_string_id_type()
    {
        var source = @"
using Polecat.Attributes;

namespace TestApp
{
    [Document]
    public class Tag
    {
        public string Id { get; set; }
        public string Label { get; set; }
    }
}";

        var result = RunGenerator(source);

        result.GeneratedTrees.Length.ShouldBe(1);

        var generatedSource = result.Results[0].GeneratedSources[0].SourceText.ToString();
        generatedSource.ShouldContain("varchar(250)");
        generatedSource.ShouldNotContain("uniqueidentifier");
    }

    [Fact]
    public void generator_ignores_classes_without_document_attribute()
    {
        var source = @"
namespace TestApp
{
    public class NotADocument
    {
        public System.Guid Id { get; set; }
    }
}";

        var result = RunGenerator(source);

        result.GeneratedTrees.Length.ShouldBe(0);
    }

    [Fact]
    public void generator_ignores_classes_without_id_property()
    {
        var source = @"
using Polecat.Attributes;

namespace TestApp
{
    [Document]
    public class NoIdClass
    {
        public string Name { get; set; }
    }
}";

        var result = RunGenerator(source);

        // Should not emit anything for a class without an Id property
        result.GeneratedTrees.Length.ShouldBe(0);
    }
}
