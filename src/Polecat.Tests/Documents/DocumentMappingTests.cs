using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

public class DocumentMappingTests
{
    private static StoreOptions DefaultOptions() => new()
    {
        ConnectionString = "Server=fake;Database=fake;TrustServerCertificate=true"
    };

    [Fact]
    public void discovers_guid_id_property()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        mapping.IdType.ShouldBe(typeof(Guid));
    }

    [Fact]
    public void discovers_string_id_property()
    {
        var mapping = new DocumentMapping(typeof(StringDoc), DefaultOptions());
        mapping.IdType.ShouldBe(typeof(string));
    }

    [Fact]
    public void generates_table_name_from_type()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        mapping.TableName.ShouldBe("pc_doc_user");
    }

    [Fact]
    public void qualified_table_name_includes_schema()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        mapping.QualifiedTableName.ShouldBe("[dbo].[pc_doc_user]");
    }

    [Fact]
    public void qualified_table_name_respects_custom_schema()
    {
        var options = DefaultOptions();
        options.DatabaseSchemaName = "myschema";
        var mapping = new DocumentMapping(typeof(User), options);
        mapping.QualifiedTableName.ShouldBe("[myschema].[pc_doc_user]");
    }

    [Fact]
    public void can_get_guid_id()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        var id = Guid.NewGuid();
        var user = new User { Id = id, FirstName = "Test" };
        mapping.GetId(user).ShouldBe(id);
    }

    [Fact]
    public void can_get_string_id()
    {
        var mapping = new DocumentMapping(typeof(StringDoc), DefaultOptions());
        var doc = new StringDoc { Id = "my-key", Name = "Test" };
        mapping.GetId(doc).ShouldBe("my-key");
    }

    [Fact]
    public void can_set_guid_id()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        var user = new User();
        var id = Guid.NewGuid();
        mapping.SetId(user, id);
        user.Id.ShouldBe(id);
    }

    [Fact]
    public void can_set_string_id()
    {
        var mapping = new DocumentMapping(typeof(StringDoc), DefaultOptions());
        var doc = new StringDoc();
        mapping.SetId(doc, "new-key");
        doc.Id.ShouldBe("new-key");
    }

    [Fact]
    public void throws_for_type_without_id_property()
    {
        Should.Throw<InvalidOperationException>(() =>
            new DocumentMapping(typeof(NoIdDoc), DefaultOptions()));
    }

    [Fact]
    public void throws_for_unsupported_id_type()
    {
        Should.Throw<InvalidOperationException>(() =>
            new DocumentMapping(typeof(DateTimeIdDoc), DefaultOptions()));
    }

    [Fact]
    public void discovers_int_id_property()
    {
        var mapping = new DocumentMapping(typeof(IntIdDoc), DefaultOptions());
        mapping.IdType.ShouldBe(typeof(int));
        mapping.IsNumericId.ShouldBeTrue();
    }

    [Fact]
    public void discovers_long_id_property()
    {
        var mapping = new DocumentMapping(typeof(LongIdDoc), DefaultOptions());
        mapping.IdType.ShouldBe(typeof(long));
        mapping.IsNumericId.ShouldBeTrue();
    }

    [Fact]
    public void dotnet_type_name_is_assembly_qualified()
    {
        var mapping = new DocumentMapping(typeof(User), DefaultOptions());
        mapping.DotNetTypeName.ShouldContain("Polecat.Tests.Harness.User");
        mapping.DotNetTypeName.ShouldContain("Polecat.Tests");
    }

    // Test types
    private class NoIdDoc
    {
        public string Name { get; set; } = string.Empty;
    }

    private class IntIdDoc
    {
        public int Id { get; set; }
    }

    private class LongIdDoc
    {
        public long Id { get; set; }
    }

    private class DateTimeIdDoc
    {
        public DateTime Id { get; set; }
    }
}
