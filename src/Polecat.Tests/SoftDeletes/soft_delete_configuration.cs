using Polecat.Attributes;
using Polecat.Metadata;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.SoftDeletes;

public class soft_delete_configuration
{
    [Fact]
    public void soft_deleted_attribute_sets_delete_style()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        var mapping = new DocumentMapping(typeof(SoftDeletedDoc), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.SoftDelete);
    }

    [Fact]
    public void isoft_deleted_interface_sets_delete_style()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        var mapping = new DocumentMapping(typeof(SoftDeletedWithInterface), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.SoftDelete);
    }

    [Fact]
    public void global_policy_sets_delete_style()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        options.Policies.AllDocumentsSoftDeleted();
        var mapping = new DocumentMapping(typeof(User), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.SoftDelete);
    }

    [Fact]
    public void per_type_policy_sets_delete_style()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        options.Policies.ForDocument<User>(x => x.SoftDeleted = true);
        var mapping = new DocumentMapping(typeof(User), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.SoftDelete);
    }

    [Fact]
    public void default_delete_style_is_remove()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        var mapping = new DocumentMapping(typeof(User), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.Remove);
    }

    [Fact]
    public void per_type_policy_does_not_affect_other_types()
    {
        var options = new StoreOptions { ConnectionString = "Server=localhost" };
        options.Policies.ForDocument<User>(x => x.SoftDeleted = true);
        var mapping = new DocumentMapping(typeof(Target), options);

        mapping.DeleteStyle.ShouldBe(DeleteStyle.Remove);
    }
}
