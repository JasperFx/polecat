using JasperFx.Core.Reflection;
using Polecat.Attributes;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     #1056, the port of marten#6019: a second Index() that resolves to the same index name used to be
///     accepted and then fail when the table was created or migrated, with an error that named only the
///     index. It now throws when the document is configured, and says which document, which member and
///     which index.
/// </summary>
public class duplicate_index_name_tests : OneOffConfigurationsContext
{
    private Exception configurationFailure(Action<StoreOptions> configure)
    {
        return Should.Throw<InvalidOperationException>(() =>
        {
            ConfigureStore(configure);
            theStore.Options.Providers.GetProvider<IndexedOrder>();
        });
    }

    [Fact]
    public void the_same_index_twice_throws()
    {
        var ex = configurationFailure(opts =>
        {
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail);
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail);
        });

        ex.Message.ShouldContain(typeof(IndexedOrder).FullNameInCode());
        ex.Message.ShouldContain("CustomerEmail");
        ex.Message.ShouldContain("ix_pc_doc_indexedorder_customeremail");
        ex.Message.ShouldContain("x.IndexName");
    }

    [Fact]
    public void an_index_with_another_indexs_name_throws()
    {
        var ex = configurationFailure(opts =>
        {
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail, x => x.IndexName = "ix_order_lookup");
            opts.Schema.For<IndexedOrder>().Index(x => x.Region, x => x.IndexName = "ix_order_lookup");
        });

        ex.Message.ShouldContain(typeof(IndexedOrder).FullNameInCode());
        ex.Message.ShouldContain("Region");
        ex.Message.ShouldContain("ix_order_lookup");
    }

    [Fact]
    public void an_index_over_a_member_already_indexed_by_attribute_throws()
    {
        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            ConfigureStore(opts => opts.Schema.For<AttributeIndexedOrder>().Index(x => x.CustomerEmail));
            theStore.Options.Providers.GetProvider<AttributeIndexedOrder>();
        });

        ex.Message.ShouldContain(typeof(AttributeIndexedOrder).FullNameInCode());
        ex.Message.ShouldContain("ix_pc_doc_attributeindexedorder_customeremail");
    }

    /// <summary>
    ///     Unlike Marten, Polecat's derived name carries the casing, so a lower-cased index over a member
    ///     that is already indexed is a second index and not a duplicate.
    /// </summary>
    [Fact]
    public async Task an_index_and_a_lower_cased_index_over_one_member_both_work()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail);
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail, x => x.Casing = IndexCasing.Lower);
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        (await IndexNamesAsync()).ShouldBe(
            ["ix_pc_doc_indexedorder_customeremail", "ix_pc_doc_indexedorder_customeremail_lower"],
            ignoreOrder: true);

        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task two_indexes_over_one_member_with_their_own_names_both_work()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail);
            opts.Schema.For<IndexedOrder>().Index(x => x.CustomerEmail, x =>
            {
                x.SortOrder = SortOrder.Descending;
                x.IndexName = "ix_indexedorder_customer_email_desc";
            });
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        (await IndexNamesAsync()).ShouldBe(
            ["ix_pc_doc_indexedorder_customeremail", "ix_indexedorder_customer_email_desc"],
            ignoreOrder: true);

        await theStore.Database.AssertDatabaseMatchesConfigurationAsync(TestContext.Current.CancellationToken);
    }

    private async Task<string[]> IndexNamesAsync()
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT i.name
            FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID('[{nameof(duplicate_index_name_tests).ToLowerInvariant()}].[pc_doc_indexedorder]')
              AND i.name LIKE 'ix[_]%'
            """);
        return rows.Select(x => (string)x[0]).ToArray();
    }

    public class IndexedOrder
    {
        public Guid Id { get; set; }
        public string CustomerEmail { get; set; } = "";
        public string Region { get; set; } = "";
    }

    public class AttributeIndexedOrder
    {
        public Guid Id { get; set; }

        [Index]
        public string CustomerEmail { get; set; } = "";
    }
}
