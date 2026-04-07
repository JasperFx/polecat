using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

// FK test types
public class FkUser
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class FkIssue
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public Guid AssigneeId { get; set; }
}

public class FkComment
{
    public Guid Id { get; set; }
    public string Body { get; set; } = string.Empty;
    public Guid IssueId { get; set; }
    public Guid AuthorId { get; set; }
}

[Collection("integration")]
public class document_foreign_key_tests : IntegrationContext
{
    public document_foreign_key_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task create_foreign_key_constraint()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_basic";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId);
        });

        // Store the referenced user first
        var user = new FkUser { Id = Guid.NewGuid(), Name = "Alice" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        // Store the issue referencing the user
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Bug #1", AssigneeId = user.Id };
        theSession.Store(issue);
        await theSession.SaveChangesAsync();

        // Verify FK constraint exists
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.foreign_keys
            WHERE name = 'fk_pc_doc_fkissue_cc_assigneeid'
              AND parent_object_id = OBJECT_ID('[fk_basic].[pc_doc_fkissue]')
              AND referenced_object_id = OBJECT_ID('[fk_basic].[pc_doc_fkuser]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task foreign_key_rejects_invalid_reference()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_reject";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId);
        });

        // Ensure both tables exist
        var user = new FkUser { Id = Guid.NewGuid(), Name = "Setup" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        // Try to insert issue with non-existent user ID
        await using var session2 = theStore.LightweightSession();
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Bad Ref", AssigneeId = Guid.NewGuid() };
        session2.Store(issue);

        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.SaveChangesAsync();
        });
    }

    [Fact]
    public async Task foreign_key_with_cascade_delete()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_cascade";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId, fk => fk.OnDelete = CascadeAction.Cascade);
        });

        var user = new FkUser { Id = Guid.NewGuid(), Name = "Bob" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Cascaded", AssigneeId = user.Id };
        session2.Store(issue);
        await session2.SaveChangesAsync();

        // Verify CASCADE is set on the FK
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT delete_referential_action FROM sys.foreign_keys
            WHERE name = 'fk_pc_doc_fkissue_cc_assigneeid'
              AND parent_object_id = OBJECT_ID('[fk_cascade].[pc_doc_fkissue]')
            """;
        var action = (byte)(await cmd.ExecuteScalarAsync())!;
        // 1 = CASCADE
        action.ShouldBe((byte)1);
    }

    [Fact]
    public async Task multiple_foreign_keys_on_same_type()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_multi";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId);
            opts.Schema.For<FkComment>()
                .ForeignKey<FkIssue>(x => x.IssueId)
                .ForeignKey<FkUser>(x => x.AuthorId);
        });

        var user = new FkUser { Id = Guid.NewGuid(), Name = "Charlie" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Multi FK", AssigneeId = user.Id };
        session2.Store(issue);
        await session2.SaveChangesAsync();

        await using var session3 = theStore.LightweightSession();
        var comment = new FkComment { Id = Guid.NewGuid(), Body = "Nice!", IssueId = issue.Id, AuthorId = user.Id };
        session3.Store(comment);
        await session3.SaveChangesAsync();

        // Verify 2 FK constraints on FkComment
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.foreign_keys
            WHERE parent_object_id = OBJECT_ID('[fk_multi].[pc_doc_fkcomment]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(2);
    }

    [Fact]
    public async Task foreign_key_with_custom_constraint_name()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_custom_name";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId, fk => fk.ConstraintName = "my_custom_fk");
        });

        var user = new FkUser { Id = Guid.NewGuid(), Name = "Eve" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Custom Name", AssigneeId = user.Id };
        session2.Store(issue);
        await session2.SaveChangesAsync();

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.foreign_keys
            WHERE name = 'my_custom_fk'
              AND parent_object_id = OBJECT_ID('[fk_custom_name].[pc_doc_fkissue]')
            """;
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }

    [Fact]
    public async Task foreign_key_is_idempotent()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "fk_idempotent";
            opts.Schema.For<FkUser>();
            opts.Schema.For<FkIssue>()
                .ForeignKey<FkUser>(x => x.AssigneeId);
        });

        var user = new FkUser { Id = Guid.NewGuid(), Name = "Setup" };
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        // Create second store with same config — should not fail
        var opts2 = new StoreOptions
        {
            ConnectionString = theStore.Options.ConnectionString,
            AutoCreateSchemaObjects = JasperFx.AutoCreate.All,
            DatabaseSchemaName = "fk_idempotent",
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };
        opts2.Schema.For<FkUser>();
        opts2.Schema.For<FkIssue>().ForeignKey<FkUser>(x => x.AssigneeId);
        using var store2 = new DocumentStore(opts2);
        await using var session2 = store2.LightweightSession();
        var issue = new FkIssue { Id = Guid.NewGuid(), Title = "Idempotent", AssigneeId = user.Id };
        session2.Store(issue);
        await session2.SaveChangesAsync();
    }
}
