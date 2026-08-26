using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Patching;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Patching;

/// <summary>
///     #503. Follow-up to marten#5290 and marten#5295, neither of which is a bug here — this pins
///     the property rather than fixing anything.
///     Marten writes <c>.Duplicate()</c> columns client-side during serialization, so a patch (which
///     is server-side SQL) has to refresh them explicitly, and marten#5295 was the several ways that
///     matching rule got it wrong: an aliased member never matched, the overlap test ran only one
///     way, and some operations' targets were never collected at all. Polecat's searchable columns
///     are SQL Server <c>AS &lt;expr&gt; PERSISTED</c> computed columns derived by the database from
///     <c>data</c>, so a patch that moves <c>data</c> moves the column with it — there is no
///     client-side write path to keep in step and therefore no matching rule to get wrong.
///     Each case gets its OWN document type on purpose. Sharing one type across the class means only
///     the first test's <c>Index(...)</c> is ever created, and the rest quietly fall back to
///     scanning <c>data</c> — passing while testing nothing, which is the exact failure mode this
///     file exists to guard against.
/// </summary>
public class patching_with_computed_column_index : OneOffConfigurationsContext
{
    public class SimpleIndexed
    {
        public Guid Id { get; set; }
        public string ServiceName { get; set; } = string.Empty;
    }

    public class AliasedIndexed
    {
        public Guid Id { get; set; }

        // marten#5290 patched an aliased member at a path the serializer never reads; marten#5295
        // then failed to match that member against its duplicated column.
        [JsonPropertyName("bucket_label")]
        public string Label { get; set; } = string.Empty;
    }

    public class NumericIndexed
    {
        public Guid Id { get; set; }
        public int Count { get; set; }
    }

    public class NestedIndexed
    {
        public Guid Id { get; set; }
        public Inner Nested { get; set; } = new();
    }

    public class Inner
    {
        public string Region { get; set; } = string.Empty;
    }

    [Fact]
    public async Task patch_keeps_an_indexed_column_consistent_with_the_document()
    {
        ConfigureStore(opts => opts.Schema.For<SimpleIndexed>().Index(x => x.ServiceName));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SimpleIndexed { Id = id, ServiceName = "before" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Checked after the seed, because the table is created lazily on first write.
        await AssertColumnExistsAsync("pc_doc_simpleindexed", "cc_servicename");

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<SimpleIndexed>(id).Set(x => x.ServiceName, "after");
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();

        // Through a predicate, not a reload: a reload reads data and so cannot see a divergence
        // between data and the computed column.
        var found = await query.Query<SimpleIndexed>()
            .Where(x => x.ServiceName == "after")
            .ToListAsync(TestContext.Current.CancellationToken);
        found.Count.ShouldBe(1);
        found[0].Id.ShouldBe(id);

        (await query.Query<SimpleIndexed>()
            .Where(x => x.ServiceName == "before")
            .CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);

        // Sharper still: read the persisted column itself. A LINQ predicate can be satisfied by an
        // expression evaluated against data rather than the column, so it does not on its own prove
        // the column moved. This does.
        (await ReadColumnAsync("pc_doc_simpleindexed", "cc_servicename", id)).ShouldBe("after");
    }

    [Fact(Skip = "polecat#507: Index() builds its path from the CLR member name, so an aliased " +
                 "member's computed column is permanently NULL. Un-skip with that fix — the test " +
                 "is correct as written and fails on the column-existence assertion.")]
    public async Task patch_keeps_an_indexed_aliased_member_consistent()
    {
        ConfigureStore(opts => opts.Schema.For<AliasedIndexed>().Index(x => x.Label));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new AliasedIndexed { Id = id, Label = "before" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The column is derived from the SERIALIZED name. If Index(...) had built its path from
        // the CLR member the way marten#5290's toPath did, this would be cc_label and the read
        // below would be looking at something the serializer never writes.
        await AssertColumnExistsAsync("pc_doc_aliasedindexed", "cc_bucket_label");

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<AliasedIndexed>(id).Set(x => x.Label, "after");
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();

        var found = await query.Query<AliasedIndexed>()
            .Where(x => x.Label == "after")
            .ToListAsync(TestContext.Current.CancellationToken);
        found.Count.ShouldBe(1);
        found[0].Id.ShouldBe(id);

        // If the patch had written the CLR-named path, the document would carry a phantom node and
        // this column would still read "before".
        (await ReadColumnAsync("pc_doc_aliasedindexed", "cc_bucket_label", id)).ShouldBe("after");

        (await query.LoadAsync<AliasedIndexed>(id, TestContext.Current.CancellationToken))!
            .Label.ShouldBe("after");
    }

    [Fact]
    public async Task incrementing_a_patch_keeps_a_numeric_indexed_column_consistent()
    {
        // Increment is server-side arithmetic on data rather than a Set of a known literal, so it is
        // the operation with the least opportunity for a client-side column write to sneak in.
        ConfigureStore(opts => opts.Schema.For<NumericIndexed>().Index(x => x.Count));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new NumericIndexed { Id = id, Count = 5 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await AssertColumnExistsAsync("pc_doc_numericindexed", "cc_count");

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<NumericIndexed>(id).Increment(x => x.Count, 3);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<NumericIndexed>()
            .Where(x => x.Count == 8)
            .CountAsync(TestContext.Current.CancellationToken)).ShouldBe(1);

        (await ReadColumnAsync("pc_doc_numericindexed", "cc_count", id)).ShouldBe("8");
    }

    [Fact]
    public async Task patching_a_parent_path_keeps_a_child_indexed_column_consistent()
    {
        // marten#5295's parent-path gap: patching a parent did not refresh a column duplicated from
        // a child, because the overlap test only ran one way. Here the whole Nested node is replaced
        // while the index is on Nested.Region.
        ConfigureStore(opts => opts.Schema.For<NestedIndexed>().Index(x => x.Nested.Region));

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new NestedIndexed { Id = id, Nested = new Inner { Region = "before" } });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await AssertColumnExistsAsync("pc_doc_nestedindexed", "cc_nested_region");

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<NestedIndexed>(id).Set(x => x.Nested, new Inner { Region = "after" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();

        var found = await query.Query<NestedIndexed>()
            .Where(x => x.Nested.Region == "after")
            .ToListAsync(TestContext.Current.CancellationToken);
        found.Count.ShouldBe(1);
        found[0].Id.ShouldBe(id);

        (await query.Query<NestedIndexed>()
            .Where(x => x.Nested.Region == "before")
            .CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);

        // The patch replaced the parent node; the child's column has to have followed it.
        (await ReadColumnAsync("pc_doc_nestedindexed", "cc_nested_region", id)).ShouldBe("after");
    }

    /// <summary>
    ///     Fail loudly if the index never made it into the table. Without this a test whose computed
    ///     column is missing still passes — the LINQ translator falls back to reading <c>data</c>,
    ///     so every assertion above holds while nothing about the column is exercised.
    /// </summary>
    private async Task AssertColumnExistsAsync(string table, string column)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*) FROM sys.computed_columns
            WHERE object_id = OBJECT_ID('[{Schema}].[{table}]') AND name = @column;
            """;
        cmd.Parameters.AddWithValue("@column", column);

        var count = (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        count.ShouldBe(1, $"Expected a persisted computed column [{column}] on [{Schema}].[{table}]");
    }

    /// <summary>
    ///     Read a persisted computed column straight out of the table. This is what a reload cannot
    ///     do, and what makes the test a guard rather than a restatement of the reload.
    /// </summary>
    private async Task<string?> ReadColumnAsync(string table, string column, Guid id)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT CONVERT(varchar(250), [{column}]) FROM [{Schema}].[{table}] WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", id);

        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value == DBNull.Value ? null : (string?)value;
    }

    private string Schema => GetType().Name.ToLowerInvariant();
}
