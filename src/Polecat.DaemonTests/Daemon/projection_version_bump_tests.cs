using JasperFx.Documents;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #1089: a projection's Version above 1 gets its own document table, <c>pc_doc_{type}_{version}</c>, as
///     Marten's does, so a blue/green bump rebuilds into an empty table instead of applying every event a second
///     time on top of the documents the old version wrote. Composite members take the composite's version, so a
///     composite bump does the same for every member.
/// </summary>
public class projection_version_bump_tests : OneOffConfigurationsContext
{
    private readonly string _schema = nameof(projection_version_bump_tests);

    [Fact]
    public async Task bumping_the_version_rebuilds_into_a_new_table_and_leaves_the_old_one_alone()
    {
        ConfigureStore(opts => opts.Projections.Add(new VersionBumpTallyProjection(), ProjectionLifecycle.Async));
        var blue = theStore;
        Disposables.Add(blue);

        var streamId = await AppendThreeIncrementsAsync(blue);
        await blue.WaitForProjectionAsync();
        (await CountInAsync(blue, streamId)).ShouldBe(3);

        ConfigureStore(opts =>
            opts.Projections.Add(new VersionBumpTallyProjection { Version = 2 }, ProjectionLifecycle.Async));
        var green = theStore;

        green.Options.Providers.GetProvider(typeof(VersionBumpTally)).Mapping.TableName
            .ShouldBe("pc_doc_versionbumptally_2");

        await green.WaitForProjectionAsync();

        // The new version built its own document from the three events...
        (await CountInAsync(green, streamId)).ShouldBe(3);

        // ...and the old version's table still holds what version 1 wrote.
        (await CountInAsync(blue, streamId)).ShouldBe(3);
        (await TableExistsAsync("pc_doc_versionbumptally")).ShouldBeTrue();
        (await TableExistsAsync("pc_doc_versionbumptally_2")).ShouldBeTrue();

        // The diagnostics report the versioned table's alias, as Marten's do.
        var types = await ((IDocumentStoreDiagnostics)green).DocumentTypesAsync(TestContext.Current.CancellationToken);
        types.Single(x => x.TypeName.EndsWith(nameof(VersionBumpTally))).Alias.ShouldBe("versionbumptally_2");
    }

    [Fact]
    public async Task bumping_a_composite_version_rebuilds_its_members_into_new_tables()
    {
        ConfigureStore(opts => opts.Projections.CompositeProjectionFor("VersionBumpComposite",
            composite => composite.Add(new CompositeVersionBumpTallyProjection())));
        var blue = theStore;
        Disposables.Add(blue);

        var streamId = await AppendThreeIncrementsAsync(blue);
        await blue.WaitForProjectionAsync();
        (await CompositeCountInAsync(blue, streamId)).ShouldBe(3);

        var member = new CompositeVersionBumpTallyProjection();
        ConfigureStore(opts => opts.Projections.CompositeProjectionFor("VersionBumpComposite", composite =>
        {
            composite.Version = 2;
            composite.Add(member);
        }));
        var green = theStore;

        // The member takes the composite's version, so it gets its own progression row and table.
        member.Version.ShouldBe(2u);
        green.Options.Providers.GetProvider(typeof(CompositeVersionBumpTally)).Mapping.TableName
            .ShouldBe("pc_doc_compositeversionbumptally_2");

        await green.WaitForProjectionAsync();

        (await CompositeCountInAsync(green, streamId)).ShouldBe(3);
        (await CompositeCountInAsync(blue, streamId)).ShouldBe(3);
    }

    [Fact]
    public void a_member_keeps_no_version_of_its_own_inside_a_composite()
    {
        var member = new CompositeVersionBumpTallyProjection { Version = 3 };
        ConfigureStore(opts => opts.Projections.CompositeProjectionFor("VersionBumpComposite",
            composite => composite.Add(member)));

        // As in Marten, the composite's version wins. A composite defaults to version 0, which names no suffix.
        member.Version.ShouldBe(0u);
        theStore.Options.Providers.GetProvider(typeof(CompositeVersionBumpTally)).Mapping.TableName
            .ShouldBe("pc_doc_compositeversionbumptally");
    }

    [Fact]
    public void version_one_keeps_the_plain_table_name()
    {
        ConfigureStore(opts => opts.Projections.Add(new VersionBumpTallyProjection(), ProjectionLifecycle.Async));

        theStore.Options.Providers.GetProvider(typeof(VersionBumpTally)).Mapping.TableName
            .ShouldBe("pc_doc_versionbumptally");
        theStore.Options.SchemaResolver.For(typeof(VersionBumpTally), qualified: false)
            .ShouldBe("pc_doc_versionbumptally");
    }

    [Fact]
    public void the_schema_resolver_names_the_versioned_table()
    {
        ConfigureStore(opts =>
            opts.Projections.Add(new VersionBumpTallyProjection { Version = 4 }, ProjectionLifecycle.Inline));

        theStore.Options.SchemaResolver.For(typeof(VersionBumpTally), qualified: false)
            .ShouldBe("pc_doc_versionbumptally_4");
    }

    private static async Task<Guid> AppendThreeIncrementsAsync(DocumentStore store)
    {
        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId, new VersionBumpIncrement(), new VersionBumpIncrement(),
            new VersionBumpIncrement());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return streamId;
    }

    private static async Task<int> CountInAsync(DocumentStore store, Guid streamId)
    {
        await using var query = store.QuerySession();
        var tally = await query.LoadAsync<VersionBumpTally>(streamId, TestContext.Current.CancellationToken);
        tally.ShouldNotBeNull();
        return tally.Count;
    }

    private static async Task<int> CompositeCountInAsync(DocumentStore store, Guid streamId)
    {
        await using var query = store.QuerySession();
        var tally = await query.LoadAsync<CompositeVersionBumpTally>(streamId,
            TestContext.Current.CancellationToken);
        tally.ShouldNotBeNull();
        return tally.Count;
    }

    private async Task<bool> TableExistsAsync(string table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                          "WHERE s.name = @schema AND t.name = @table";
        cmd.Parameters.AddWithValue("@schema", _schema);
        cmd.Parameters.AddWithValue("@table", table);
        return (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))! == 1;
    }
}

public record VersionBumpIncrement;

public class VersionBumpTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class VersionBumpTallyProjection : SingleStreamProjection<VersionBumpTally, Guid>
{
    public VersionBumpTally Create(VersionBumpIncrement _) => new() { Count = 1 };

    public void Apply(VersionBumpIncrement _, VersionBumpTally tally) => tally.Count++;
}

public class CompositeVersionBumpTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class CompositeVersionBumpTallyProjection : SingleStreamProjection<CompositeVersionBumpTally, Guid>
{
    public CompositeVersionBumpTally Create(VersionBumpIncrement _) => new() { Count = 1 };

    public void Apply(VersionBumpIncrement _, CompositeVersionBumpTally tally) => tally.Count++;
}
