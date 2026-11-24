using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #1147 (jasperfx#684): a composite with <c>KeepUnchangedMembers</c> on, deployed again with its own version
///     bumped and one member's version bumped, replays only the member that changed. The other keeps its documents and
///     its progress.
/// </summary>
/// <remarks>
///     <c>CompositeKeepUnchangedMembersCompliance</c> simulates a bump by deleting the composite's progression row.
///     These facts make a real one, across two stores, which is the only way to see that the members keep versions of
///     their own: a composite member takes the composite's version (#1089), so without this a bump renamed every
///     member's progression row and document table, and nothing could ever be kept.
/// </remarks>
public class composite_keep_unchanged_members_tests : OneOffConfigurationsContext
{
    private const string CompositeName = "KeptMembersComposite";

    // A value no replay produces, so a member replayed over its documents reads 1002, and one torn down and
    // replayed reads 3
    private const int Marked = 999;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(uint compositeVersion, uint changingVersion, bool keepUnchangedMembers = true)
    {
        ConfigureStore(opts =>
        {
            #region sample_polecat_composite_keep_unchanged_members

            opts.Projections.CompositeProjectionFor(CompositeName, composite =>
            {
                // Bump the composite's version and the version of each member you changed...
                composite.Version = compositeVersion;
                composite.Add(new KeptStableTallyProjection());
                composite.Add(new KeptChangingTallyProjection { Version = changingVersion });

                // ...and the members whose version didn't change keep their documents and their progress
                composite.KeepUnchangedMembers = keepUnchangedMembers;
            });

            #endregion
        });
    }

    private async Task<DocumentStore> deployVersionOneAsync(Guid streamId)
    {
        configure(compositeVersion: 1, changingVersion: 1);
        var blue = theStore;
        Disposables.Add(blue);

        await using (var session = blue.LightweightSession())
        {
            session.Events.StartStream(streamId, new KeptIncrement(), new KeptIncrement(), new KeptIncrement());
            await session.SaveChangesAsync(Ct);
        }

        await blue.WaitForProjectionAsync();

        await using (var session = blue.LightweightSession())
        {
            (await session.LoadAsync<KeptStableTally>(streamId, Ct))!.Count.ShouldBe(3);
            session.Store(new KeptStableTally { Id = streamId, Count = Marked });
            await session.SaveChangesAsync(Ct);
        }

        return blue;
    }

    [Fact]
    public async Task a_bump_replays_only_the_members_that_changed()
    {
        var streamId = Guid.NewGuid();
        await deployVersionOneAsync(streamId);

        configure(compositeVersion: 2, changingVersion: 2);
        var green = theStore;

        // Each member kept the version it was given, so only the changed one gets a new table
        green.Options.Providers.GetProvider(typeof(KeptStableTally)).Mapping.TableName.ShouldBe("pc_doc_keptstabletally");
        green.Options.Providers.GetProvider(typeof(KeptChangingTally)).Mapping.TableName
            .ShouldBe("pc_doc_keptchangingtally_2");

        await green.WaitForProjectionAsync();

        await using (var query = green.QuerySession())
        {
            // Neither torn down nor replayed over
            (await query.LoadAsync<KeptStableTally>(streamId, Ct))!.Count.ShouldBe(Marked);

            // The changed member replayed the stream into its new table
            (await query.LoadAsync<KeptChangingTally>(streamId, Ct))!.Count.ShouldBe(3);
        }

        // From here on the kept member is an ordinary member again
        await using (var session = green.LightweightSession())
        {
            session.Events.Append(streamId, new KeptIncrement());
            await session.SaveChangesAsync(Ct);
        }

        await green.WaitForProjectionAsync();

        await using (var query = green.QuerySession())
        {
            (await query.LoadAsync<KeptStableTally>(streamId, Ct))!.Count.ShouldBe(Marked + 1);
            (await query.LoadAsync<KeptChangingTally>(streamId, Ct))!.Count.ShouldBe(4);
        }
    }

    [Fact]
    public async Task a_rebuild_right_after_a_bump_leaves_the_unchanged_members_standing()
    {
        var streamId = Guid.NewGuid();
        await deployVersionOneAsync(streamId);

        configure(compositeVersion: 2, changingVersion: 2);
        var green = theStore;

        using (var daemon = await green.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(CompositeName, Ct);
        }

        await using var query = green.QuerySession();
        (await query.LoadAsync<KeptStableTally>(streamId, Ct))!.Count.ShouldBe(Marked);
        (await query.LoadAsync<KeptChangingTally>(streamId, Ct))!.Count.ShouldBe(3);
    }

    [Fact]
    public async Task a_rebuild_with_no_bump_still_rebuilds_every_member()
    {
        var streamId = Guid.NewGuid();
        var blue = await deployVersionOneAsync(streamId);

        // Every member is in lockstep with the composite, so this rebuild is a repair, and nothing is kept
        using (var daemon = await blue.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(CompositeName, Ct);
        }

        await using var query = blue.QuerySession();
        (await query.LoadAsync<KeptStableTally>(streamId, Ct))!.Count.ShouldBe(3);
        (await query.LoadAsync<KeptChangingTally>(streamId, Ct))!.Count.ShouldBe(3);
    }

    [Fact]
    public void without_the_setting_every_member_takes_the_composites_version()
    {
        // #1089 is unchanged when the setting is off
        configure(compositeVersion: 2, changingVersion: 3, keepUnchangedMembers: false);

        theStore.Options.Providers.GetProvider(typeof(KeptStableTally)).Mapping.TableName
            .ShouldBe("pc_doc_keptstabletally_2");
        theStore.Options.Providers.GetProvider(typeof(KeptChangingTally)).Mapping.TableName
            .ShouldBe("pc_doc_keptchangingtally_2");
    }

    [Fact]
    public void a_composite_of_more_than_one_stage_gives_its_members_its_own_version()
    {
        // A later stage reads what the stages before it produce as they replay, so a multi-stage composite replays
        // every member whatever the setting, and its members take its version as before
        ConfigureStore(opts => opts.Projections.CompositeProjectionFor(CompositeName, composite =>
        {
            composite.Version = 2;
            composite.Add(new KeptStableTallyProjection());
            composite.Add(new KeptChangingTallyProjection(), stageNumber: 2);
            composite.KeepUnchangedMembers = true;
        }));

        theStore.Options.Providers.GetProvider(typeof(KeptStableTally)).Mapping.TableName
            .ShouldBe("pc_doc_keptstabletally_2");
    }
}

public record KeptIncrement;

public class KeptStableTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class KeptStableTallyProjection : SingleStreamProjection<KeptStableTally, Guid>
{
    public KeptStableTally Create(KeptIncrement _) => new() { Count = 1 };

    public void Apply(KeptIncrement _, KeptStableTally tally) => tally.Count++;
}

public class KeptChangingTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class KeptChangingTallyProjection : SingleStreamProjection<KeptChangingTally, Guid>
{
    public KeptChangingTally Create(KeptIncrement _) => new() { Count = 1 };

    public void Apply(KeptIncrement _, KeptChangingTally tally) => tally.Count++;
}
