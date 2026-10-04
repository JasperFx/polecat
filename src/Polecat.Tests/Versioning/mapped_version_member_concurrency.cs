using JasperFx;
using Polecat.Attributes;
using Polecat.TestUtils;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Versioning;

/// <summary>
///     #720 — a version/revision member declared through <c>Metadata(m =&gt; m.Version.MapTo(...))</c>
///     is a real concurrency declaration, not just a place to project the stored value.
/// </summary>
/// <remarks>
///     <para>
///         Polecat has no <c>Schema.For&lt;T&gt;().UseOptimisticConcurrency(true)</c> switch — the mode
///         is declared by the member, exactly as the marker interfaces declare it by their type. So the
///         mapped member has to do three things the marker interfaces already do: select the mode, be
///         written and read back by the storage binders, and seed the write's expected version. Before
///         #720 it did none of them, which made <c>MapTo</c> on the version column an offered API that
///         could not be turned on at all.
///     </para>
///     <para>
///         The facts come in pairs — "a cross-session write succeeds" next to "a stale write is still
///         refused" — because the cheap wrong fix (seed nothing, or seed whatever the row holds) passes
///         one half and fails the other. Same shape as fisher#245 / marten#5372 / polecat#592: the
///         fourth independent sighting of this one field.
///     </para>
/// </remarks>
[Collection("integration")]
public class mapped_version_member_concurrency : IntegrationContext
{
    public mapped_version_member_concurrency(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "mapped_version";
            opts.Schema.For<EtaggedDoc>().Metadata(m => m.Version.MapTo(x => x.Etag));
            opts.Schema.For<RevisionedDoc>().Metadata(m => m.Version.MapTo(x => x.Revision));
            opts.Schema.For<LongRevisionedDoc>().Metadata(m => m.Version.MapTo(x => x.Sequence));
            opts.Schema.For<AttributedDoc>();
        });
    }

    // ---- Guid: the mapped member carries the optimistic-concurrency version ----

    [Fact]
    public async Task mapped_guid_member_is_stamped_on_insert()
    {
        var doc = new EtaggedDoc { Id = Guid.NewGuid(), Name = "first" };
        doc.Etag.ShouldBe(Guid.Empty);

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        doc.Etag.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task mapped_guid_member_is_populated_on_load()
    {
        var doc = new EtaggedDoc { Id = Guid.NewGuid(), Name = "first" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<EtaggedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Etag.ShouldBe(doc.Etag);
    }

    /// <summary>The fact #720 is filed on: the writing session never read the row.</summary>
    [Fact]
    public async Task mapped_guid_document_loaded_in_one_session_stores_through_another()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new EtaggedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        EtaggedDoc loaded;
        await using (var read = theStore.QuerySession())
        {
            loaded = (await read.LoadAsync<EtaggedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        loaded.ShouldNotBeNull();
        loaded.Name = "v2";

        await using (var write = theStore.LightweightSession())
        {
            write.Store(loaded);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<EtaggedDoc>(id, TestContext.Current.CancellationToken);
        stored!.Name.ShouldBe("v2");
    }

    /// <summary>The twin: without it, "stop guarding" passes the fact above.</summary>
    [Fact]
    public async Task mapped_guid_stale_instance_is_refused_and_the_winner_stands()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new EtaggedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        EtaggedDoc first, second;
        await using (var read = theStore.QuerySession())
        {
            first = (await read.LoadAsync<EtaggedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        await using (var read = theStore.QuerySession())
        {
            second = (await read.LoadAsync<EtaggedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        first.Name = "winner";
        await using (var write = theStore.LightweightSession())
        {
            write.Store(first);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        second.Name = "loser";
        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await using var write = theStore.LightweightSession();
            write.Store(second);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        });

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<EtaggedDoc>(id, TestContext.Current.CancellationToken);
        stored!.Name.ShouldBe("winner");
    }

    // ---- numeric: int ----
    //
    // The mapped member has to behave exactly as IRevisioned.Version does, which means #559's rule:
    // Store() passes the document's own revision as the TARGET, and the target must be strictly
    // greater than what is stored. So "both directions" here is "0 auto-increments" and "a named
    // next revision lands", not "re-storing at the loaded revision increments".

    [Fact]
    public async Task mapped_int_revision_starts_at_one_and_is_populated_on_load()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "v1" };
        doc.Revision.ShouldBe(0);

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        doc.Revision.ShouldBe(1);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<RevisionedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded!.Revision.ShouldBe(1);
    }

    /// <summary>#559's rule reaches the mapped member too — re-storing at the loaded revision is refused.</summary>
    [Fact]
    public async Task mapped_int_revision_at_the_loaded_revision_is_refused()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new RevisionedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.LightweightSession();
        var loaded = await session2.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        loaded!.Revision.ShouldBe(1);

        loaded.Name = "v2";
        session2.Store(loaded);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     The two supported ways forward, through a session that never read the row: hand back 0 and
    ///     let the store increment, or name the next revision through <c>UpdateRevision</c>.
    /// </summary>
    [Fact]
    public async Task mapped_int_revision_moves_forward_across_a_session_boundary()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new RevisionedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        RevisionedDoc loaded;
        await using (var read = theStore.QuerySession())
        {
            loaded = (await read.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        // Name the next revision. UpdateRevision assigns it to the document and re-Stores, so the
        // mapped member needs the write-back half as well as the read (#720).
        loaded.Name = "v2";
        await using (var write = theStore.LightweightSession())
        {
            write.UpdateRevision(loaded, loaded.Revision + 1);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        loaded.Revision.ShouldBe(2);

        // Or hand back 0 and let the store count on from whatever it has.
        loaded.Name = "v3";
        loaded.Revision = 0;
        await using (var write = theStore.LightweightSession())
        {
            write.Store(loaded);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        loaded.Revision.ShouldBe(3);

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        stored!.Name.ShouldBe("v3");
        stored.Revision.ShouldBe(3);
    }

    /// <summary>The twin: a loser naming a revision the winner already took is refused.</summary>
    [Fact]
    public async Task mapped_int_revision_stale_instance_is_refused_and_the_winner_stands()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new RevisionedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        RevisionedDoc first, second;
        await using (var read = theStore.QuerySession())
        {
            first = (await read.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        await using (var read = theStore.QuerySession())
        {
            second = (await read.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        first.Name = "winner";
        await using (var write = theStore.LightweightSession())
        {
            write.UpdateRevision(first, first.Revision + 1);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        second.Name = "loser";
        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await using var write = theStore.LightweightSession();
            write.UpdateRevision(second, second.Revision + 1);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        });

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<RevisionedDoc>(id, TestContext.Current.CancellationToken);
        stored!.Name.ShouldBe("winner");
    }

    // ---- numeric: long ----

    [Fact]
    public async Task mapped_long_revision_moves_forward_across_a_session_boundary()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new LongRevisionedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        LongRevisionedDoc loaded;
        await using (var read = theStore.QuerySession())
        {
            loaded = (await read.LoadAsync<LongRevisionedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        loaded.Sequence.ShouldBe(1L);

        loaded.Name = "v2";
        loaded.Sequence = 0;
        await using (var write = theStore.LightweightSession())
        {
            write.Store(loaded);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        loaded.Sequence.ShouldBe(2L);

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<LongRevisionedDoc>(id, TestContext.Current.CancellationToken);
        stored!.Sequence.ShouldBe(2L);
    }

    // ---- the control ----

    /// <summary>
    ///     A type that maps no version member is untouched by any of this. The cheap over-reaction to
    ///     the facts above is a store-wide guard, which is a breaking change for every document that
    ///     never asked for one.
    /// </summary>
    [Fact]
    public async Task a_type_with_no_mapped_version_member_is_unguarded()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new PlainDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        PlainDoc first, second;
        await using (var read = theStore.QuerySession())
        {
            first = (await read.LoadAsync<PlainDoc>(id, TestContext.Current.CancellationToken))!;
        }

        await using (var read = theStore.QuerySession())
        {
            second = (await read.LoadAsync<PlainDoc>(id, TestContext.Current.CancellationToken))!;
        }

        first.Name = "second writer wins";
        await using (var write = theStore.LightweightSession())
        {
            write.Store(first);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        second.Name = "and so does this one";
        await using (var write = theStore.LightweightSession())
        {
            write.Store(second);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var verify = theStore.QuerySession();
        var stored = await verify.LoadAsync<PlainDoc>(id, TestContext.Current.CancellationToken);
        stored!.Name.ShouldBe("and so does this one");
    }

    // ---- the attribute spelling, and the misconfiguration ----

    /// <summary>
    ///     <c>[VersionMetadata]</c> is the same declaration by another route, and it is discovered in
    ///     the mapping's constructor rather than merged in afterwards — the two code paths that call
    ///     <c>ResolveMappedConcurrencyMode</c>.
    /// </summary>
    [Fact]
    public async Task the_version_metadata_attribute_declares_concurrency_too()
    {
        var id = Guid.NewGuid();
        await using (var write = theStore.LightweightSession())
        {
            write.Store(new AttributedDoc { Id = id, Name = "v1" });
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        AttributedDoc first, second;
        await using (var read = theStore.QuerySession())
        {
            first = (await read.LoadAsync<AttributedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        await using (var read = theStore.QuerySession())
        {
            second = (await read.LoadAsync<AttributedDoc>(id, TestContext.Current.CancellationToken))!;
        }

        first.Etag.ShouldNotBe(Guid.Empty);

        first.Name = "winner";
        await using (var write = theStore.LightweightSession())
        {
            write.Store(first);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        second.Name = "loser";
        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await using var write = theStore.LightweightSession();
            write.Store(second);
            await write.SaveChangesAsync(TestContext.Current.CancellationToken);
        });
    }

    /// <summary>
    ///     A mapped version member whose type names no mode is refused rather than silently ignored —
    ///     being silently ignored is what #720 was.
    /// </summary>
    [Fact]
    public void a_mapped_version_member_of_the_wrong_type_is_refused()
    {
        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            var options = new StoreOptions { ConnectionString = ConnectionSource.ConnectionString };
            options.DatabaseSchemaName = "mapped_version_bad";
            options.Schema.For<BadlyMappedDoc>().Metadata(m => m.Version.MapTo(x => x.Label));

            using var store = new DocumentStore(options);
            store.Options.Providers.GetProvider<BadlyMappedDoc>();
        });

        ex.Message.ShouldContain("Label");
        ex.Message.ShouldContain("Guid");
    }

    public class EtaggedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid Etag { get; set; }
    }

    public class RevisionedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Revision { get; set; }
    }

    public class LongRevisionedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public long Sequence { get; set; }
    }

    public class PlainDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class AttributedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;

        [VersionMetadata] public Guid Etag { get; set; }
    }

    public class BadlyMappedDoc
    {
        public Guid Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
