using JasperFx;
using JasperFx.Metadata;
using Microsoft.Data.SqlClient;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Operations;

/// <summary>
///     #831, the twin of marten#5743. Before <c>OperationResultSets</c>, the flush called
///     <c>NextResultAsync</c> once per operation, while a batch only holds a result set for the
///     operations that really return one. A no-result operation queued ahead of a versioned write
///     stepped the reader past that write's result set, so the write read nothing, which is how it
///     spells "conflict":
///     <list type="bullet">
///         <item>a valid write failed with a false <see cref="ConcurrencyException" />, and nothing committed</item>
///         <item>
///             with another document behind it, a genuinely stale write read that document's row
///             instead, reported success, and the transaction committed around the dropped write
///         </item>
///     </list>
///     The second is the one to care about, so every assertion on a conflict also proves nothing
///     landed. The daemon's half of this is <c>projection_batch_result_set_alignment_tests</c>.
/// </summary>
public class mixed_batch_result_set_alignment_tests(mixed_batch_store fixture) : IClassFixture<mixed_batch_store>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private DocumentStore theStore => fixture.Store;

    private string Schema => theStore.Options.DatabaseSchemaName;

    /// <summary>Every operation a session can queue whose command contributes no result set.</summary>
    public static TheoryData<string> NoResultOperations =>
    [
        "Delete", "HardDelete", "DeleteWhere", "HardDeleteWhere", "UndoDeleteWhere", "Patch",
        "QueueSqlCommand", "QueueSqlCommand DML", "ArchiveStream", "TombstoneStream"
    ];

    private void QueueNoResultOperation(IDocumentSession session, string operation)
    {
        switch (operation)
        {
            case "Delete":
                session.Delete<Bystander>(Guid.NewGuid());
                break;
            case "HardDelete":
                session.HardDelete<Bystander>(Guid.NewGuid());
                break;
            case "DeleteWhere":
                session.DeleteWhere<Bystander>(x => x.Name == "nothing matches this");
                break;
            case "HardDeleteWhere":
                session.HardDeleteWhere<Bystander>(x => x.Name == "nothing matches this");
                break;
            case "UndoDeleteWhere":
                session.UndoDeleteWhere<SoftBystander>(x => x.Name == "nothing matches this");
                break;
            case "Patch":
                session.Patch<Bystander>(Guid.NewGuid()).Set(x => x.Name, "patched");
                break;
            case "QueueSqlCommand":
                session.QueueSqlCommand("DECLARE @x int = 1;");
                break;
            case "QueueSqlCommand DML":
                session.QueueSqlCommand($"UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = last_seq_id WHERE 1 = 0");
                break;
            case "ArchiveStream":
                session.Events.ArchiveStream(Guid.NewGuid());
                break;
            case "TombstoneStream":
                session.Events.TombstoneStream(Guid.NewGuid());
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    private async Task<RevisionedDoc> SeedRevisionedAsync()
    {
        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "one" };
        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Ct);
        return doc;
    }

    private async Task<VersionedDoc> SeedVersionedAsync()
    {
        var doc = new VersionedDoc { Id = Guid.NewGuid(), Name = "one" };
        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Ct);
        return doc;
    }

    /// <summary>
    ///     A copy holding the version <paramref name="doc" /> had, with the row moved on underneath it
    ///     by a second writer: the shape of a genuine lost update.
    /// </summary>
    private async Task<VersionedDoc> SupersedeAsync(VersionedDoc doc)
    {
        var stale = new VersionedDoc { Id = doc.Id, Name = "stale", Version = doc.Version };

        await using var other = theStore.LightweightSession();
        var fresh = (await other.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull();
        fresh.Name = "winner";
        other.Store(fresh);
        await other.SaveChangesAsync(Ct);

        return stale;
    }

    // ---- controls: the same writes with nothing queued ahead of them, which always worked ---------

    [Fact]
    public async Task control_a_revision_update_alone_succeeds()
    {
        var doc = await SeedRevisionedAsync();

        await using var session = theStore.LightweightSession();
        doc.Name = "two";
        session.UpdateRevision(doc, doc.Version + 1);
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");
    }

    [Fact]
    public async Task control_a_versioned_store_alone_succeeds()
    {
        var doc = await SeedVersionedAsync();

        await using var session = theStore.LightweightSession();
        var loaded = (await session.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull();
        loaded.Name = "two";
        session.Store(loaded);
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");
    }

    [Fact]
    public async Task control_a_real_conflict_with_no_delete_ahead_of_it_throws()
    {
        var stale = await SupersedeAsync(await SeedVersionedAsync());

        await using var session = theStore.LightweightSession();
        session.Store(stale);
        session.Store(new Bystander { Id = Guid.NewGuid(), Name = "innocent" });

        await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    // ---- a valid write behind a no-result operation: the false conflict ---------------------------

    [Theory]
    [MemberData(nameof(NoResultOperations))]
    public async Task a_revision_update_behind_a_no_result_operation_succeeds(string operation)
    {
        var doc = await SeedRevisionedAsync();

        await using (var session = theStore.LightweightSession())
        {
            QueueNoResultOperation(session, operation);
            doc.Name = "two";
            session.UpdateRevision(doc, doc.Version + 1);
            await session.SaveChangesAsync(Ct);
        }

        doc.Version.ShouldBe(2);

        await using var query = theStore.QuerySession();
        var stored = (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull();
        stored.Name.ShouldBe("two");
        stored.Version.ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(NoResultOperations))]
    public async Task a_versioned_store_behind_a_no_result_operation_succeeds(string operation)
    {
        var doc = await SeedVersionedAsync();

        await using (var session = theStore.LightweightSession())
        {
            var loaded = (await session.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull();
            QueueNoResultOperation(session, operation);
            loaded.Name = "two";
            session.Store(loaded);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");
    }

    [Fact]
    public async Task an_insert_behind_a_no_result_operation_succeeds()
    {
        var doc = new Bystander { Id = Guid.NewGuid(), Name = "inserted" };

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Bystander>(Guid.NewGuid());
            session.Insert(doc);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bystander>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("inserted");
    }

    [Fact]
    public async Task an_update_behind_a_no_result_operation_succeeds()
    {
        var doc = new Bystander { Id = Guid.NewGuid(), Name = "one" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Bystander>(Guid.NewGuid());
            doc.Name = "two";
            session.Update(doc);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bystander>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");
    }

    // ---- a real conflict behind a no-result operation: the swallowed one --------------------------

    /// <summary>
    ///     The worst row of the issue's table. The trailing unrelated write is what makes this silent:
    ///     its row is what the stale write's <c>ReadAsync</c> used to land on, so there was no exception
    ///     at all and the transaction committed <c>innocent</c> around the dropped write.
    /// </summary>
    [Theory]
    [MemberData(nameof(NoResultOperations))]
    public async Task a_real_guid_conflict_behind_a_no_result_operation_throws_and_commits_nothing(string operation)
    {
        var stale = await SupersedeAsync(await SeedVersionedAsync());
        var innocent = new Bystander { Id = Guid.NewGuid(), Name = "innocent" };

        await using (var session = theStore.LightweightSession())
        {
            QueueNoResultOperation(session, operation);
            session.Store(stale);
            session.Store(innocent);

            await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VersionedDoc>(stale.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("winner");
        (await query.LoadAsync<Bystander>(innocent.Id, Ct)).ShouldBeNull();
    }

    [Theory]
    [MemberData(nameof(NoResultOperations))]
    public async Task a_stale_revision_behind_a_no_result_operation_throws_and_commits_nothing(string operation)
    {
        var doc = await SeedRevisionedAsync();
        var innocent = new Bystander { Id = Guid.NewGuid(), Name = "innocent" };

        await using (var session = theStore.LightweightSession())
        {
            QueueNoResultOperation(session, operation);
            doc.Name = "stale";
            session.UpdateRevision(doc, doc.Version);
            session.Store(innocent);

            await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("one");
        (await query.LoadAsync<Bystander>(innocent.Id, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_duplicate_insert_behind_a_no_result_operation_throws_and_commits_nothing()
    {
        var existing = new Bystander { Id = Guid.NewGuid(), Name = "one" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(existing);
            await session.SaveChangesAsync(Ct);
        }

        var innocent = new Bystander { Id = Guid.NewGuid(), Name = "innocent" };

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Bystander>(Guid.NewGuid());
            session.Insert(new Bystander { Id = existing.Id, Name = "duplicate" });
            session.Store(innocent);

            await Should.ThrowAsync<DocumentAlreadyExistsException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bystander>(existing.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("one");
        (await query.LoadAsync<Bystander>(innocent.Id, Ct)).ShouldBeNull();
    }

    // ---- every order ------------------------------------------------------------------------------

    public enum Step
    {
        NoResultDml,
        NoResultSql,
        UnversionedUpsert,
        GuidVersionedUpsert,
        NumericUpdate,
        StaleGuidUpsert,
        StaleNumericUpdate
    }

    private static IEnumerable<Step[]> Orderings(params Step[] steps)
    {
        if (steps.Length <= 1)
        {
            yield return steps;
            yield break;
        }

        foreach (var first in steps)
        {
            foreach (var rest in Orderings(steps.Where(x => x != first).ToArray()))
            {
                yield return [first, .. rest];
            }
        }
    }

    /// <summary>
    ///     The unversioned upsert earns its place here. It never reads its result set, so nothing
    ///     about its <c>PostprocessAsync</c> shows that it has one — but a walk that skipped it would
    ///     hand its set to the next versioned write, which would be misaligned the other way.
    /// </summary>
    [Fact]
    public async Task every_order_of_a_mixed_batch_of_valid_writes_commits()
    {
        foreach (var ordering in Orderings(Step.NoResultDml, Step.NoResultSql, Step.UnversionedUpsert,
                     Step.GuidVersionedUpsert, Step.NumericUpdate))
        {
            var versioned = await SeedVersionedAsync();
            var revisioned = await SeedRevisionedAsync();
            var plain = new Bystander { Id = Guid.NewGuid(), Name = "stored" };

            await using (var session = theStore.LightweightSession())
            {
                var loaded = (await session.LoadAsync<VersionedDoc>(versioned.Id, Ct)).ShouldNotBeNull();
                loaded.Name = "two";
                revisioned.Name = "two";

                foreach (var step in ordering)
                {
                    switch (step)
                    {
                        case Step.NoResultDml:
                            session.Delete<Bystander>(Guid.NewGuid());
                            break;
                        case Step.NoResultSql:
                            session.QueueSqlCommand($"UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = last_seq_id WHERE 1 = 0");
                            break;
                        case Step.UnversionedUpsert:
                            session.Store(plain);
                            break;
                        case Step.GuidVersionedUpsert:
                            session.Store(loaded);
                            break;
                        case Step.NumericUpdate:
                            session.UpdateRevision(revisioned, revisioned.Version + 1);
                            break;
                    }
                }

                await session.SaveChangesAsync(Ct);
            }

            var order = string.Join(", ", ordering);
            await using var query = theStore.QuerySession();
            (await query.LoadAsync<VersionedDoc>(versioned.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two", order);
            (await query.LoadAsync<RevisionedDoc>(revisioned.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two", order);
            (await query.LoadAsync<Bystander>(plain.Id, Ct)).ShouldNotBeNull();
        }
    }

    [Theory]
    [InlineData(Step.StaleGuidUpsert)]
    [InlineData(Step.StaleNumericUpdate)]
    public async Task a_stale_write_is_caught_in_every_position(Step staleStep)
    {
        foreach (var ordering in Orderings(Step.NoResultDml, Step.NoResultSql, Step.UnversionedUpsert, staleStep))
        {
            var stale = await SupersedeAsync(await SeedVersionedAsync());
            var revisioned = await SeedRevisionedAsync();
            var plain = new Bystander { Id = Guid.NewGuid(), Name = "stored" };

            await using (var session = theStore.LightweightSession())
            {
                foreach (var step in ordering)
                {
                    switch (step)
                    {
                        case Step.NoResultDml:
                            session.Delete<Bystander>(Guid.NewGuid());
                            break;
                        case Step.NoResultSql:
                            session.QueueSqlCommand($"UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = last_seq_id WHERE 1 = 0");
                            break;
                        case Step.UnversionedUpsert:
                            session.Store(plain);
                            break;
                        case Step.StaleGuidUpsert:
                            session.Store(stale);
                            break;
                        case Step.StaleNumericUpdate:
                            session.UpdateRevision(revisioned, revisioned.Version);
                            break;
                    }
                }

                await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct),
                    string.Join(", ", ordering));
            }

            await using var query = theStore.QuerySession();
            (await query.LoadAsync<Bystander>(plain.Id, Ct)).ShouldBeNull(string.Join(", ", ordering));
        }
    }

    /// <summary>
    ///     A row carries a value, so the right row can be told from a neighbour's. <c>ReadAsync</c> landing
    ///     on any row at all would pass every "did it throw?" test in this class; this one only
    ///     passes when each write read its own.
    /// </summary>
    [Fact]
    public async Task each_versioned_write_reads_its_own_row()
    {
        var first = await SeedRevisionedAsync();
        var second = await SeedRevisionedAsync();
        var third = await SeedRevisionedAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.UpdateRevision(first, 5);
            session.Delete<Bystander>(Guid.NewGuid());
            session.UpdateRevision(second, 9);
            session.Store(new Bystander { Id = Guid.NewGuid(), Name = "unversioned" });
            session.QueueSqlCommand("DECLARE @x int = 1;");
            session.DeleteWhere<Bystander>(x => x.Name == "nothing matches this");
            session.UpdateRevision(third, 14);
            await session.SaveChangesAsync(Ct);
        }

        first.Version.ShouldBe(5);
        second.Version.ShouldBe(9);
        third.Version.ShouldBe(14);
    }

    // ---- the full-text variant, whose result set comes from a trailing SELECT ---------------------

    [Fact]
    public async Task a_full_text_versioned_write_behind_a_delete_succeeds()
    {
        var versioned = new SearchableVersioned { Id = Guid.NewGuid(), Body = "alpha" };
        var revisioned = new SearchableRevisioned { Id = Guid.NewGuid(), Body = "alpha" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(versioned);
            session.Store(revisioned);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var loaded = (await session.LoadAsync<SearchableVersioned>(versioned.Id, Ct)).ShouldNotBeNull();
            session.Delete<SearchableVersioned>(Guid.NewGuid());
            loaded.Body = "beta";
            session.Store(loaded);
            session.Delete<SearchableRevisioned>(Guid.NewGuid());
            revisioned.Body = "beta";
            session.UpdateRevision(revisioned, revisioned.Version + 1);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<SearchableVersioned>(versioned.Id, Ct)).ShouldNotBeNull().Body.ShouldBe("beta");
        (await query.LoadAsync<SearchableRevisioned>(revisioned.Id, Ct)).ShouldNotBeNull().Body.ShouldBe("beta");
    }

    [Fact]
    public async Task a_stale_full_text_write_behind_a_delete_throws_and_commits_nothing()
    {
        var doc = new SearchableVersioned { Id = Guid.NewGuid(), Body = "alpha" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }

        // Unversioned on purpose. A versioned neighbour would run out of result sets itself and throw,
        // which is the wrong reason for this to pass.
        var innocent = new Bystander { Id = Guid.NewGuid(), Name = "innocent" };

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Bystander>(Guid.NewGuid());
            session.Store(new SearchableVersioned { Id = doc.Id, Body = "stale", Version = Guid.NewGuid() });
            session.Store(innocent);

            await Should.ThrowAsync<ConcurrencyException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<SearchableVersioned>(doc.Id, Ct)).ShouldNotBeNull().Body.ShouldBe("alpha");
        (await query.LoadAsync<Bystander>(innocent.Id, Ct)).ShouldBeNull();
    }

    // ---- events alongside ----------------------------------------------------------------------

    [Fact]
    public async Task a_session_appending_events_keeps_its_document_batch_aligned()
    {
        var doc = await SeedRevisionedAsync();
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Bystander>(Guid.NewGuid());
            session.Events.StartStream(streamId, new AlignmentHappened(1), new AlignmentHappened(2));
            doc.Name = "two";
            session.UpdateRevision(doc, doc.Version + 1);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RevisionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("two");

        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Count.ShouldBe(2);
        events.ShouldAllBe(e => e.Sequence > 0);
    }

    // ---- the drain: a batch error only surfaces when the reader advances over its command ---------

    public static TheoryData<string> ViolationLayouts =>
    [
        "document violation last",
        "document violation before DML",
        "document violation between DML",
        "document violation before a versioned write",
        "sql violation last",
        "sql violation among only DML",
        "sql violation between result sets",
        "sql violation first",
        "sql violation behind rows nothing declared"
    ];

    /// <summary>
    ///     The walk used to advance once per operation, so it reached most commands on its own and the
    ///     drain behind it only covered the tail. It now advances once per RESULT SET, so the drain is
    ///     the only thing that ever reads past the last of those. A violation sitting behind the walk's
    ///     final advance must still fail the save, and take the whole batch with it — whichever kind
    ///     of command raised it and wherever it sits.
    ///     <para>
    ///         The last layout is the drain's hardest case: a queued statement that returned rows it
    ///         was not supposed to, so the reader holds a result set nothing accounts for and one
    ///         final advance would stop short of the violation.
    ///     </para>
    /// </summary>
    [Theory]
    [MemberData(nameof(ViolationLayouts))]
    public async Task a_unique_violation_in_a_later_command_surfaces_and_commits_nothing(string layout)
    {
        var taken = new UniqueDoc { Id = Guid.NewGuid(), Email = $"{Guid.NewGuid()}@example.com" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(taken);
            await session.SaveChangesAsync(Ct);
        }

        var doc = await SeedVersionedAsync();
        var plain = new Bystander { Id = Guid.NewGuid(), Name = "stored" };

        await using (var session = theStore.LightweightSession())
        {
            var loaded = (await session.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull();
            loaded.Name = "two";

            var delete = () => session.Delete<Bystander>(Guid.NewGuid());
            var violatingDocument = () => session.Store(new UniqueDoc { Id = Guid.NewGuid(), Email = taken.Email });
            var violatingSql = () => session.QueueSqlCommand($"INSERT INTO [{Schema}].[{mixed_batch_store.DrainProbe}] (value) VALUES (1)");
            var selectingSql = () => session.QueueSqlCommand("SELECT 1");
            var versionedWrite = () => session.Store(loaded);
            var plainWrite = () => session.Store(plain);

            Action[] steps = layout switch
            {
                "document violation last" => [versionedWrite, violatingDocument],
                "document violation before DML" => [versionedWrite, violatingDocument, delete],
                "document violation between DML" => [delete, versionedWrite, violatingDocument, delete],
                "document violation before a versioned write" => [violatingDocument, versionedWrite],
                "sql violation last" => [versionedWrite, violatingSql],
                "sql violation among only DML" => [delete, violatingSql, delete],
                "sql violation between result sets" => [versionedWrite, violatingSql, plainWrite],
                "sql violation first" => [violatingSql, versionedWrite],
                "sql violation behind rows nothing declared" => [selectingSql, plainWrite, violatingSql],
                _ => throw new ArgumentOutOfRangeException(nameof(layout), layout, null)
            };

            foreach (var step in steps)
            {
                step();
            }

            var ex = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
            ex.Number.ShouldBe(2601);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<VersionedDoc>(doc.Id, Ct)).ShouldNotBeNull().Name.ShouldBe("one");
        (await query.LoadAsync<Bystander>(plain.Id, Ct)).ShouldBeNull();
    }

    public class Bystander
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class SoftBystander : ISoftDeleted
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Deleted { get; set; }
        public DateTimeOffset? DeletedAt { get; set; }
    }

    public class UniqueDoc
    {
        public Guid Id { get; set; }
        public string Email { get; set; } = string.Empty;
    }

    public class SearchableVersioned : IVersioned
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public Guid Version { get; set; }
    }

    public class SearchableRevisioned : IRevisioned
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public int Version { get; set; }
    }
}

/// <summary>
///     One schema for the whole class: creating it costs seconds, and these facts are told apart by
///     their own document ids rather than by a clean slate.
/// </summary>
public class mixed_batch_store : OneOffConfigurationsContext
{
    public const string DrainProbe = "pc_drain_probe";

    public DocumentStore Store => theStore;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        ConfigureStore(opts =>
        {
            opts.Schema.For<mixed_batch_result_set_alignment_tests.UniqueDoc>().UniqueIndex(x => x.Email);
            opts.Schema.For<mixed_batch_result_set_alignment_tests.SearchableVersioned>().FullTextIndex(x => x.Body);
            opts.Schema.For<mixed_batch_result_set_alignment_tests.SearchableRevisioned>().FullTextIndex(x => x.Body);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var schema = theStore.Options.DatabaseSchemaName;
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE [{schema}].[{DrainProbe}] (value int NOT NULL);
            CREATE UNIQUE INDEX ux_{DrainProbe} ON [{schema}].[{DrainProbe}] (value);
            INSERT INTO [{schema}].[{DrainProbe}] (value) VALUES (1);
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
