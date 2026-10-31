using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.TestUtils;
using Shouldly;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Tests.Operations;

/// <summary>
///     polecat#442 (the marten#5210 class) and #831. These pin the facts
///     <c>OperationResultSets.ApplyAsync</c>, the one walk over a batch's result sets, is built on.
///     <para>
///         <b>A batch does not hold one result set per operation.</b> SQL Server's batch reader surfaces
///         only <em>real</em> result sets. A pure-DML command contributes none at all, however many
///         statements it holds (<c>TombstoneStreamOperation</c>'s two DELETEs are still zero), and the
///         reader moves straight past it. Every document write contributes exactly one, through its
///         <c>OUTPUT</c> or the full-text variant's trailing <c>SELECT</c>, even when a guard refused the
///         write and the set is empty. Measured for pure DML below, and for every operation shape in
///         <c>operation_result_set_census_tests</c>.
///     </para>
///     <para>
///         <b>So the walk is driven by a declaration, and the declaration has to be true.</b> An
///         operation marked <see cref="NoDataReturnedCall" /> gets no turn; every other operation owns
///         one result set. That makes Marten's exact mechanism possible here too: if a marked
///         operation's SQL returned rows, every operation behind it would read the wrong set, which
///         is a failure that surfaces far from its cause. This class holds the marker to the SQL.
///     </para>
///     <para>
///         This remark used to say the opposite. It claimed that "an operation in the document batch
///         contributes ZERO result sets" and that the operations which read in
///         <c>PostprocessAsync</c> never shared a batch with ones that did not, which was the
///         justification for calling <c>NextResultAsync</c> once per operation. Neither was true of the
///         operations that matter: a versioned upsert reads in <c>PostprocessAsync</c> and sits in the
///         document batch beside everything else, and in the async daemon's batch the event appends do
///         too. That is what #831 was.
///     </para>
///     <para>
///         What does hold is narrower: a <em>session</em> runs its event appends in their own batch.
///         <c>SaveChangesAsync</c> executes the DCB boundary phase, then
///         <c>ProcessStreamsClosedShapeAsync</c>, and only then the document operations. Pinned
///         end-to-end below by a session that writes documents and appends events together.
///     </para>
///     <para>
///         A reflection sweep makes a newly added <see cref="NoDataReturnedCall" /> type fail this class
///         until someone audits its SQL. <c>operation_result_set_census_tests</c> has the sweep for the
///         opposite mistake, an operation that returns nothing and was not marked.
///     </para>
/// </summary>
public class operation_result_set_alignment_tests : IAsyncLifetime
{
    private const string Schema = "op_alignment";
    private const int Sentinel = 424242;

    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    private EventGraph Events => _store.Database.Events;

    public static IEnumerable<object[]> Operations()
    {
        yield return ["RecordProgressionOperation (merge)", nameof(RecordProgressionOperation) + ":upsert"];
        yield return ["RecordProgressionOperation (update)", nameof(RecordProgressionOperation) + ":update"];
        yield return ["TombstoneStreamOperation", nameof(TombstoneStreamOperation)];
        yield return ["SetStreamArchivedOperation", nameof(SetStreamArchivedOperation)];
        yield return ["NaturalKeyArchiveOperation", nameof(NaturalKeyArchiveOperation)];
        yield return ["ExecuteSqlStorageOperation", nameof(ExecuteSqlStorageOperation)];
    }

    private Polecat.Internal.IStorageOperation Build(string key) => key switch
    {
        nameof(RecordProgressionOperation) + ":upsert" => new RecordProgressionOperation(
            Events.ProgressionTableName, "Alignment:All", 1, false, upsert: true),
        nameof(RecordProgressionOperation) + ":update" => new RecordProgressionOperation(
            Events.ProgressionTableName, "Alignment:All", 2, false, upsert: false),
        nameof(TombstoneStreamOperation) => new TombstoneStreamOperation(Events, Guid.NewGuid(),
            StorageConstants.DefaultTenantId),
        nameof(SetStreamArchivedOperation) => new SetStreamArchivedOperation(Events, Guid.NewGuid(),
            StorageConstants.DefaultTenantId, true),
        nameof(NaturalKeyArchiveOperation) => new NaturalKeyArchiveOperation(
            $"[{Schema}].[pc_alignment_probe]", Guid.NewGuid(), isGuidStream: true),
        nameof(ExecuteSqlStorageOperation) => new ExecuteSqlStorageOperation(
            $"UPDATE {Events.ProgressionTableName} SET last_seq_id = last_seq_id WHERE name = ?", "Alignment:All"),
        _ => throw new ArgumentOutOfRangeException(nameof(key), key, "Unknown operation key")
    };

    /// <summary>
    ///     The zero half of the declaration, measured. One operation, then a sentinel command: count how
    ///     many result sets the reader walks through before reaching the sentinel. That count IS the
    ///     number the operation contributed, and for a pure-DML operation it has to be zero.
    /// </summary>
    [Theory]
    [MemberData(nameof(Operations))]
    public async Task an_operation_contributes_no_result_set(string description, string key)
    {
        await EnsureProbeTableAsync();

        var operation = Build(key);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        await using var batch = new SqlBatch(conn);
        var builder = new BatchBuilder(batch);
        operation.ConfigureCommand(builder);
        builder.StartNewCommand();
        builder.Append($"SELECT {Sentinel} AS marker;");
        builder.Compile();

        await using var reader = await batch.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        var contributed = 0;
        while (true)
        {
            if (await reader.ReadAsync(TestContext.Current.CancellationToken) &&
                reader.FieldCount > 0 && reader.GetName(0) == "marker")
            {
                reader.GetInt32(0).ShouldBe(Sentinel);
                break;
            }

            (await reader.NextResultAsync(TestContext.Current.CancellationToken)).ShouldBeTrue(
                $"the sentinel never appeared for {description}");
            contributed++;
        }

        contributed.ShouldBe(0,
            $"{description} contributed {contributed} result set(s). SQL Server's batch reader surfaces "
            + "only real result sets, so a DML operation must contribute none — see the class remarks.");
    }

    /// <summary>
    ///     A session's appends run in their own batch. Documents and events written by the SAME session
    ///     must both land, and the appended events must come back carrying their assigned sequences.
    /// </summary>
    [Fact]
    public async Task events_and_documents_in_one_session_do_not_share_a_reader()
    {
        var streamId = Guid.NewGuid();
        var docId = Guid.NewGuid();

        await using (var session = _store.LightweightSession())
        {
            // Queued FIRST, ahead of the append, so that this also holds if the two batches are ever
            // merged: the walk has to keep the append's result set apart from these upserts' sets.
            session.Store(new AlignmentDoc { Id = docId, Name = "first" });
            session.Store(new AlignmentDoc { Id = Guid.NewGuid(), Name = "second" });
            session.Delete<AlignmentDoc>(Guid.NewGuid());

            session.Events.StartStream(streamId, new AlignmentHappened(1), new AlignmentHappened(2));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = _store.QuerySession();

        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(2);
        events.ShouldAllBe(e => e.Sequence > 0);
        events.Select(e => e.Sequence).Distinct().Count().ShouldBe(2);

        (await query.LoadAsync<AlignmentDoc>(docId, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("first");
    }

    /// <summary>
    ///     The coverage guard. Every type in the Polecat assembly that declares the
    ///     <see cref="NoDataReturnedCall" /> marker has to be listed here with a verdict, so a new one
    ///     cannot be added without someone confirming its SQL really returns nothing (marten#5210).
    /// </summary>
    [Fact]
    public void every_no_data_marked_operation_type_is_audited()
    {
        var audited = new HashSet<string>
        {
            // Emits a single DELETE (hard) or UPDATE (soft) with no OUTPUT clause.
            "Polecat.Storage.ClosedShape.ClosedShapeDeletion`2",

            // A change-set projection of a deletion that already happened. It is never configured into
            // a command -- it exists only so IChangeSet can report what was deleted.
            "Polecat.Services.ChangeSet+Deletion",

            // #831. Every one of these is plain DML with no OUTPUT clause and no trailing SELECT, and
            // operation_result_set_census_tests measures each against a sentinel.
            "Polecat.Events.Operations.AssignTagWhereOperation",
            "Polecat.Events.Projections.NaturalKeyArchiveOperation",
            "Polecat.Events.Projections.NaturalKeyRetireOperation",
            "Polecat.Events.Projections.NaturalKeyUpsertOperation",
            "Polecat.Events.Protected.DeleteEventsOperation",
            "Polecat.Events.Protected.OverwriteEventOperation",
            "Polecat.Events.Protected.ReplaceEventOperation",
            "Polecat.Events.Protected.SetCompactedVersionOperation",
            "Polecat.Internal.Operations.DeleteWhereOperation",
            "Polecat.Internal.Operations.RecordProgressionOperation",
            "Polecat.Internal.Operations.SetStreamArchivedOperation",
            "Polecat.Internal.Operations.TombstoneStreamOperation",
            "Polecat.Internal.Operations.UnDeleteByIdOperation",
            "Polecat.Internal.Operations.UndoDeleteWhereOperation",
            "Polecat.Patching.PatchOperation",
            "Polecat.Projections.Flattened.FlatTableSqlOperation",

            // Declared as returning nothing because QueueSqlCommand's contract says so. This is the one
            // entry that rests on the caller rather than on SQL we can read: see IDocumentOperations.
            "Polecat.Internal.Operations.ExecuteSqlStorageOperation"
        };

        var discovered = typeof(DocumentStore).Assembly
            .GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false })
            .Where(t => typeof(NoDataReturnedCall).IsAssignableFrom(t))
            .Select(t => t.FullName!.Split('[')[0])
            .Distinct()
            .ToArray();

        discovered.ShouldNotBeEmpty("the reflection sweep found nothing, so it is not auditing anything");

        var unaudited = discovered.Where(x => !audited.Contains(x)).ToArray();

        unaudited.ShouldBeEmpty(
            "a new NoDataReturnedCall operation was added without auditing its SQL. Confirm it returns no "
            + "result set, then add it to the audited set above: " + string.Join(", ", unaudited));
    }

    private async Task EnsureProbeTableAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID('[{Schema}].[pc_alignment_probe]', 'U') IS NULL
                CREATE TABLE [{Schema}].[pc_alignment_probe] (
                    natural_key_value nvarchar(200) NOT NULL PRIMARY KEY,
                    stream_id uniqueidentifier NOT NULL,
                    is_archived bit NOT NULL DEFAULT 0);
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

public class AlignmentDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record AlignmentHappened(int Number);
