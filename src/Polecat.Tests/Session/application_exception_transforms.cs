using JasperFx.Events;
using JasperFx.Core.Exceptions;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Services;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Session;

public class GuardRefusedException : Exception
{
    public GuardRefusedException(SqlException inner) : base("The guard refused the write", inner)
    {
    }
}

/// <summary>
///     #849, the Polecat half of marten#5750. An application registers its own transforms on
///     <see cref="StoreOptions.ExceptionTransforms" />, so a guard's own error number can surface from
///     a save as a typed exception instead of a <see cref="SqlException" />.
/// </summary>
public class application_exception_transforms : OneOffConfigurationsContext
{
    private const int GuardError = 51850;
    private const int UniqueKeyViolation = 2627;
    private const int CheckConstraintViolation = 547;

    private static readonly string Guard = $"THROW {GuardError}, 'refused by the guard', 1";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public class Note
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private void RegisterGuardTransform(StoreOptions opts)
        => opts.ExceptionTransforms.IfExceptionIs<SqlException>()
            .If(e => e.Number == GuardError)
            .ThenTransformTo(e => new GuardRefusedException(e));

    [Fact]
    public async Task a_guard_queued_in_the_committing_batch_surfaces_as_the_applications_exception()
    {
        ConfigureStore(RegisterGuardTransform);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession();
        var note = new Note { Id = Guid.NewGuid() };
        session.Store(note);
        session.QueueSqlCommand(Guard);

        var ex = await Should.ThrowAsync<GuardRefusedException>(() => session.SaveChangesAsync(Ct));
        ex.InnerException.ShouldBeOfType<SqlException>().Number.ShouldBe(GuardError);

        // Translating the failure did not change what it did: the save is rolled back.
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Note>(note.Id, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_guard_queued_from_a_session_listener_surfaces_as_the_applications_exception()
    {
        ConfigureStore(RegisterGuardTransform);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession(new SessionOptions
        {
            Listeners = { new GuardingListener() }
        });
        session.Store(new Note { Id = Guid.NewGuid() });

        await Should.ThrowAsync<GuardRefusedException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task an_error_the_application_does_not_claim_is_reported_as_it_was_before()
    {
        ConfigureStore(RegisterGuardTransform);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession();
        session.QueueSqlCommand("SELECT no_such_column FROM sys.tables");

        var ex = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        ex.Number.ShouldNotBe(GuardError);
    }

    [Fact]
    public async Task without_a_registration_the_guard_is_a_sql_exception_as_before()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession();
        session.QueueSqlCommand(Guard);

        var ex = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        ex.Number.ShouldBe(GuardError);
    }

    [Fact]
    public async Task transforms_belong_to_the_store_that_registered_them()
    {
        ConfigureStore(RegisterGuardTransform);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        using var other = DocumentStore.For(opts =>
        {
            opts.ConnectionString = theStore.Options.ConnectionString;
            opts.DatabaseSchemaName = theStore.Options.DatabaseSchemaName;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        await using var session = other.LightweightSession();
        session.QueueSqlCommand(Guard);

        (await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct))).Number.ShouldBe(GuardError);
    }

    /// <summary>
    ///     2627 while an insert is queued, which is what Polecat reports as
    ///     <see cref="DocumentAlreadyExistsException" />. Raised by a queued statement because the
    ///     document path's own MERGE turns a collision into zero rows.
    /// </summary>
    private async Task<IDocumentSession> QueueACollisionBesideAnInsertAsync()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var table = $"[{theStore.Options.DatabaseSchemaName}].[transform_collision]";
        await ExecuteAsync($"CREATE TABLE {table} (id int PRIMARY KEY); INSERT INTO {table} VALUES (1);");

        var session = theStore.LightweightSession();
        session.QueueSqlCommand($"INSERT INTO {table} VALUES (1)");
        session.Insert(new Note { Id = Guid.NewGuid() });
        return session;
    }

    [Fact]
    public async Task a_transform_that_matches_an_error_polecat_translates_replaces_that_translation()
    {
        ConfigureStore(opts => opts.ExceptionTransforms.AddTransform(new DuplicateClaim()));

        await using var session = await QueueACollisionBesideAnInsertAsync();

        await Should.ThrowAsync<DuplicateClaimedException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task polecats_translation_stands_when_no_transform_claims_the_error()
    {
        ConfigureStore(RegisterGuardTransform);

        await using var session = await QueueACollisionBesideAnInsertAsync();

        await Should.ThrowAsync<DocumentAlreadyExistsException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task an_error_the_event_append_raises_reaches_the_applications_transforms()
    {
        ConfigureStore(opts => opts.ExceptionTransforms.IfExceptionIs<SqlException>()
            .If(e => e.Number == CheckConstraintViolation)
            .ThenTransformTo(e => new GuardRefusedException(e)));

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await ExecuteAsync($"""
            ALTER TABLE [{theStore.Options.DatabaseSchemaName}].[pc_events]
            WITH NOCHECK ADD CONSTRAINT ck_refuse_every_event CHECK (version < 0);
            """);

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("refused"));

        await Should.ThrowAsync<GuardRefusedException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     The operation that failed knows the most, so it goes first: this is the same collision
    ///     <see cref="a_transform_that_matches_an_error_polecat_translates_replaces_that_translation" />
    ///     claims, but on an append, whose operation names it itself.
    /// </summary>
    [Fact]
    public async Task an_operations_own_transform_goes_before_the_applications()
    {
        ConfigureStore(opts => opts.ExceptionTransforms.AddTransform(new DuplicateClaim()));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var stream = Guid.NewGuid();
        await using (var seed = theStore.LightweightSession())
        {
            seed.Events.StartStream(stream, new QuestStarted("first"));
            await seed.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(stream, new QuestStarted("second"));

        await Should.ThrowAsync<ExistingStreamIdCollisionException>(() => session.SaveChangesAsync(Ct));
    }

    public class DuplicateClaimedException : Exception;

    private sealed class DuplicateClaim : IExceptionTransform
    {
        public bool TryTransform(Exception original, out Exception? transformed)
        {
            if (original is SqlException { Number: UniqueKeyViolation })
            {
                transformed = new DuplicateClaimedException();
                return true;
            }

            transformed = null;
            return false;
        }
    }

    private sealed class GuardingListener : IDocumentSessionListener
    {
        public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            session.QueueSqlCommand(Guard);
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }
}
