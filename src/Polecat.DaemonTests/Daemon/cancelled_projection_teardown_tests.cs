using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.Tests.Projections;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #896: a cancelled rebuild has to reach the caller as <see cref="OperationCanceledException" />, because
///     that is all a caller catches, and SqlClient reports a cancelled command in forms that are not.
/// </summary>
/// <remarks>
///     A rebuild cancelled at a random moment reaches these forms only now and then (11 in 84 for the store
///     global teardown), so each fact here parks its command on a lock first. Cancelling then lands inside
///     the command every time.
/// </remarks>
public class cancelled_projection_teardown_tests : IAsyncLifetime
{
    private static string NewSchema() => "cpt_" + Guid.NewGuid().ToString("N")[..10];

    private readonly string _schema = NewSchema();
    private DocumentStore? _store;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private DocumentStore Store => _store ??= DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = _schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
    });

    private IEventStore<IDocumentSession, IQuerySession> EventStore => Store;

    private string ProjectionName => Store.Options.Projections.All.Single().Name;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        _store?.Dispose();
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            IF SCHEMA_ID(@schema) IS NOT NULL SET @sql = @sql + 'DROP SCHEMA [' + @schema + '];';
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", _schema);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     Holds an exclusive lock on a table until disposed, so any command that needs it parks there.
    /// </summary>
    private sealed class TableLock : IAsyncDisposable
    {
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;

        private TableLock(SqlConnection connection, SqlTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }

        public static async Task<TableLock> TakeAsync(string table)
        {
            var connection = new SqlConnection(ConnectionSource.ConnectionString);
            await connection.OpenAsync(Ct);
            var transaction = (SqlTransaction)await connection.BeginTransactionAsync(Ct);

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = $"SELECT TOP (0) 1 FROM {table} WITH (TABLOCKX, HOLDLOCK);";
            await cmd.ExecuteNonQueryAsync(Ct);

            return new TableLock(connection, transaction);
        }

        public async ValueTask DisposeAsync()
        {
            await _transaction.RollbackAsync();
            await _connection.DisposeAsync();
        }
    }

    private static async Task ShouldReportCancellationAsync(Func<CancellationToken, Task> action)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

        var ex = await sql_cancellation_tests.CaughtFrom(action(cancellation.Token));

        ex.ShouldBeAssignableTo<OperationCanceledException>();
        ((OperationCanceledException)ex).CancellationToken.ShouldBe(cancellation.Token);
    }

    [Fact]
    public async Task a_store_global_teardown_cancelled_inside_its_command_reports_cancellation()
    {
        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var blocked = await TableLock.TakeAsync(Store.Events.ProgressionTableName);

        await ShouldReportCancellationAsync(token =>
            EventStore.TeardownExistingProjectionStateAsync(Store.Database, ProjectionName, token));
    }

    [Fact]
    public async Task deleting_store_global_progress_cancelled_inside_its_command_reports_cancellation()
    {
        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var blocked = await TableLock.TakeAsync(Store.Events.ProgressionTableName);

        await ShouldReportCancellationAsync(token =>
            EventStore.DeleteProjectionProgressAsync(Store.Database, ProjectionName, token));
    }

    /// <summary>
    ///     Unset, the memo from #868 is what sends a rebuild down this path, after
    ///     <c>ResetSchemaExistenceChecks</c> or when the migration lock was not attained.
    /// </summary>
    [Fact]
    public async Task ensuring_storage_cancelled_inside_its_commands_reports_cancellation()
    {
        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        Store.Database.ResetSchemaExistenceChecks();

        // Parks the ensure on the sp_getapplock it runs first, the way another process migrating would.
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        (await conn.TryGetGlobalLock($"polecat:migrate:{_schema}", cancellation: Ct)).ShouldBeTrue();

        await ShouldReportCancellationAsync(token =>
            Store.Database.EnsureStorageExistsAsync(typeof(IEvent), token));
    }

    /// <summary>
    ///     The other half of the rule. Teardown against a table that is not there is a real failure, and it
    ///     has to stay one.
    /// </summary>
    [Fact]
    public async Task a_failure_on_a_token_that_was_not_cancelled_is_left_alone()
    {
        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE {Store.Events.ProgressionTableName};";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        using var cancellation = new CancellationTokenSource();

        await Should.ThrowAsync<SqlException>(() =>
            EventStore.TeardownExistingProjectionStateAsync(Store.Database, ProjectionName, cancellation.Token));
        await Should.ThrowAsync<SqlException>(() =>
            EventStore.DeleteProjectionProgressAsync(Store.Database, ProjectionName, cancellation.Token));
    }
}

public class sql_cancellation_tests
{
    /// <summary>
    ///     SqlClient's <c>SQL.OperationCancelled()</c>, for a command cancelled before it reaches the server.
    ///     The window is too small to aim a real command at, and this is the form the sweep in #896 saw five
    ///     times out of seventy eight.
    /// </summary>
    private static InvalidOperationException CancelledBeforeReachingTheServer() => new("Operation cancelled by user.");

    /// <summary>
    ///     Not <c>Should.ThrowAsync</c>, which reads a task that ended in an
    ///     <see cref="OperationCanceledException" /> as canceled and hands back a new exception, when what
    ///     happened to the original is the question.
    /// </summary>
    internal static async Task<Exception> CaughtFrom(Task task)
    {
        try
        {
            await task;
        }
        catch (Exception ex)
        {
            return ex;
        }

        throw new ShouldAssertException("Nothing was thrown");
    }

    private static async Task Throwing(Exception ex)
    {
        await Task.Yield();
        throw ex;
    }

    private static async Task<int> ThrowingResult(Exception ex)
    {
        await Task.Yield();
        throw ex;
    }

    [Fact]
    public async Task an_exception_from_a_cancelled_token_becomes_a_cancellation_carrying_it()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        var inner = CancelledBeforeReachingTheServer();

        var ex = (await CaughtFrom(Throwing(inner).TranslatingCancellation("Teardown", cancellation.Token)))
            .ShouldBeOfType<OperationCanceledException>();
        ex.InnerException.ShouldBeSameAs(inner);
        ex.CancellationToken.ShouldBe(cancellation.Token);
        ex.Message.ShouldBe("Teardown was cancelled.");

        var withResult = await CaughtFrom(ThrowingResult(inner).TranslatingCancellation("Teardown", cancellation.Token));
        withResult.ShouldBeOfType<OperationCanceledException>().InnerException.ShouldBeSameAs(inner);
    }

    [Fact]
    public async Task nothing_is_translated_while_the_token_has_not_been_cancelled()
    {
        using var cancellation = new CancellationTokenSource();
        var inner = CancelledBeforeReachingTheServer();

        (await CaughtFrom(Throwing(inner).TranslatingCancellation("Teardown", cancellation.Token)))
            .ShouldBeSameAs(inner);

        (await CaughtFrom(ThrowingResult(inner).TranslatingCancellation("Teardown", cancellation.Token)))
            .ShouldBeSameAs(inner);

        (await CaughtFrom(Throwing(inner).TranslatingCancellation("Teardown", CancellationToken.None)))
            .ShouldBeSameAs(inner);
    }

    [Fact]
    public async Task a_cancellation_is_not_wrapped_in_another_one()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        var inner = new OperationCanceledException(cancellation.Token);

        (await CaughtFrom(Throwing(inner).TranslatingCancellation("Teardown", cancellation.Token)))
            .ShouldBeSameAs(inner);
    }

    [Fact]
    public async Task success_passes_through()
    {
        (await Task.FromResult(5).TranslatingCancellation("Teardown", CancellationToken.None)).ShouldBe(5);
    }
}
