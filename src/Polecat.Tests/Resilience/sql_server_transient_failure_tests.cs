using System.Data.Common;
using System.Reflection;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Polecat.Resilience;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Resilience;

/// <summary>
///     jasperfx#1016 / wolverine#4986: <see cref="ShardFailure.IsTransient" /> must be true for a
///     SQL Server fault that clears on its own and false for a permanent one. SqlClient never sets
///     <see cref="DbException.IsTransient" />, so without Polecat's classifier a timeout or a deadlock
///     would read as permanent.
/// </summary>
/// <remarks>
///     ⚠️ This class deliberately touches <see cref="SqlServerTransientFailures" /> only through a
///     <see cref="DocumentStore" />: accessing any member of that type runs its static constructor,
///     which registers the classifier by itself and would hide a store that forgot to.
/// </remarks>
public class sql_server_transient_failure_tests
{
    public sql_server_transient_failure_tests()
    {
        // Building a store is what registers the classifier; nothing here connects
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
    }

    // ---- built SqlExceptions ----------------------------------------------------------------------

    [Theory]
    [InlineData(-2)]     // client timeout
    [InlineData(1205)]   // deadlock victim
    [InlineData(1222)]   // lock request timeout
    [InlineData(40613)]  // Azure: database not currently available
    [InlineData(10054)]  // connection reset by the remote host
    public void a_transient_sql_error_is_transient(int number)
    {
        var exception = SqlExceptions.WithErrors(number);

        // The reason the classifier exists: SqlClient itself says no
        ((DbException)exception).IsTransient.ShouldBeFalse();

        TransientFailures.IsTransient(exception).ShouldBeTrue();
    }

    [Theory]
    [InlineData(207)]    // invalid column name
    [InlineData(208)]    // invalid object name
    [InlineData(2627)]   // primary key violation
    [InlineData(102)]    // syntax error
    public void a_permanent_sql_error_is_not_transient(int number)
    {
        TransientFailures.IsTransient(SqlExceptions.WithErrors(number)).ShouldBeFalse();
    }

    [Fact]
    public void one_transient_error_among_several_is_enough()
    {
        // The first error is what SqlException.Number reports, and it is the permanent one here
        TransientFailures.IsTransient(SqlExceptions.WithErrors(208, 1205)).ShouldBeTrue();
        TransientFailures.IsTransient(SqlExceptions.WithErrors(207, 208)).ShouldBeFalse();
    }

    [Fact]
    public void a_transient_sql_error_wrapped_in_another_exception_is_transient()
    {
        var wrapped = new InvalidOperationException("the projection failed",
            SqlExceptions.WithErrors(1205));

        TransientFailures.IsTransient(wrapped).ShouldBeTrue();
        TransientFailures.IsTransient(new AggregateException(new Exception("other"), wrapped))
            .ShouldBeTrue();
    }

    [Fact]
    public void a_permanent_sql_error_does_not_hide_a_transient_inner_exception()
    {
        // The classifier answers false for the 208, but TransientFailures keeps walking, so the
        // TimeoutException inside it still makes the whole failure transient
        var exception = SqlExceptions.WithErrors(new TimeoutException("pool exhausted"), 208);

        TransientFailures.IsTransient(exception).ShouldBeTrue();
    }

    [Fact]
    public void a_permanent_sql_error_wrapping_nothing_transient_is_not_transient()
    {
        var exception = SqlExceptions.WithErrors(new InvalidOperationException("nope"), 208);

        TransientFailures.IsTransient(exception).ShouldBeFalse();
    }

    // ---- real SQL Server ----------------------------------------------------------------------------

    [Fact]
    public async Task a_real_command_timeout_is_transient()
    {
        var exception = await captureAsync("waitfor delay '00:00:05'", commandTimeout: 1);

        exception.Number.ShouldBe(-2);

        // Negative control: neither SqlClient nor the Win32Exception SqlClient wraps inside a
        // timeout ("The wait operation timed out") reads as transient by JasperFx's built-in rules
        ((DbException)exception).IsTransient.ShouldBeFalse();
        exception.InnerException.ShouldNotBeOfType<TimeoutException>();

        TransientFailures.IsTransient(exception).ShouldBeTrue();

        // And end to end, the label the supervisor reads
        ShardFailure.For(exception, DateTimeOffset.UtcNow).IsTransient.ShouldBeTrue();
        ShardFailure.For(new InvalidOperationException("projection failed", exception),
            DateTimeOffset.UtcNow).IsTransient.ShouldBeTrue();
    }

    [Fact]
    public async Task a_real_missing_column_is_not_transient()
    {
        var exception = await captureAsync("select no_such_column from sys.objects");

        exception.Number.ShouldBe(207);

        TransientFailures.IsTransient(exception).ShouldBeFalse();
        ShardFailure.For(exception, DateTimeOffset.UtcNow).IsTransient.ShouldBeFalse();
    }

    [Fact]
    public async Task a_real_deadlock_is_transient()
    {
        var token = TestContext.Current.CancellationToken;
        var table = $"dbo.pc_transient_deadlock_{Guid.NewGuid():N}";

        await using (var setup = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await setup.OpenAsync(token);
            await executeAsync(setup, null,
                $"create table {table} (id int primary key, value int); insert into {table} values (1, 0), (2, 0);",
                token);
        }

        try
        {
            await using var one = new SqlConnection(ConnectionSource.ConnectionString);
            await using var two = new SqlConnection(ConnectionSource.ConnectionString);
            await one.OpenAsync(token);
            await two.OpenAsync(token);

            await using var txOne = (SqlTransaction)await one.BeginTransactionAsync(token);
            await using var txTwo = (SqlTransaction)await two.BeginTransactionAsync(token);

            // Each takes one row, then reaches for the other's, so SQL Server must pick a victim
            await executeAsync(one, txOne, $"update {table} set value = 1 where id = 1", token);
            await executeAsync(two, txTwo, $"update {table} set value = 2 where id = 2", token);

            var crossOne = captureOrNullAsync(() =>
                executeAsync(one, txOne, $"update {table} set value = 1 where id = 2", token));
            var crossTwo = captureOrNullAsync(() =>
                executeAsync(two, txTwo, $"update {table} set value = 2 where id = 1", token));

            var failures = await Task.WhenAll(crossOne, crossTwo);

            var victim = failures.Single(x => x != null).ShouldBeOfType<SqlException>();
            victim.Number.ShouldBe(1205);

            ((DbException)victim).IsTransient.ShouldBeFalse();
            TransientFailures.IsTransient(victim).ShouldBeTrue();
            ShardFailure.For(victim, DateTimeOffset.UtcNow).IsTransient.ShouldBeTrue();
        }
        finally
        {
            await using var cleanup = new SqlConnection(ConnectionSource.ConnectionString);
            await cleanup.OpenAsync(token);
            await executeAsync(cleanup, null, $"drop table if exists {table}", token);
        }
    }

    private static async Task<SqlException> captureAsync(string sql, int commandTimeout = 30)
    {
        var token = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(token);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = commandTimeout };

        return await Should.ThrowAsync<SqlException>(() => command.ExecuteNonQueryAsync(token));
    }

    private static async Task<Exception?> captureOrNullAsync(Func<Task> action)
    {
        try
        {
            await Task.Run(action);
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static async Task executeAsync(SqlConnection connection, SqlTransaction? transaction, string sql,
        CancellationToken token)
    {
        await using var command = new SqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(token);
    }
}

public class sql_server_transient_failure_registration_tests
{
    [Fact]
    public void the_classifier_is_registered_once_however_many_stores_are_built()
    {
        using var one = DocumentStore.For(ConnectionSource.ConnectionString);
        using var two = DocumentStore.For(ConnectionSource.ConnectionString);
        SqlServerTransientFailures.EnsureRegistered();

        var classifiers = (Func<Exception, bool?>[])typeof(TransientFailures)
            .GetField("_classifiers", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

        classifiers.Count(x => x.Method.DeclaringType == typeof(SqlServerTransientFailures)).ShouldBe(1);
    }

    [Fact]
    public void anything_but_a_sql_exception_is_left_to_the_built_in_rules()
    {
        SqlServerTransientFailures.Classify(new TimeoutException()).ShouldBeNull();
        SqlServerTransientFailures.Classify(new InvalidOperationException()).ShouldBeNull();
    }

    [Fact]
    public void the_set_carries_sqlclients_own_baseline()
    {
        foreach (var number in SqlConfigurableRetryFactory.BaselineTransientErrors)
        {
            SqlServerTransientFailures.TransientErrorNumbers.ShouldContain(number);
        }
    }
}

/// <summary>
///     <see cref="SqlException" /> has no public constructor. These are the internal factories
///     Microsoft.Data.SqlClient 7.0.0 uses itself: <c>SqlError</c>'s eight-argument constructor,
///     <c>SqlErrorCollection.Add</c> and <c>SqlException.CreateException(SqlErrorCollection, string,
///     Guid, Exception)</c>.
/// </summary>
internal static class SqlExceptions
{
    private const BindingFlags Internal = BindingFlags.NonPublic | BindingFlags.Instance;

    public static SqlException WithErrors(params int[] numbers) => WithErrors(null, numbers);

    public static SqlException WithErrors(Exception? inner, params int[] numbers)
    {
        var errors = (SqlErrorCollection)Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
        var add = typeof(SqlErrorCollection).GetMethod("Add", Internal, [typeof(SqlError)])!;

        var errorCtor = typeof(SqlError).GetConstructor(Internal,
        [
            typeof(int), typeof(byte), typeof(byte), typeof(string), typeof(string), typeof(string),
            typeof(int), typeof(Exception)
        ])!;

        foreach (var number in numbers)
        {
            var error = errorCtor.Invoke([number, (byte)0, (byte)16, "server", $"error {number}", "", 0, null]);
            add.Invoke(errors, [error]);
        }

        var create = typeof(SqlException).GetMethod("CreateException", BindingFlags.NonPublic | BindingFlags.Static,
            [typeof(SqlErrorCollection), typeof(string), typeof(Guid), typeof(Exception)])!;

        return (SqlException)create.Invoke(null, [errors, "17.0", Guid.Empty, inner])!;
    }
}
