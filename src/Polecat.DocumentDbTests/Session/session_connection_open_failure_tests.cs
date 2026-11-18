using System.Data;
using System.Diagnostics;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

/// <summary>
///     gh-1003, Polecat's sibling of marten#5881: a cancelled connection open used to wedge the session.
///     <c>TransactionalConnection.EnsureConnectionOpenAsync</c> assigned its field before <c>OpenAsync</c>
///     and only ever opened while the field was null, so the closed connection a cancelled open left behind
///     was never opened again and every later command or transaction failed with "The connection is closed."
/// </summary>
[Collection("integration")]
public class a_cancelled_connection_open_does_not_wedge_the_session : IntegrationContext
{
    public a_cancelled_connection_open_does_not_wedge_the_session(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // The internal BeginTransactionAsync is the direct route to the open (a public call may check its token
    // earlier and never reach it); FetchForWriting(forUpdate) and the exclusive appends call exactly this.
    [Fact]
    public async Task the_next_save_on_the_same_session_succeeds()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var session = theStore.LightweightSession();

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await ((DocumentSessionBase)session).BeginTransactionAsync(cancelled.Token));

        var doc = new Target { Id = Guid.NewGuid(), Color = "after a cancelled open" };
        session.Store(doc);
        await session.SaveChangesAsync(ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(doc.Id, ct)).ShouldNotBeNull();
    }
}

/// <summary>
///     gh-1003 / marten#5881: <c>DocumentStore.OpenSessionAsync</c> opens a connection and begins a
///     transaction (for any isolation level but ReadCommitted) before the caller has a session to dispose,
///     so when beginning the transaction fails or is cancelled the factory itself has to give the
///     connection back. It used to have no try/catch there at all.
/// </summary>
/// <remarks>
///     Each test uses a store whose connection string carries an Application Name unique to the test,
///     clears that pool (which closes every connection that was given back), and then counts the server
///     sessions still carrying the name. A leaked connection was never given back, so it is still there.
/// </remarks>
public class open_session_async_gives_its_connection_back_on_failure
{
    private readonly string _app = "gh1003_" + Guid.NewGuid().ToString("N")[..8];

    private string ConnectionString =>
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { ApplicationName = _app }.ConnectionString;

    private DocumentStore BuildStore() => new(new StoreOptions
    {
        ConnectionString = ConnectionString,
        AutoCreateSchemaObjects = AutoCreate.All,
        DatabaseSchemaName = "gh1003_open_session",
        UseNativeJsonType = ConnectionSource.SupportsNativeJson
    });

    // Deterministic stand-in for "beginning the transaction failed after the connection opened": SqlClient
    // refuses IsolationLevel.Chaos in BeginTransaction, which is after the lifetime has opened its connection.
    [Fact]
    public async Task when_beginning_the_transaction_fails()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = BuildStore();

        IDocumentSession? session = null;
        Exception? thrown = null;
        try
        {
            session = await store.OpenSessionAsync(new SessionOptions { IsolationLevel = IsolationLevel.Chaos }, ct);
        }
        catch (Exception e)
        {
            thrown = e;
        }

        if (session != null) await session.DisposeAsync();

        thrown.ShouldNotBeNull("SqlClient began a Chaos transaction, so this test never failed the begin");
        await ShouldHaveNoServerSessionsAsync(ct);
    }

    // marten#5881's own case: the caller's token is cancelled after the connection opened. Cancelled from
    // SqlClient's WriteConnectionOpenAfter diagnostic event for this test's Application Name only, as
    // Marten's repro does from Npgsql's OpenedConnection log event, so no delay decides the timing.
    [Fact]
    public async Task when_cancelled_after_the_open()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = BuildStore();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var fired = 0;
        using (new SqlConnectionOpenedObserver(_app, () =>
               {
                   Interlocked.Exchange(ref fired, 1);
                   try
                   {
                       cts.Cancel();
                   }
                   catch (AggregateException)
                   {
                       // A registration's callback failing is not what this test is about
                   }
               }))
        {
            IDocumentSession? session = null;
            Exception? thrown = null;
            try
            {
                session = await store.OpenSessionAsync(
                    new SessionOptions { IsolationLevel = IsolationLevel.Serializable }, cts.Token);
            }
            catch (Exception e)
            {
                thrown = e;
            }

            if (session != null) await session.DisposeAsync();

            fired.ShouldBe(1, "SqlClient's WriteConnectionOpenAfter event never fired, so nothing was cancelled");
            thrown.ShouldBeAssignableTo<OperationCanceledException>();
        }

        await ShouldHaveNoServerSessionsAsync(ct);
    }

    private async Task ShouldHaveNoServerSessionsAsync(CancellationToken ct)
    {
        using (var pooled = new SqlConnection(ConnectionString))
        {
            SqlConnection.ClearPool(pooled);
        }

        var clock = Stopwatch.StartNew();
        int count;
        do
        {
            await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name = @app;";
            cmd.Parameters.AddWithValue("@app", _app);
            count = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
            if (count == 0) return;

            await Task.Delay(100, ct);
        } while (clock.Elapsed < TimeSpan.FromSeconds(5));

        count.ShouldBe(0, $"{count} connection(s) for '{_app}' were never given back to the pool");
    }

    /// <summary>
    ///     Calls back when SqlClient finishes opening a connection whose Application Name matches.
    /// </summary>
    private sealed class SqlConnectionOpenedObserver :
        IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>, IDisposable
    {
        private readonly string _app;
        private readonly Action _onOpened;
        private readonly object _gate = new();
        private readonly List<IDisposable> _subscriptions = new();
        private readonly IDisposable _all;

        public SqlConnectionOpenedObserver(string app, Action onOpened)
        {
            _app = app;
            _onOpened = onOpened;
            _all = DiagnosticListener.AllListeners.Subscribe(this);
        }

        void IObserver<DiagnosticListener>.OnNext(DiagnosticListener listener)
        {
            if (listener.Name != "SqlClientDiagnosticListener") return;

            lock (_gate)
            {
                _subscriptions.Add(listener.Subscribe(this));
            }
        }

        void IObserver<KeyValuePair<string, object?>>.OnNext(KeyValuePair<string, object?> value)
        {
            if (!value.Key.EndsWith("WriteConnectionOpenAfter", StringComparison.Ordinal)) return;

            var connection = value.Value?.GetType().GetProperty("Connection")?.GetValue(value.Value) as SqlConnection;
            if (connection is null) return;

            if (new SqlConnectionStringBuilder(connection.ConnectionString).ApplicationName != _app) return;

            _onOpened();
        }

        void IObserver<DiagnosticListener>.OnError(Exception error)
        {
        }

        void IObserver<DiagnosticListener>.OnCompleted()
        {
        }

        void IObserver<KeyValuePair<string, object?>>.OnError(Exception error)
        {
        }

        void IObserver<KeyValuePair<string, object?>>.OnCompleted()
        {
        }

        public void Dispose()
        {
            _all.Dispose();
            lock (_gate)
            {
                foreach (var subscription in _subscriptions) subscription.Dispose();
                _subscriptions.Clear();
            }
        }
    }
}
