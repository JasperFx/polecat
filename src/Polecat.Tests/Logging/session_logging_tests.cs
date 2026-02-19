using Microsoft.Data.SqlClient;
using Polecat.Logging;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Logging;

[Collection("integration")]
public class session_logging_tests : IntegrationContext
{
    public session_logging_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task request_count_increments_on_load()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Counter" });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        query.RequestCount.ShouldBe(0);

        await query.LoadAsync<User>(id);
        query.RequestCount.ShouldBe(1);

        await query.LoadAsync<User>(id);
        query.RequestCount.ShouldBe(2);
    }

    [Fact]
    public async Task request_count_increments_on_load_many()
    {
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        theSession.Store(new User { Id = id1, FirstName = "A" });
        theSession.Store(new User { Id = id2, FirstName = "B" });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        await query.LoadManyAsync<User>([id1, id2]);

        query.RequestCount.ShouldBe(1);
    }

    [Fact]
    public async Task request_count_increments_on_load_json()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Json" });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        await query.LoadJsonAsync<User>(id);

        query.RequestCount.ShouldBe(1);
    }

    [Fact]
    public async Task session_logger_receives_before_and_success()
    {
        var logger = new RecordingSessionLogger();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "session_log";
            opts.Logger = new RecordingLogger(logger);
        });

        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new User { Id = id, FirstName = "Logged" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        query.Logger = logger;
        await query.LoadAsync<User>(id);

        logger.BeforeExecuteCount.ShouldBeGreaterThan(0);
        logger.SuccessCount.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task session_logger_records_saved_changes()
    {
        var logger = new RecordingSessionLogger();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "session_save_log";
            opts.Logger = new RecordingLogger(logger);
        });

        await using var session = theStore.LightweightSession();
        session.Store(new User { Id = Guid.NewGuid(), FirstName = "SaveLog" });
        await session.SaveChangesAsync();

        logger.SavedChangesCount.ShouldBe(1);
    }

    [Fact]
    public async Task store_level_logger_creates_session_logger()
    {
        var storeLogger = new RecordingLogger();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "store_logger";
            opts.Logger = storeLogger;
        });

        await using var session = theStore.LightweightSession();
        session.Store(new User { Id = Guid.NewGuid(), FirstName = "Store" });
        await session.SaveChangesAsync();

        storeLogger.SessionsCreated.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task per_session_logger_override()
    {
        var customLogger = new RecordingSessionLogger();

        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Override" });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        query.Logger = customLogger;
        await query.LoadAsync<User>(id);

        customLogger.SuccessCount.ShouldBe(1);
    }

    private class RecordingLogger : IPolecatLogger
    {
        private readonly RecordingSessionLogger? _sharedLogger;
        public int SessionsCreated { get; private set; }

        public RecordingLogger(RecordingSessionLogger? sharedLogger = null)
        {
            _sharedLogger = sharedLogger;
        }

        public IPolecatSessionLogger StartSession(IQuerySession session)
        {
            SessionsCreated++;
            return _sharedLogger ?? new RecordingSessionLogger();
        }
    }

    private class RecordingSessionLogger : IPolecatSessionLogger
    {
        public int BeforeExecuteCount { get; private set; }
        public int SuccessCount { get; private set; }
        public int FailureCount { get; private set; }
        public int SavedChangesCount { get; private set; }

        public void OnBeforeExecute(SqlCommand command) => BeforeExecuteCount++;
        public void LogSuccess(SqlCommand command) => SuccessCount++;
        public void LogFailure(SqlCommand command, Exception ex) => FailureCount++;
        public void RecordSavedChanges(IDocumentSession session) => SavedChangesCount++;
    }
}
