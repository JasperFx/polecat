using JasperFx.Events;
using JasperFx.Events.Grouping;
using JasperFx.Events.Projections;
using Polecat.Events.Aggregation;
using Polly;
using Polly.Retry;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Thrown by <see cref="FailsOnceOutbox" />, and the only thing the test's retry strategy
///     handles, so the retry under test can only be the one this test asked for.
/// </summary>
public class TransientFailureAfterTheFlush : Exception;

/// <summary>
///     Fails the first <see cref="BeforeCommitAsync" />. That hook runs inside the daemon's
///     transaction, after every transaction participant, which makes it the real seat of "a failure
///     after the flush": the EF rows have been written and the transaction has not committed.
/// </summary>
public class FailsOnceOutbox : IMessageOutbox, IMessageBatch
{
    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session) => new(this);

    public ValueTask PublishAsync<T>(T message, string tenantId) => ValueTask.CompletedTask;

    public ValueTask PublishAsync<T>(T message, MessageMetadata metadata) => ValueTask.CompletedTask;

    public Task BeforeCommitAsync(CancellationToken token)
    {
        return Interlocked.Increment(ref _attempts) == 1
            ? throw new TransientFailureAfterTheFlush()
            : Task.CompletedTask;
    }

    public Task AfterCommitAsync(CancellationToken token) => Task.CompletedTask;
}

/// <summary>
///     <see cref="OrderAggregate" /> plus one published message per slice, which is what gets the
///     daemon's batch to reach <see cref="FailsOnceOutbox" /> at all.
/// </summary>
public class PublishingOrderAggregate : OrderAggregate
{
    public override ValueTask RaiseSideEffects(IDocumentSession operations, IEventSlice<Order> slice)
    {
        slice.PublishMessage(new OrderProjected());
        return ValueTask.CompletedTask;
    }
}

public record OrderProjected;

/// <summary>
///     #846, the Polecat twin of marten#5603 on the daemon path.
///     <c>DbContextTransactionParticipant.BeforeCommitAsync</c> runs inside the block
///     <c>PolecatProjectionBatch.ExecuteAsync</c> hands to the resilience pipeline, and flushed with
///     <c>SaveChangesAsync(token)</c>, which accepts the change tracker as soon as the statements
///     run. With a user-configured retry, a failure between that flush and the commit rolled the rows
///     back while leaving a tracker that believed there was nothing left to save.
/// </summary>
public class Bug_846_daemon_retry_keeps_the_ef_row
{
    private const string Schema = "ef846_retry";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<(DocumentStore Store, FailsOnceOutbox Outbox)> CreateStoreAsync()
    {
        var outbox = new FailsOnceOutbox();

        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = Schema;

            opts.Events.MessageOutbox = outbox;

            opts.ConfigurePolly(pipeline => pipeline.AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<TransientFailureAfterTheFlush>(),
                MaxRetryAttempts = 2,
                Delay = TimeSpan.Zero
            }));

            opts.Projections.Add<PublishingOrderAggregate, Order, TestDbContext>(
                opts, new PublishingOrderAggregate(), ProjectionLifecycle.Async);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_orders",
            "ef_order_summaries");

        return (store, outbox);
    }

    [Fact]
    public async Task the_retried_batch_commits_the_new_row_the_first_attempt_rolled_back()
    {
        var (store, outbox) = await CreateStoreAsync();
        using var _ = store;

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "retried", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        // Not vacuous: the first attempt really failed after flushing, and a second one ran.
        outbox.Attempts.ShouldBe(2);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_orders WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);

        // Written straight through the DbContext in ApplyEvent, so it has no aggregate to reveal it
        // going missing and is lost the same way.
        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_summaries WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);
    }

    [Fact]
    public async Task the_retried_batch_commits_an_update_to_an_existing_row()
    {
        var (store, outbox) = await CreateStoreAsync();
        using var _ = store;

        var orderId = Guid.NewGuid();

        await EfCoreTestHelper.QueryScalarAsync<int>(
            """
            INSERT INTO ef_orders (id, customer_name, total_amount, item_count, is_shipped, is_cancelled)
            VALUES (@id, 'existing', 1, 1, 0, 0)
            """, ("@id", orderId));

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        outbox.Attempts.ShouldBe(2);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_orders WHERE id = @id AND is_shipped = 1",
            ("@id", orderId))).ShouldBe(1);
    }
}
