using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Batching;

/// <summary>
///     #849. An item's completion source was only ever given a result, so after a failed
///     <c>Execute</c> an <c>await batch.Load(...)</c> waited forever. Every await here carries a bound,
///     or the test for a hang would be a hang.
/// </summary>
[Collection("integration")]
public class batch_failure_tests : IntegrationContext
{
    private const int GuardError = 51849;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public batch_failure_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static string Refuse(string message) => $"THROW {GuardError}, '{message}', 1;";

    private async Task<Guid> SeedUserAsync()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "A" };
        theSession.Store(user);
        await theSession.SaveChangesAsync(Ct);
        return user.Id;
    }

    [Fact]
    public async Task every_item_faults_when_the_batch_cannot_be_sent()
    {
        var id = await SeedUserAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // First, so the failure arrives before any item has a result
        var raw = batch.Query<int>("SELECT no_such_column FROM no_such_table");

        var load = batch.Load<User>(id);
        var exists = batch.CheckExists<User>(id);
        var many = batch.LoadMany<User>(id);
        var linq = batch.Query<User>().Where(x => x.FirstName == "Alice").ToList();
        var count = batch.Query<User>().Count();
        var any = batch.Query<User>().Any();
        var first = batch.Query<User>().FirstOrDefault();

        var failure = await Should.ThrowAsync<SqlException>(() => batch.Execute(Ct));

        foreach (var item in new Task[] { raw, load, exists, many, linq, count, any, first })
        {
            var thrown = await Should.ThrowAsync<SqlException>(() => item.WaitAsync(Bound, Ct));
            thrown.Number.ShouldBe(failure.Number);
        }
    }

    [Fact]
    public async Task an_item_read_before_the_failure_keeps_its_result_and_the_rest_fault()
    {
        var id = await SeedUserAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var before = batch.Load<User>(id);
        var refused = batch.Query<int>(Refuse("refused by the guard"));
        var after = batch.CheckExists<User>(id);

        var failure = await Should.ThrowAsync<SqlException>(() => batch.Execute(Ct));
        failure.Number.ShouldBe(GuardError);

        (await before.WaitAsync(Bound, Ct)).ShouldNotBeNull().Id.ShouldBe(id);

        (await Should.ThrowAsync<SqlException>(() => refused.WaitAsync(Bound, Ct))).Number.ShouldBe(GuardError);
        (await Should.ThrowAsync<SqlException>(() => after.WaitAsync(Bound, Ct))).Number.ShouldBe(GuardError);
    }

    [Fact]
    public async Task an_item_that_fails_to_read_its_own_result_set_faults_with_that_failure()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Not a document, so reading it back fails on the client after the server has answered
        var unreadable = batch.Query<User>("SELECT 'this is not json'");

        var failure = await Should.ThrowAsync<Exception>(() => batch.Execute(Ct));

        var thrown = await Should.ThrowAsync<Exception>(() => unreadable.WaitAsync(Bound, Ct));
        thrown.GetType().ShouldBe(failure.GetType());
    }

    [Fact]
    public async Task a_cancelled_execute_cancels_every_item()
    {
        var id = await SeedUserAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var load = batch.Load<User>(id);
        var exists = batch.CheckExists<User>(id);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(() => batch.Execute(cts.Token));

        await Should.ThrowAsync<OperationCanceledException>(() => load.WaitAsync(Bound, Ct));
        await Should.ThrowAsync<OperationCanceledException>(() => exists.WaitAsync(Bound, Ct));
    }

    [Fact]
    public async Task an_event_item_faults_too()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var refused = batch.Query<int>(Refuse("refused by the guard"));
        var state = batch.Events.FetchStreamState(Guid.NewGuid());
        var events = batch.Events.FetchStream(Guid.NewGuid());

        await Should.ThrowAsync<SqlException>(() => batch.Execute(Ct));

        await Should.ThrowAsync<SqlException>(() => refused.WaitAsync(Bound, Ct));
        await Should.ThrowAsync<SqlException>(() => state.WaitAsync(Bound, Ct));
        await Should.ThrowAsync<SqlException>(() => events.WaitAsync(Bound, Ct));
    }

    /// <summary>
    ///     #916: this used to run the batch again and throw "An attempt was made to transition a task to
    ///     a final state when it had already completed" from the first item's <c>SetResult</c>.
    /// </summary>
    [Fact]
    public async Task a_second_execute_after_a_failure_says_what_happened()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var refused = batch.Query<int>(Refuse("refused by the guard"));

        await Should.ThrowAsync<SqlException>(() => batch.Execute(Ct));

        var second = await Should.ThrowAsync<InvalidOperationException>(() => batch.Execute(Ct));
        second.Message.ShouldContain("already been executed");

        // The first failure is still what the item reports
        (await Should.ThrowAsync<SqlException>(() => refused.WaitAsync(Bound, Ct))).Number.ShouldBe(GuardError);
    }

    [Fact]
    public async Task a_second_execute_after_a_success_says_what_happened_and_keeps_the_results()
    {
        var id = await SeedUserAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        var load = batch.Load<User>(id);

        await batch.Execute(Ct);

        var second = await Should.ThrowAsync<InvalidOperationException>(() => batch.Execute(Ct));
        second.Message.ShouldContain("already been executed");

        (await load.WaitAsync(Bound, Ct)).ShouldNotBeNull().Id.ShouldBe(id);
    }

    /// <summary>
    ///     The second <c>Execute</c> is the only thing waiting on an item added after the first, so
    ///     refusing has to finish it or it hangs, as #849 was about.
    /// </summary>
    [Fact]
    public async Task an_item_added_after_execute_faults_with_the_refusal()
    {
        var id = await SeedUserAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        _ = batch.Load<User>(id);
        await batch.Execute(Ct);

        var late = batch.CheckExists<User>(id);

        await Should.ThrowAsync<InvalidOperationException>(() => batch.Execute(Ct));

        var thrown = await Should.ThrowAsync<InvalidOperationException>(() => late.WaitAsync(Bound, Ct));
        thrown.Message.ShouldContain("already been executed");
    }

    /// <summary>
    ///     The caller who catches <c>Execute</c> and never touches the items, which is everyone written
    ///     before this fix. A faulted task nobody awaited surfaces through
    ///     <see cref="TaskScheduler.UnobservedTaskException" />, and fatally in a host that sets
    ///     <c>ThrowUnobservedTaskExceptions</c>.
    /// </summary>
    [Fact]
    public async Task an_item_nobody_awaits_does_not_become_an_unobserved_task_exception()
    {
        const string marker = "the unobserved batch item";

        var unobserved = 0;

        void Observe(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            if (args.Exception.Flatten().InnerExceptions.Any(x => x.Message.Contains(marker)))
            {
                Interlocked.Increment(ref unobserved);
            }
        }

        TaskScheduler.UnobservedTaskException += Observe;
        try
        {
            await FailABatchAndDropItsItemsAsync(marker);

            for (var i = 0; i < 5; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Yield();
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= Observe;
        }

        unobserved.ShouldBe(0);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async Task FailABatchAndDropItsItemsAsync(string marker)
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        _ = batch.Query<int>(Refuse(marker));
        _ = batch.Query<int>("SELECT 1");

        await Should.ThrowAsync<SqlException>(() => batch.Execute(Ct));
    }
}
