using JasperFx.Events.Daemon;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Services;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #916. A session's flush lets the operation behind a SQL error name it, and then the application's
///     transforms. The daemon's flush did neither, so a raised event's lost race came out of a shard as
///     Msg 52905 rather than the concurrency failure it is. A batch reads the stream's version when it is
///     built, ahead of its transaction, which leaves room for that race.
/// </summary>
public partial class projection_batch_exception_transform_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public record BatchRaised(int Number);

    public class BatchClaim
    {
        public Guid Id { get; set; }
        public string Code { get; set; } = "";
    }

    public class ClaimRefused(SqlException inner) : Exception("Refused", inner);

    /// <summary>
    ///     Runs in the only gap there is, after the batch has read the version and before it flushes.
    /// </summary>
    private class AppendAheadOfTheFlush(Func<Task> append) : IChangeListener
    {
        public Task BeforeCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => append();

        public Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
            => Task.CompletedTask;
    }

    private async Task<PolecatProjectionBatch> StartWithRaceAsync(Guid stream, Action<IDocumentSession> racer,
        TenancyStyle tenancy = TenancyStyle.Single)
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = tenancy;
            opts.Events.AddEventType<BatchRaised>();
            opts.Projections.AsyncListeners.Add(new AppendAheadOfTheFlush(async () =>
            {
                await using var racing = theStore.LightweightSession();
                racer(racing);
                await racing.SaveChangesAsync(Ct);
            }));
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(stream, new BatchRaised(1));
            await session.SaveChangesAsync(Ct);
        }

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database, ShardExecutionMode.Continuous);
        AsyncDisposables.Add(batch);
        return batch;
    }

    private async Task<StreamAction> RaiseOnAsync(Guid stream)
    {
        await using var builder = theStore.LightweightSession();
        return builder.Events.Append(stream, new BatchRaised(3));
    }

    /// <summary>
    ///     The version check comes after the inserts, so an append that got in first is met at the event's
    ///     unique index, not at the check.
    /// </summary>
    [Fact]
    public async Task a_raised_event_that_loses_the_race_to_an_append_is_a_concurrency_failure()
    {
        var stream = Guid.NewGuid();
        var batch = await StartWithRaceAsync(stream, x => x.Events.Append(stream, new BatchRaised(2)));

        batch.QuickAppendEvents(await RaiseOnAsync(stream));

        var failure = await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            () => batch.ExecuteAsync(Ct));
        failure.Message.ShouldContain(stream.ToString());

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).Select(x => x.Version).ShouldBe([1, 2]);
    }

    /// <summary>
    ///     Nothing in the way of the inserts here, so this is the one that gets as far as Msg 52905. Conjoined
    ///     because <c>pc_events</c> has a foreign key to <c>pc_streams</c> otherwise, which refuses them first.
    /// </summary>
    [Fact]
    public async Task a_raised_event_on_a_stream_deleted_in_the_gap_is_a_concurrency_failure()
    {
        var stream = Guid.NewGuid();
        var batch = await StartWithRaceAsync(stream, x => x.Events.TombstoneStream(stream), TenancyStyle.Conjoined);

        batch.QuickAppendEvents(await RaiseOnAsync(stream));

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(() => batch.ExecuteAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_application_can_claim_what_an_operation_does_not()
    {
        ConfigureStore(opts =>
        {
            opts.Schema.For<BatchClaim>().UniqueIndex(x => x.Code);
            opts.ExceptionTransforms.IfExceptionIs<SqlException>()
                .If(e => e.Number is 2601 or 2627)
                .ThenTransformTo(e => new ClaimRefused(e));
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new BatchClaim { Id = Guid.NewGuid(), Code = "taken" });
            await session.SaveChangesAsync(Ct);
        }

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database, ShardExecutionMode.Continuous);
        AsyncDisposables.Add(batch);

        batch.SessionForTenant(JasperFx.StorageConstants.DefaultTenantId)
            .Store(new BatchClaim { Id = Guid.NewGuid(), Code = "taken" });

        await Should.ThrowAsync<ClaimRefused>(() => batch.ExecuteAsync(Ct));
    }

    [Fact]
    public async Task an_error_nothing_claims_is_reported_as_it_was()
    {
        ConfigureStore(opts => opts.Schema.For<BatchClaim>().UniqueIndex(x => x.Code));

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new BatchClaim { Id = Guid.NewGuid(), Code = "taken" });
            await session.SaveChangesAsync(Ct);
        }

        var batch = new PolecatProjectionBatch(theStore, theStore.Options.EventGraph, theStore.Database, ShardExecutionMode.Continuous);
        AsyncDisposables.Add(batch);

        batch.SessionForTenant(JasperFx.StorageConstants.DefaultTenantId)
            .Store(new BatchClaim { Id = Guid.NewGuid(), Code = "taken" });

        await Should.ThrowAsync<SqlException>(() => batch.ExecuteAsync(Ct));
    }
}
