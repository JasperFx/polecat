using JasperFx.Events;
using Polecat.Events.Schema;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     jasperfx#1215: the <see cref="IEventStore.RewriteStoredEventsAsync" /> seam a Wolverine job drives, beyond what
///     <c>EventRewriteCompliance</c> covers: how a definition's stored type names and transforms select rows, the
///     <see cref="StoredEventRewrite.StopAfter" /> it rests on, and the event-range marks being a Polecat 6 default.
/// </summary>
public class rewriting_stored_events_as_a_job : OneOffConfigurationsContext
{
    public record PersonRegisteredV1(string Name);

    public record PersonRegistered(string FullName);

    public interface IRelocation
    {
        string City { get; }
    }

    public record PersonMoved(string City) : IRelocation;

    public record CompanyMoved(string City) : IRelocation;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(Action<StoreOptions>? more = null) => ConfigureStore(opts =>
    {
        opts.Events.Upcast<PersonRegisteredV1, PersonRegistered>(x => new PersonRegistered(x.Name));
        opts.Events.AddEventType<PersonMoved>();
        opts.Events.AddEventType<CompanyMoved>();
        more?.Invoke(opts);
    });

    private string aliasOf<T>() => theStore.Options.EventGraph.EventMappingFor(typeof(T)).EventTypeName;

    private async Task<Guid> appendAsync(params object[] events)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, events);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task<IReadOnlyList<IEvent>> streamAsync(Guid streamId)
    {
        await using var query = theStore.QuerySession();
        return await query.Events.FetchStreamAsync(streamId, token: Ct);
    }

    private async Task<EventRewriteProgress> rewriteAsync(EventRewriteDefinition definition)
    {
        definition.Name = "job" + Guid.NewGuid().ToString("N")[..12];
        var run = await EventRewriteRun.FromMarksAsync(theDatabase, definition, Ct);
        return (await ((IEventStore)theStore).RewriteStoredEventsAsync(run, Ct)).Single();
    }

    [Fact]
    public async Task a_listed_type_name_takes_in_the_names_an_upcaster_reads_into_it()
    {
        configure();
        await appendAsync(new PersonRegisteredV1("Alice"), new PersonMoved("Utrecht"));

        var total = await rewriteAsync(new EventRewriteDefinition { EventTypes = [aliasOf<PersonRegistered>()] });

        // The V1 row is read through the upcaster and written back under today's name; the move is not selected
        total.Visited.ShouldBe(1);
        total.Rewritten.ShouldBe(1);
    }

    [Fact]
    public async Task a_listed_name_no_type_is_stored_under_is_still_read()
    {
        configure();
        await appendAsync(new PersonRegisteredV1("Alice"));

        // The stored name of the upcast source, which is not an event type of this application
        var total = await rewriteAsync(new EventRewriteDefinition
        {
            EventTypes = [theStore.Options.EventGraph.EventMappingFor(typeof(PersonRegisteredV1)).EventTypeName]
        });

        total.Visited.ShouldBe(1);
        total.Rewritten.ShouldBe(1);
    }

    [Fact]
    public async Task a_transform_on_an_interface_selects_every_event_type_that_implements_it()
    {
        configure(opts => opts.EventGraph.AddRewriteTransform<IRelocation>("upper-city", x => x switch
        {
            PersonMoved p => p with { City = p.City.ToUpperInvariant() },
            CompanyMoved c => c with { City = c.City.ToUpperInvariant() },
            _ => x
        }));

        var stream = await appendAsync(new PersonMoved("Utrecht"), new CompanyMoved("Delft"),
            new PersonRegistered("Bob"));

        var total = await rewriteAsync(new EventRewriteDefinition { Transforms = ["upper-city"] });

        total.Visited.ShouldBe(2);
        total.Rewritten.ShouldBe(2);
        (await streamAsync(stream)).Select(x => x.Data)
            .ShouldBe([new PersonMoved("UTRECHT"), new CompanyMoved("DELFT"), new PersonRegistered("Bob")]);
    }

    [Fact]
    public async Task stop_after_leaves_later_events_alone()
    {
        configure();
        var stream = await appendAsync(new PersonMoved("a"), new PersonMoved("b"), new PersonMoved("c"));
        var events = await streamAsync(stream);

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.Transform<PersonMoved>(e => e with { City = e.City + "!" });
            x.StopAfter = events[1].Sequence;
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Rewritten.ShouldBe(2);
        result.Databases.Single().Completed.ShouldBeTrue();
        (await streamAsync(stream)).Select(x => x.Data)
            .ShouldBe([new PersonMoved("a!"), new PersonMoved("b!"), new PersonMoved("c")]);
    }

    [Fact]
    public async Task a_mark_key_longer_than_the_column_is_refused_before_it_is_truncated()
    {
        configure();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var key = new string('k', EventRangeMarksTable.KeyLength + 1);

        await Should.ThrowAsync<ArgumentOutOfRangeException>(() => theDatabase.SetEventRangeMarkAsync(key, null, 1, Ct));
        await Should.ThrowAsync<ArgumentOutOfRangeException>(() => theDatabase.FetchEventRangeMarkAsync(key, null, Ct));
    }

    [Fact]
    public void event_range_marks_are_on_by_default_and_restore_v5_defaults_leaves_the_table_out()
    {
        var options = new StoreOptions();
        options.Events.EnableEventRangeMarks.ShouldBeTrue();
        tableNames(options).ShouldContain(EventRangeMarksTable.TableName);

        options.RestoreV5Defaults();
        options.Events.EnableEventRangeMarks.ShouldBeFalse();
        tableNames(options).ShouldNotContain(EventRangeMarksTable.TableName);

        static string[] tableNames(StoreOptions options) =>
            ((Weasel.Core.Migrations.IFeatureSchema)new EventStoreFeatureSchema(options)).Objects
            .Select(x => x.Identifier.Name).ToArray();
    }

    [Fact]
    public async Task without_the_marks_the_store_refuses_rewrite_jobs()
    {
        configure(opts => opts.Events.EnableEventRangeMarks = false);

        ((IEventStore)theStore).SupportsEventRewrites.ShouldBeFalse();

        var refused = await Should.ThrowAsync<NotSupportedException>(() =>
            theDatabase.FetchEventRangeMarkAsync(EventRangeMarks.RewriteKey("anything"), null, Ct));
        refused.Message.ShouldContain(nameof(EventStoreOptions.EnableEventRangeMarks));
    }
}
