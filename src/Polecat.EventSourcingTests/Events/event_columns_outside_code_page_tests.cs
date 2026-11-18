using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

public record OutsideCodePageCity(string Value);

public record OutsideCodePageHappened(string Name);

public record 東京Happened(string Name);

public class OutsideCodePageShop
{
    public string Id { get; set; } = string.Empty;
}

public class OutsideCodePageKiosk: OutsideCodePageShop;

/// <summary>
///     #922: the varchar columns that are neither an id nor a tenant. Measured under the default collation
///     (code page 1252) before any of this was refused: a tag query for <c>東京</c> returned the events tagged
///     <c>大阪</c> as well, <c>correlation_id</c> lost its text, and an event whose CLR type name had a
///     character outside the code page was saved without complaint and then missing from every read.
/// </summary>
public class event_columns_outside_code_page_tests: OneOffConfigurationsContext
{
    private const string Tokyo = "東京";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.AddEventType<OutsideCodePageHappened>();
            opts.Events.AddEventType<東京Happened>();
            opts.Events.RegisterTagType<OutsideCodePageCity>("city");
            opts.Schema.For<OutsideCodePageShop>().AddSubClass<OutsideCodePageKiosk>(Tokyo);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    [Fact]
    public async Task a_correlation_id_outside_the_code_page_is_refused()
    {
        await ConfigureAsync();
        await using var session = theStore.LightweightSession();
        session.CorrelationId = $"{Tokyo}-1";
        session.Events.StartStream(Guid.NewGuid(), new OutsideCodePageHappened("a"));

        var refused = await Should.ThrowAsync<ColumnValueOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.Column.ShouldBe("correlation_id");
        refused.Value.ShouldBe($"{Tokyo}-1");
        refused.CodePage.ShouldBe(1252);
    }

    [Fact]
    public async Task a_tag_value_outside_the_code_page_is_refused_so_it_cannot_become_another_tags_value()
    {
        await ConfigureAsync();
        await using var session = theStore.LightweightSession();
        var tagged = session.Events.BuildEvent(new OutsideCodePageHappened("a"));
        tagged.WithTag(new OutsideCodePageCity(Tokyo));
        session.Events.Append(Guid.NewGuid(), tagged);

        var refused = await Should.ThrowAsync<ColumnValueOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.Column.ShouldBe("tag value (city)");
        refused.Value.ShouldBe(Tokyo);

        await using var query = theStore.LightweightSession();
        (await query.Events.QueryByTagsAsync(new EventTagQuery().Or<OutsideCodePageCity>(new OutsideCodePageCity("大阪")), Ct))
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task an_event_type_whose_name_is_outside_the_code_page_is_refused()
    {
        await ConfigureAsync();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new 東京Happened("a"));

        var refused = await Should.ThrowAsync<ColumnValueOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.Column.ShouldBe("dotnet_type");
    }

    [Fact]
    public async Task a_subclass_alias_outside_the_code_page_is_refused()
    {
        await ConfigureAsync();
        await using var session = theStore.LightweightSession();
        session.Store(new OutsideCodePageKiosk { Id = "a" });

        var refused = await Should.ThrowAsync<ColumnValueOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.Column.ShouldBe("doc_type");
        refused.Value.ShouldBe(Tokyo);
    }

    [Fact]
    public async Task values_within_the_code_page_are_unaffected()
    {
        await ConfigureAsync();
        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "café-1";
            var tagged = session.Events.BuildEvent(new OutsideCodePageHappened("a"));
            tagged.WithTag(new OutsideCodePageCity("Málaga"));
            session.Events.Append(id, tagged);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchStreamAsync(id, token: Ct)).Single().CorrelationId.ShouldBe("café-1");
        (await query.Events.QueryByTagsAsync(new EventTagQuery().Or<OutsideCodePageCity>(new OutsideCodePageCity("Málaga")), Ct))
            .Count.ShouldBe(1);
    }
}
