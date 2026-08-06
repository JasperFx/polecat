using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// Events for masking tests
public record PersonCreated(string Name, string Email, string SocialSecurityNumber);
public record PersonUpdated(string Email);

// A mutable event with two protected members, so an interface rule and a concrete-type rule can
// each mask a different one and prove both ran. See #422.
public interface IHasSubject
{
    string Subject { get; set; }
}

public class PersonRecorded: IHasSubject
{
    public string Subject { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

[Collection("integration")]
public class event_data_masking_tests : IntegrationContext
{
    public event_data_masking_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_mask_event_data_with_func_rule()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "mask_func");

        theStore.Events.AddMaskingRuleForProtectedInformation<PersonCreated>(e =>
            e with { Email = "***masked***", SocialSecurityNumber = "***masked***" });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new PersonCreated("Alice", "alice@example.com", "123-45-6789"),
            new PersonUpdated("alice-new@example.com"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId);
        }, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(2);

        var created = events[0].Data.ShouldBeOfType<PersonCreated>();
        created.Email.ShouldBe("***masked***");
        created.SocialSecurityNumber.ShouldBe("***masked***");
        created.Name.ShouldBe("Alice");

        var updated = events[1].Data.ShouldBeOfType<PersonUpdated>();
        updated.Email.ShouldBe("alice-new@example.com");
    }

    [Fact]
    public async Task masking_with_stream_filter()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "mask_filter");

        theStore.Events.AddMaskingRuleForProtectedInformation<PersonCreated>(e =>
            e with { Email = "***", SocialSecurityNumber = "***" });

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new PersonCreated("Charlie", "charlie@test.com", "111-22-3333"),
            new PersonCreated("Diana", "diana@test.com", "444-55-6666"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId, e => e.Version == 1);
        }, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var first = events[0].Data.ShouldBeOfType<PersonCreated>();
        first.Email.ShouldBe("***");

        var second = events[1].Data.ShouldBeOfType<PersonCreated>();
        second.Email.ShouldBe("diana@test.com");
    }

    [Fact]
    public async Task masking_only_applies_to_matching_event_types()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "mask_types");

        theStore.Events.AddMaskingRuleForProtectedInformation<PersonUpdated>(e =>
            new PersonUpdated("***redacted***"));

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new PersonCreated("Eve", "eve@test.com", "000-00-0000"),
            new PersonUpdated("eve-new@test.com"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId);
        }, TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var created = events[0].Data.ShouldBeOfType<PersonCreated>();
        created.Email.ShouldBe("eve@test.com");

        var updated = events[1].Data.ShouldBeOfType<PersonUpdated>();
        updated.Email.ShouldBe("***redacted***");
    }

    [Fact]
    public async Task masking_throws_without_sources()
    {
        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await theStore.Advanced.ApplyEventDataMasking(_ => { });
        });
    }

    /// <summary>
    ///     Regression for #422. Every other test here registers a single rule, so no event ever
    ///     matched more than one and the short-circuiting `matched || masker.TryMask(e)` in TryMask
    ///     was unobservable: the first rule to match an event stopped every later rule from being
    ///     invoked at all, and the operation still reported success. On a right-to-erasure path
    ///     that leaves protected information in place.
    /// </summary>
    [Fact]
    public async Task every_matching_rule_runs_not_just_the_first()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "mask_compose");

        // Two rules that both match PersonCreated: one contravariantly through the interface, one
        // against the concrete type. Each masks a different member.
        theStore.Events.AddMaskingRuleForProtectedInformation<IHasSubject>(e => e.Subject = "***masked***");
        theStore.Events.AddMaskingRuleForProtectedInformation<PersonRecorded>(e => e.Email = "***masked***");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new PersonRecorded { Subject = "Alice", Email = "alice@example.com" });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.Advanced.ApplyEventDataMasking(masking => masking.IncludeStream(streamId),
            TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var recorded = events[0].Data.ShouldBeOfType<PersonRecorded>();
        recorded.Subject.ShouldBe("***masked***");
        recorded.Email.ShouldBe("***masked***");
    }

    /// <summary>
    ///     ... and in the other registration order, so the test cannot pass by accident of which
    ///     rule happens to be registered first.
    /// </summary>
    [Fact]
    public async Task every_matching_rule_runs_regardless_of_registration_order()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "mask_compose_reversed");

        theStore.Events.AddMaskingRuleForProtectedInformation<PersonRecorded>(e => e.Email = "***masked***");
        theStore.Events.AddMaskingRuleForProtectedInformation<IHasSubject>(e => e.Subject = "***masked***");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new PersonRecorded { Subject = "Alice", Email = "alice@example.com" });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.Advanced.ApplyEventDataMasking(masking => masking.IncludeStream(streamId),
            TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var recorded = events[0].Data.ShouldBeOfType<PersonRecorded>();
        recorded.Subject.ShouldBe("***masked***");
        recorded.Email.ShouldBe("***masked***");
    }
}
