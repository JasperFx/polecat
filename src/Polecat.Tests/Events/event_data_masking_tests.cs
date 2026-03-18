using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// Events for masking tests
public record PersonCreated(string Name, string Email, string SocialSecurityNumber);
public record PersonUpdated(string Email);

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
        await theSession.SaveChangesAsync();

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId);
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
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
        await theSession.SaveChangesAsync();

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId, e => e.Version == 1);
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

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
        await theSession.SaveChangesAsync();

        await theStore.Advanced.ApplyEventDataMasking(masking =>
        {
            masking.IncludeStream(streamId);
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

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
}
