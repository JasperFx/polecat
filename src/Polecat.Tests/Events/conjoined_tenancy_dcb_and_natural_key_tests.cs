using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Tests for conjoined tenancy with DCB tags and natural keys.
///     Verifies tenant isolation for tag queries, EventsExist, AggregateByTags,
///     FetchForWritingByTags, DCB concurrency checks, and natural key operations.
/// </summary>
public class conjoined_tenancy_dcb_and_natural_key_tests : OneOffConfigurationsContext
{
    // Reuse the tag types and domain events from dcb_tag_query_and_consistency_tests
    private const string RedTenant = "Red";
    private const string BlueTenant = "Blue";

    private void ConfigureConjoinedStoreWithTags()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });
    }

    private void ConfigureConjoinedStoreWithNaturalKeys()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
    }

    #region DCB Tag Tests - Tenant Isolation

    [Fact]
    public async Task dcb_tag_query_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        // Red tenant appends an event with tags
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var enrolled = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        redSession.Events.Append(Guid.NewGuid(), enrolled);
        await redSession.SaveChangesAsync();

        // Blue tenant queries by the same tag - should find nothing
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var blueEvents = await blueSession.Events.QueryByTagsAsync(query);
        blueEvents.Count.ShouldBe(0);

        // Red tenant queries by the same tag - should find the event
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redEvents = await redQuery.Events.QueryByTagsAsync(query);
        redEvents.Count.ShouldBe(1);
        redEvents[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");
    }

    [Fact]
    public async Task events_exist_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        // Red tenant appends an event with tags
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var enrolled = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        redSession.Events.Append(Guid.NewGuid(), enrolled);
        await redSession.SaveChangesAsync();

        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Blue tenant checks existence - should be false
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueExists = await blueSession.Events.EventsExistAsync(query);
        blueExists.ShouldBeFalse();

        // Red tenant checks existence - should be true
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redExists = await redQuery.Events.EventsExistAsync(query);
        redExists.ShouldBeTrue();
    }

    [Fact]
    public async Task aggregate_by_tags_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        // Red tenant creates enrollment and adds assignment
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var e1 = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        e1.WithTag(studentId, courseId);
        var e2 = redSession.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        e2.WithTag(studentId, courseId);
        redSession.Events.Append(Guid.NewGuid(), e1, e2);
        await redSession.SaveChangesAsync();

        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Blue tenant aggregates - should get null
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueAggregate = await blueSession.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query);
        blueAggregate.ShouldBeNull();

        // Red tenant aggregates - should get the enrollment
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redAggregate = await redQuery.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query);
        redAggregate.ShouldNotBeNull();
        redAggregate!.StudentName.ShouldBe("Alice");
        redAggregate.Assignments.Count.ShouldBe(1);
    }

    [Fact]
    public async Task fetch_for_writing_by_tags_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        // Red tenant creates enrollment
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var enrolled = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        redSession.Events.Append(Guid.NewGuid(), enrolled);
        await redSession.SaveChangesAsync();

        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Blue tenant fetches for writing - should get empty boundary
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueBoundary = await blueSession.Events.FetchForWritingByTags<StudentCourseEnrollment>(query);
        blueBoundary.Aggregate.ShouldBeNull();

        // Red tenant fetches for writing - should get the aggregate
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redBoundary = await redQuery.Events.FetchForWritingByTags<StudentCourseEnrollment>(query);
        redBoundary.Aggregate.ShouldNotBeNull();
        redBoundary.Aggregate!.StudentName.ShouldBe("Alice");
    }

    [Fact]
    public async Task dcb_concurrency_check_cross_tenant_should_not_conflict()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Red tenant fetches for writing (empty)
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redBoundary = await redSession.Events.FetchForWritingByTags<StudentCourseEnrollment>(query);

        // Blue tenant appends events with the SAME tags
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var enrolled = blueSession.Events.BuildEvent(new StudentEnrolled("Bob", "Science"));
        enrolled.WithTag(studentId, courseId);
        blueSession.Events.Append(Guid.NewGuid(), enrolled);
        await blueSession.SaveChangesAsync();

        // Red tenant appends with its own DCB boundary - should NOT conflict
        // because the blue tenant's events are in a different tenant
        var redEnrolled = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        redEnrolled.WithTag(studentId, courseId);
        redBoundary.AppendOne(redEnrolled);
        await Should.NotThrowAsync(() => redSession.SaveChangesAsync());
    }

    [Fact]
    public async Task dcb_concurrency_check_same_tenant_should_conflict()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Red tenant session 1 fetches for writing (empty)
        await using var session1 = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var boundary = await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(query);

        // Red tenant session 2 appends events with the same tags
        await using var session2 = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var enrolled = session2.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        enrolled.WithTag(studentId, courseId);
        session2.Events.Append(Guid.NewGuid(), enrolled);
        await session2.SaveChangesAsync();

        // Session 1 tries to save - should conflict because same tenant has new events
        var conflictEvent = session1.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        conflictEvent.WithTag(studentId, courseId);
        boundary.AppendOne(conflictEvent);

        var ex = await Should.ThrowAsync<Exception>(() => session1.SaveChangesAsync());
        // The DcbConcurrencyException may be wrapped in an AggregateException
        if (ex is AggregateException agg)
        {
            agg.InnerExceptions.ShouldContain(e => e is DcbConcurrencyException);
        }
        else
        {
            ex.ShouldBeOfType<DcbConcurrencyException>();
        }
    }

    [Fact]
    public async Task same_tag_values_in_different_tenants()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        // Red and Blue tenants both use the same tag values
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redEvent = redSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        redEvent.WithTag(studentId, courseId);
        redSession.Events.Append(Guid.NewGuid(), redEvent);
        await redSession.SaveChangesAsync();

        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueEvent = blueSession.Events.BuildEvent(new StudentEnrolled("Bob", "Science"));
        blueEvent.WithTag(studentId, courseId);
        blueSession.Events.Append(Guid.NewGuid(), blueEvent);
        await blueSession.SaveChangesAsync();

        var query = new EventTagQuery().Or<StudentId>(studentId);

        // Each tenant should see only their own event
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redEvents = await redQuery.Events.QueryByTagsAsync(query);
        redEvents.Count.ShouldBe(1);
        redEvents[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");

        await using var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueEvents = await blueQuery.Events.QueryByTagsAsync(query);
        blueEvents.Count.ShouldBe(1);
        blueEvents[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Bob");
    }

    [Fact]
    public async Task schema_creation_with_conjoined_and_tags()
    {
        ConfigureConjoinedStoreWithTags();

        // This should not throw - schema with conjoined tenancy + tags should be valid
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        // Verify we can insert and query without errors
        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var enrolled = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        session.Events.Append(Guid.NewGuid(), enrolled);
        await session.SaveChangesAsync();
    }

    #endregion

    #region Natural Key Tests - Tenant Isolation

    [Fact]
    public async Task natural_key_same_key_different_tenants()
    {
        ConfigureConjoinedStoreWithNaturalKeys();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var orderNumber = new OrderNumber("ORD-001");

        // Red tenant creates an order with the natural key
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        redSession.Events.StartStream(Guid.NewGuid(),
            new NkOrderCreated(orderNumber, "Red Alice"));
        await redSession.SaveChangesAsync();

        // Blue tenant creates an order with the SAME natural key - should NOT conflict
        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        blueSession.Events.StartStream(Guid.NewGuid(),
            new NkOrderCreated(orderNumber, "Blue Bob"));
        await Should.NotThrowAsync(() => blueSession.SaveChangesAsync());
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithNaturalKeys();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var orderNumber = new OrderNumber("ORD-002");

        // Red tenant creates an order
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        redSession.Events.StartStream(Guid.NewGuid(),
            new NkOrderCreated(orderNumber, "Red Alice"),
            new NkOrderItemAdded("Widget", 9.99m));
        await redSession.SaveChangesAsync();

        // Red tenant can fetch by natural key
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redStream = await redQuery.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
        redStream.Aggregate.ShouldNotBeNull();
        redStream.Aggregate!.CustomerName.ShouldBe("Red Alice");

        // Blue tenant cannot fetch by the same natural key - should throw because stream doesn't exist
        await using var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        await Should.ThrowAsync<InvalidOperationException>(
            blueQuery.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber));
    }

    [Fact]
    public async Task fetch_latest_by_natural_key_isolated_by_tenant()
    {
        ConfigureConjoinedStoreWithNaturalKeys();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var orderNumber = new OrderNumber("ORD-003");

        // Red tenant creates an order
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        redSession.Events.StartStream(Guid.NewGuid(),
            new NkOrderCreated(orderNumber, "Red Alice"),
            new NkOrderItemAdded("Widget", 9.99m));
        await redSession.SaveChangesAsync();

        // Red tenant can fetch latest
        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        var redAggregate = await redQuery.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);
        redAggregate.ShouldNotBeNull();
        redAggregate!.CustomerName.ShouldBe("Red Alice");

        // Blue tenant gets null for the same natural key
        await using var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant });
        var blueAggregate = await blueQuery.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);
        blueAggregate.ShouldBeNull();
    }

    [Fact]
    public async Task schema_creation_with_conjoined_and_natural_keys()
    {
        ConfigureConjoinedStoreWithNaturalKeys();

        // This should not throw - schema with conjoined tenancy + natural keys should be valid
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        // Verify we can insert and query without errors
        await using var session = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant });
        session.Events.StartStream(Guid.NewGuid(),
            new NkOrderCreated(new OrderNumber("ORD-SCHEMA"), "Alice"));
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task natural_key_same_key_different_tenants_both_fetch_for_writing()
    {
        ConfigureConjoinedStoreWithNaturalKeys();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var orderNumber = new OrderNumber("ORD-004");

        // Both tenants create orders with the same natural key
        await using (var redSession = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant }))
        {
            redSession.Events.StartStream(Guid.NewGuid(),
                new NkOrderCreated(orderNumber, "Red Alice"));
            await redSession.SaveChangesAsync();
        }

        await using (var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant }))
        {
            blueSession.Events.StartStream(Guid.NewGuid(),
                new NkOrderCreated(orderNumber, "Blue Bob"));
            await blueSession.SaveChangesAsync();
        }

        // Red tenant fetches and completes before blue starts
        await using (var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant }))
        {
            var redStream = await redQuery.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
            redStream.Aggregate.ShouldNotBeNull();
            redStream.Aggregate!.CustomerName.ShouldBe("Red Alice");
        }

        await using (var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant }))
        {
            var blueStream = await blueQuery.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
            blueStream.Aggregate.ShouldNotBeNull();
            blueStream.Aggregate!.CustomerName.ShouldBe("Blue Bob");
        }
    }

    #endregion
}
