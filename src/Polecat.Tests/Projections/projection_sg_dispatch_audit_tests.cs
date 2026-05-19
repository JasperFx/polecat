using JasperFx;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

/// <summary>
///     Pre-4.0 regression guard for <c>Polecat#108</c>. Registers every
///     emit-expected test-library projection through a fresh
///     <see cref="DocumentStore"/> and asserts construction completes without
///     <see cref="JasperFx.Events.Projections.InvalidProjectionException"/>.
///     Future <c>JasperFx.Events.SourceGenerator</c> regressions (a shape the
///     SG silently fails to emit a <c>[GeneratedEvolver]</c> for) trip a row
///     in this single theory immediately, before the broader test suite's
///     slower SQL-backed fixtures get a chance to fail with the same symptom
///     across many files.
/// </summary>
/// <remarks>
///     <para>
///         Uses a fake connection string. The <see cref="DocumentStore"/>
///         constructor calls
///         <c>options.Projections.DiscoverGeneratedEvolvers(...)</c> +
///         <c>options.Projections.AssertValidity(...)</c> — the
///         <c>InvalidProjectionException</c> fires there, before any SQL is
///         touched. No <see cref="IntegrationContext"/> inheritance, no
///         database setup.
///     </para>
///     <para>
///         The full inventory + status table lives at
///         <c>docs/projection-sg-audit-108.md</c>; this file is the
///         executable companion to that audit.
///     </para>
///     <para>
///         <b>Deliberately not covered here</b>:
///         <c>CompositeOrderProjection</c> /
///         <c>OrderShippingNotificationProjection</c> (Evolve override),
///         <c>InlineSeProjection</c> (no Apply on the projection itself —
///         delegates to <c>InlineSeAggregate</c>'s self-aggregating
///         evolver), the <c>FlatTableProjection</c> subclasses (own
///         dispatch), and the <c>EfCoreSingleStreamProjection</c>
///         subclasses (different inheritance chain, lives in
///         <c>Polecat.EntityFrameworkCore.Tests</c>).
///     </para>
/// </remarks>
public class projection_sg_dispatch_audit_tests
{
    // Self-aggregating doc types — registered via the bare
    // SingleStreamProjection<TDoc, TId> base class (the same shape
    // Snapshot<T> and AggregateStreamAsync<T> use internally).
    public static IEnumerable<object[]> SelfAggregatingDocs => new[]
    {
        // (label, registration delegate)
        Row("Bug4197Aggregate (Guid, Apply + Create)",
            opts => opts.Projections.Add<SingleStreamProjection<Bug4197Aggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("DeletableAggregate (Guid, Apply + Create + ShouldDelete)",
            opts => opts.Projections.Add<SingleStreamProjection<DeletableAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("StringQuestAggregate (string)",
            opts => opts.Projections.Add<SingleStreamProjection<StringQuestAggregate, string>>(ProjectionLifecycle.Inline)),
        Row("StudentCourseEnrollment (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<StudentCourseEnrollment, Guid>>(ProjectionLifecycle.Inline)),
        Row("QuestAggregate (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<QuestAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("OrderAggregate (Guid, has natural key)",
            opts => opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("InvoiceAggregate (Guid, has natural key)",
            opts => opts.Projections.Add<SingleStreamProjection<InvoiceAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("Report (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<Report, Guid>>(ProjectionLifecycle.Inline)),
        Row("StringReport (string)",
            opts => opts.Projections.Add<SingleStreamProjection<StringReport, string>>(ProjectionLifecycle.Inline)),
        Row("ScenarioQuestParty (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<ScenarioQuestParty, Guid>>(ProjectionLifecycle.Inline)),

        // Evolve-based self-aggregating types — exercise the
        // SelfAggregatingEvolve SG emit mode (IGeneratedAsyncEvolver) and
        // the sync IGeneratedSyncEvolver mode (mutable + immutable
        // record flavors of the same shape).
        Row("MutableIEventEvolveAggregate (Guid, Evolve(snapshot, IEvent))",
            opts => opts.Projections.Add<SingleStreamProjection<MutableIEventEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("MutableObjectEvolveAggregate (Guid, Evolve(snapshot, TEvent))",
            opts => opts.Projections.Add<SingleStreamProjection<MutableObjectEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("ImmutableIEventEvolveAggregate (Guid record, Evolve returning new)",
            opts => opts.Projections.Add<SingleStreamProjection<ImmutableIEventEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("ImmutableObjectEvolveAggregate (Guid record, Evolve returning new)",
            opts => opts.Projections.Add<SingleStreamProjection<ImmutableObjectEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("AsyncEvolveAggregate (Guid, EvolveAsync)",
            opts => opts.Projections.Add<SingleStreamProjection<AsyncEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),
        Row("ImmutableAsyncEvolveAggregate (Guid record, EvolveAsync returning new)",
            opts => opts.Projections.Add<SingleStreamProjection<ImmutableAsyncEvolveAggregate, Guid>>(ProjectionLifecycle.Inline)),

        // Composite projection tests' aggregates
        Row("CompositeQuestParty (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<CompositeQuestParty, Guid>>(ProjectionLifecycle.Inline)),
        Row("QuestStats (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<QuestStats, Guid>>(ProjectionLifecycle.Inline)),
        Row("QuestParty (Guid, has ShouldDelete — the JasperFx#298 repro)",
            opts => opts.Projections.Add<SingleStreamProjection<QuestParty, Guid>>(ProjectionLifecycle.Inline)),

        // String identity
        Row("SelfAggregatingStringQuest (string)",
            opts => opts.Projections.Add<SingleStreamProjection<SelfAggregatingStringQuest, string>>(ProjectionLifecycle.Inline)),
        Row("SnapshotParty (Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<SnapshotParty, Guid>>(ProjectionLifecycle.Inline)),
        Row("SnapshotPartyByString (string)",
            opts => opts.Projections.Add<SingleStreamProjection<SnapshotPartyByString, string>>(ProjectionLifecycle.Inline)),

        // Strong-typed id (wrapper struct over Guid / string) —
        // exercises the DocumentMapping.IdType resolution path
        // (Polecat-side fix for JasperFx#276 Phase 3, commit 2fd263b).
        Row("Payment (PaymentId wraps Guid)",
            opts => opts.Projections.Add<SingleStreamProjection<Payment, PaymentId>>(ProjectionLifecycle.Inline)),
        Row("Payment2 (Payment2Id wraps string)",
            opts => opts.Projections.Add<SingleStreamProjection<Payment2, Payment2Id>>(ProjectionLifecycle.Inline)),
    };

    // Projection subclasses (EventProjection + SingleStreamProjection +
    // MultiStreamProjection subclasses with conventional Apply/Create
    // or Project methods on the projection class itself — PartialProjection
    // SG emit mode).
    public static IEnumerable<object[]> ProjectionSubclasses => new[]
    {
        Row("QuestLogProjection (EventProjection, conventional Project)",
            opts => opts.Projections.Add<QuestLogProjection>(ProjectionLifecycle.Inline)),
        Row("MultiEventQuestLogProjection (EventProjection, Project ×2)",
            opts => opts.Projections.Add<MultiEventQuestLogProjection>(ProjectionLifecycle.Inline)),
        Row("AuditRecordCreatorProjection (EventProjection, Create returning doc)",
            opts => opts.Projections.Add<AuditRecordCreatorProjection>(ProjectionLifecycle.Inline)),
        Row("AuditRecordProjection (EventProjection, ApplyAsync override + TypeRegistration)",
            opts => opts.Projections.Add<AuditRecordProjection>(ProjectionLifecycle.Inline)),
        Row("ImportSqlProjection (EventProjection, Project ×2)",
            opts => opts.Projections.Add<ImportSqlProjection>(ProjectionLifecycle.Inline)),
        Row("StringQuestPartyProjection (SingleStreamProjection<,> subclass, Apply/Create + ShouldDelete)",
            opts => opts.Projections.Add<StringQuestPartyProjection>(ProjectionLifecycle.Inline)),
        Row("CustomerSummaryProjection (MultiStreamProjection<,> subclass)",
            opts => opts.Projections.Add<CustomerSummaryProjection>(ProjectionLifecycle.Inline)),
        Row("MonthlyAccountActivityProjection (MultiStreamProjection<,> subclass, string id)",
            opts => opts.Projections.Add<MonthlyAccountActivityProjection>(ProjectionLifecycle.Inline)),
    };

    /// <summary>
    ///     Registers each self-aggregating document type and confirms the
    ///     <see cref="DocumentStore"/> constructor's
    ///     <c>Projections.AssertValidity(...)</c> sweep doesn't throw
    ///     <see cref="JasperFx.Events.Projections.InvalidProjectionException"/>.
    ///     A miss here means the SG silently failed to emit a
    ///     <c>[GeneratedEvolver]</c> for that shape — file back against
    ///     <c>JasperFx/jasperfx#276</c>.
    /// </summary>
    [Theory]
    [MemberData(nameof(SelfAggregatingDocs))]
    public void self_aggregating_doc_dispatcher_resolves(string label, Action<StoreOptions> register)
        => AssertDispatcherResolves(label, register);

    /// <summary>
    ///     Same gate as above but for projection subclasses (PartialProjection /
    ///     EventProjection SG emit modes).
    /// </summary>
    [Theory]
    [MemberData(nameof(ProjectionSubclasses))]
    public void projection_subclass_dispatcher_resolves(string label, Action<StoreOptions> register)
        => AssertDispatcherResolves(label, register);

    private static object[] Row(string label, Action<StoreOptions> register)
        => new object[] { label, register };

    private static void AssertDispatcherResolves(string label, Action<StoreOptions> register)
    {
        var options = new StoreOptions
        {
            // Connection string never used — DocumentStore's ctor runs
            // AssertValidity before any SQL connection is opened.
            ConnectionString = "Server=projection-sg-audit;Database=audit;Integrated Security=False;User Id=sa;Password=unused;TrustServerCertificate=True",
            AutoCreateSchemaObjects = AutoCreate.None,
            DatabaseSchemaName = "projection_sg_audit",
        };

        register(options);

        // DocumentStore ctor calls DiscoverGeneratedEvolvers + AssertValidity.
        // If the SG didn't emit a [GeneratedEvolver] for the registered shape
        // (and the projection doesn't override Evolve/EvolveAsync/DetermineAction*
        // directly), this throws InvalidProjectionException with the diagnostic
        // "No source-generated dispatcher found for ...".
        var store = new DocumentStore(options);
        store.Dispose();

        _ = label; // surfaced in xUnit failure message via the theory display name
    }
}
