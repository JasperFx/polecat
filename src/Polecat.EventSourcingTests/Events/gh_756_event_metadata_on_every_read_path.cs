using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #756: the opt-in event metadata — <c>CorrelationId</c>, <c>CausationId</c>, <c>Headers</c>,
///     <c>UserName</c> — arrives on EVERY path that turns a stored row into an <see cref="IEvent" />.
/// </summary>
/// <remarks>
///     <para>
///         The reported bug was the async daemon's loader, which selected none of these columns, so a
///         subscription or async projection saw them all as null and nothing downstream of the daemon could
///         be traced to the request that caused it. Fixing that one path would have repeated the mistake that
///         produced it: Polecat has five hydration paths (CLAUDE.md), each carried its own hand-written copy
///         of the metadata mapping, and no two agreed — the event LINQ path never read <c>headers</c> and
///         the DCB tag paths never read <c>user_name</c>. All of them now read through
///         <c>PcEventsRowReader.ReadMetadata</c>.
///     </para>
///     <para>
///         So this asserts all four values on every path, against one event appended under known values.
///         A path that drops any one of them fails by name.
///     </para>
/// </remarks>
public partial class gh_756_event_metadata_on_every_read_path: OneOffConfigurationsContext
{
    private const string Correlation = "corr-756";
    private const string Causation = "cause-756";
    private const string HeaderKey = "origin";
    private const string HeaderValue = "checkout";
    private const string User = "alice-756";

    private readonly StudentId _student = new(Guid.NewGuid());
    private readonly Guid _streamId = Guid.NewGuid();

    private void ConfigureEverythingEnabled()
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
            opts.Events.EnableHeaders = true;
            opts.Events.EnableUserName = true;

            opts.Events.RegisterTagType<StudentId>("student").ForAggregate<StudentCourseEnrollment>();

            opts.Projections.Subscribe<MetadataRecordingSubscription>();
            opts.Projections.Add(new MetadataRecordingProjection(), ProjectionLifecycle.Async);
        });
    }

    private async Task AppendUnderKnownMetadataAsync()
    {
        await using var session = theStore.LightweightSession();
        session.CorrelationId = Correlation;
        session.CausationId = Causation;
        session.SetHeader(HeaderKey, HeaderValue);
        session.LastModifiedBy = User;

        var enrolled = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(_student);
        session.Events.StartStream(_streamId, enrolled);

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static void ShouldCarryTheMetadata(IEvent e, string path)
    {
        e.CorrelationId.ShouldBe(Correlation, $"{path}: CorrelationId");
        e.CausationId.ShouldBe(Causation, $"{path}: CausationId");
        e.UserName.ShouldBe(User, $"{path}: UserName");
        e.Headers.ShouldNotBeNull($"{path}: Headers");
        e.Headers![HeaderKey].ToString().ShouldBe(HeaderValue, $"{path}: Headers[{HeaderKey}]");
    }

    // The issue's own test: the async daemon's loader, seen through a subscription.
    [Fact]
    public async Task a_subscription_sees_the_metadata()
    {
        MetadataRecordingSubscription.Reset();
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await theStore.WaitForProjectionAsync();

        ShouldCarryTheMetadata(MetadataRecordingSubscription.Events.Single(), "subscription");
    }

    // The same loader feeds every async projection.
    [Fact]
    public async Task an_async_projection_sees_the_metadata()
    {
        MetadataRecordingProjection.Reset();
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await theStore.WaitForProjectionAsync();

        ShouldCarryTheMetadata(MetadataRecordingProjection.Events.Single(), "async projection");
    }

    // The canonical reader, which always had it -- the control.
    [Fact]
    public async Task fetch_stream_sees_the_metadata()
    {
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(_streamId, token: TestContext.Current.CancellationToken);

        ShouldCarryTheMetadata(events.Single(), "FetchStreamAsync");
    }

    // EventListHandler -- never selected or read headers.
    [Fact]
    public async Task query_all_raw_events_sees_the_metadata()
    {
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents().ToListAsync(TestContext.Current.CancellationToken);

        ShouldCarryTheMetadata(events.Single(), "QueryAllRawEvents");
    }

    // The two DCB copies -- neither read user_name.
    [Fact]
    public async Task a_tag_query_sees_the_metadata()
    {
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(new EventTagQuery().Or<StudentId>(_student),
            TestContext.Current.CancellationToken);

        ShouldCarryTheMetadata(events.Single(), "QueryByTagsAsync");
    }

    [Fact]
    public async Task fetch_for_writing_by_tags_sees_the_metadata()
    {
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<StudentId>(_student), TestContext.Current.CancellationToken);

        ShouldCarryTheMetadata(boundary.Events.Single(), "FetchForWritingByTags");
    }

    [Fact]
    public async Task a_batched_fetch_for_writing_by_tags_sees_the_metadata()
    {
        ConfigureEverythingEnabled();
        await AppendUnderKnownMetadataAsync();

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var pending = batch.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<StudentId>(_student));
        await batch.Execute(TestContext.Current.CancellationToken);

        ShouldCarryTheMetadata((await pending).Events.Single(), "batched FetchForWritingByTags");
    }

    public class MetadataRecordingSubscription: SubscriptionBase
    {
        private static readonly List<IEvent> _events = [];
        private static readonly object _lock = new();

        public static IReadOnlyList<IEvent> Events
        {
            get { lock (_lock) return _events.ToList(); }
        }

        public static void Reset()
        {
            lock (_lock) _events.Clear();
        }

        public override Task<IChangeListener> ProcessEventsAsync(EventRange page,
            ISubscriptionController controller, IDocumentOperations operations,
            CancellationToken cancellationToken)
        {
            lock (_lock) _events.AddRange(page.Events.Where(x => x.Data is StudentEnrolled));
            return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
        }
    }

    public partial class MetadataRecordingProjection: EventProjection
    {
        private static readonly List<IEvent> _events = [];
        private static readonly object _lock = new();

        public static IReadOnlyList<IEvent> Events
        {
            get { lock (_lock) return _events.ToList(); }
        }

        public static void Reset()
        {
            lock (_lock) _events.Clear();
        }

        public void Project(IEvent<StudentEnrolled> e, IDocumentSession session)
        {
            lock (_lock) _events.Add(e);
        }
    }
}
