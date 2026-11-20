using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record SameSessionCounterStarted;

public record SameSessionCounterBumped;

public record SameSessionCounterReplaced;

public class SameSessionMutableCounter
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public static SameSessionMutableCounter Create(SameSessionCounterStarted _) => new();

    public void Apply(SameSessionCounterBumped _) => Count++;

    public SameSessionMutableCounter Apply(SameSessionCounterReplaced _) => new() { Id = Id, Count = Count };
}

public record SameSessionImmutableCounter(Guid Id, int Count)
{
    public static SameSessionImmutableCounter Create(IEvent<SameSessionCounterStarted> e) => new(e.StreamId, 0);

    public SameSessionImmutableCounter Apply(SameSessionCounterBumped _) => this with { Count = Count + 1 };

    public SameSessionImmutableCounter Apply(SameSessionCounterReplaced _) => this with { };
}

public enum SameSessionKind
{
    Lightweight,
    Identity
}

public static class ReadingAfterFetchForWritingCases
{
    public static TheoryData<ProjectionLifecycle, bool, SameSessionKind> All()
    {
        var data = new TheoryData<ProjectionLifecycle, bool, SameSessionKind>();
        foreach (var lifecycle in new[] { ProjectionLifecycle.Live, ProjectionLifecycle.Inline, ProjectionLifecycle.Async })
        {
            foreach (var useIdentityMap in new[] { true, false })
            {
                foreach (var kind in Enum.GetValues<SameSessionKind>())
                {
                    data.Add(lifecycle, useIdentityMap, kind);
                }
            }
        }

        return data;
    }
}

public class reading_after_fetch_for_writing_a_mutable_aggregate()
    : reading_after_fetch_for_writing_in_the_same_session<SameSessionMutableCounter>(x => x.Count);

public class reading_after_fetch_for_writing_an_immutable_aggregate()
    : reading_after_fetch_for_writing_in_the_same_session<SameSessionImmutableCounter>(x => x.Count);

/// <summary>
///     polecat#1094: <c>FetchLatest</c> in the same session after <c>FetchForWriting</c> and a commit returned the
///     aggregate as the fetch loaded it, without the committed events, when <c>UseIdentityMapForAggregates</c> is on.
///     Also the Polecat side of marten#6104 (a read after <c>FetchForWriting</c> applying the session's appended events
///     twice) and marten#6106 (the same stale read on an <c>IdentitySession</c> with the option off), ported from
///     Marten's <c>reading_after_fetch_for_writing_in_the_same_session</c>.
/// </summary>
public abstract class reading_after_fetch_for_writing_in_the_same_session<T>(Func<T, int> countOf)
    : OneOffConfigurationsContext where T : class
{
    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task project_latest_applies_the_appended_event_once(ProjectionLifecycle lifecycle,
        bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());

        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task project_latest_answers_the_same_when_called_twice(ProjectionLifecycle lifecycle,
        bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());

        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(2);
        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task project_latest_after_fetch_latest_applies_the_appended_event_once(
        ProjectionLifecycle lifecycle, bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());

        await session.Events.FetchLatest<T>(streamId);

        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task committing_after_project_latest_leaves_the_aggregate_counted_once(
        ProjectionLifecycle lifecycle, bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using (var session = openSession(kind))
        {
            var stream = await session.Events.FetchForWriting<T>(streamId);
            stream.AppendOne(new SameSessionCounterBumped());

            await session.Events.ProjectLatest<T>(streamId);
            await session.SaveChangesAsync();

            countOf((await session.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
        }

        await using var query = theStore.LightweightSession();
        countOf((await query.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
    }

    /// <summary>
    ///     polecat#1094 itself, and marten#6106 on the <c>Identity</c> rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task fetch_latest_after_the_commit_includes_the_committed_events(
        ProjectionLifecycle lifecycle, bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());
        await session.SaveChangesAsync();

        countOf((await session.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
        countOf((await session.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task project_latest_after_the_commit_includes_the_committed_events(
        ProjectionLifecycle lifecycle, bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());
        await session.SaveChangesAsync();

        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task fetch_latest_answers_the_same_twice_when_an_event_replaces_the_aggregate(
        ProjectionLifecycle lifecycle, bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendMany(new SameSessionCounterBumped(), new SameSessionCounterReplaced());
        await session.SaveChangesAsync();

        countOf((await session.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
        countOf((await session.Events.FetchLatest<T>(streamId))!).ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(ReadingAfterFetchForWritingCases.All), MemberType = typeof(ReadingAfterFetchForWritingCases))]
    public async Task project_latest_applies_an_event_appended_after_the_commit(ProjectionLifecycle lifecycle,
        bool useIdentityMap, SameSessionKind kind)
    {
        var streamId = await startCountedOnce(lifecycle, useIdentityMap);

        await using var session = openSession(kind);
        var stream = await session.Events.FetchForWriting<T>(streamId);
        stream.AppendOne(new SameSessionCounterBumped());
        await session.SaveChangesAsync();

        session.Events.Append(streamId, new SameSessionCounterBumped());

        countOf((await session.Events.ProjectLatest<T>(streamId))!).ShouldBe(3);
    }

    private IDocumentSession openSession(SameSessionKind kind) => kind == SameSessionKind.Identity
        ? theStore.IdentitySession()
        : theStore.LightweightSession();

    private async Task<Guid> startCountedOnce(ProjectionLifecycle lifecycle, bool useIdentityMap)
    {
        ConfigureStore(opts =>
        {
            opts.Projections.UseIdentityMapForAggregates = useIdentityMap;
            opts.Projections.Add(new SingleStreamProjection<T, Guid>(), lifecycle);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<T>(streamId, new SameSessionCounterStarted(), new SameSessionCounterBumped());
        await session.SaveChangesAsync();

        return streamId;
    }
}
