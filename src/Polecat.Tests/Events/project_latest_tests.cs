using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record ReportCreated(string Title);
public record SectionAdded(string SectionName);
public record ReportPublished;

public class Report
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public int SectionCount { get; set; }
    public bool IsPublished { get; set; }

    public static Report Create(ReportCreated e) => new Report { Title = e.Title };
    public void Apply(SectionAdded e) => SectionCount++;
    public void Apply(ReportPublished e) => IsPublished = true;
}

[Collection("integration")]
public class project_latest_tests : IntegrationContext
{
    public project_latest_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    #region sample_polecat_project_latest_example

    [Fact]
    public async Task includes_pending_events_from_start_stream()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();

        // Append events without committing
        session.Events.StartStream(streamId,
            new ReportCreated("Q1 Report"),
            new SectionAdded("Revenue"),
            new SectionAdded("Costs")
        );

        // ProjectLatest includes the pending events above
        var report = await session.Events.ProjectLatest<Report>(streamId);

        report.ShouldNotBeNull();
        report.Title.ShouldBe("Q1 Report");
        report.SectionCount.ShouldBe(2);

        // SaveChangesAsync can happen later
        await session.SaveChangesAsync();
    }

    #endregion

    #region sample_polecat_project_latest_merge_example

    [Fact]
    public async Task includes_pending_events_after_committed_events()
    {
        var streamId = Guid.NewGuid();

        // First, commit some events
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new ReportCreated("Q1 Report"),
                new SectionAdded("Revenue")
            );
            await session.SaveChangesAsync();
        }

        // In a new session, append more events without committing
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId,
                new SectionAdded("Costs"),
                new SectionAdded("Outlook"),
                new ReportPublished()
            );

            // ProjectLatest merges the committed state with pending events
            var report = await session.Events.ProjectLatest<Report>(streamId);

            report.ShouldNotBeNull();
            report.Title.ShouldBe("Q1 Report");
            report.SectionCount.ShouldBe(3);       // 1 committed + 2 pending
            report.IsPublished.ShouldBeTrue();      // from pending ReportPublished
        }
    }

    #endregion

    [Fact]
    public async Task no_pending_events_behaves_like_fetch_latest()
    {
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new ReportCreated("Q1 Report"),
                new SectionAdded("Revenue")
            );
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            var fromProjectLatest = await session.Events.ProjectLatest<Report>(streamId);
            var fromFetchLatest = await session.Events.FetchLatest<Report>(streamId);

            fromProjectLatest.ShouldNotBeNull();
            fromFetchLatest.ShouldNotBeNull();
            fromProjectLatest.Title.ShouldBe(fromFetchLatest.Title);
            fromProjectLatest.SectionCount.ShouldBe(fromFetchLatest.SectionCount);
        }
    }

    [Fact]
    public async Task returns_null_for_nonexistent_stream()
    {
        await using var session = theStore.LightweightSession();
        var report = await session.Events.ProjectLatest<Report>(Guid.NewGuid());
        report.ShouldBeNull();
    }

    [Fact]
    public async Task project_latest_with_string_key_includes_pending_events()
    {
        // Reconfigure for string-keyed streams with isolated schema
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "project_latest_str";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var key = "report-" + Guid.NewGuid().ToString("N");

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(key,
            new ReportCreated("Quarterly Report"),
            new SectionAdded("Revenue"),
            new SectionAdded("Costs"),
            new ReportPublished()
        );

        var report = await session.Events.ProjectLatest<Report>(key);

        report.ShouldNotBeNull();
        report.Title.ShouldBe("Quarterly Report");
        report.SectionCount.ShouldBe(2);
        report.IsPublished.ShouldBeTrue();
    }

    [Fact]
    public async Task project_latest_merges_committed_and_pending_for_string_key()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "project_latest_str2";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var key = "report-" + Guid.NewGuid().ToString("N");

        // Commit initial events
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(key,
                new ReportCreated("Annual Report"),
                new SectionAdded("Overview")
            );
            await session.SaveChangesAsync();
        }

        // Append more events without committing, then project
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(key,
                new SectionAdded("Financials"),
                new ReportPublished()
            );

            var report = await session.Events.ProjectLatest<Report>(key);

            report.ShouldNotBeNull();
            report.Title.ShouldBe("Annual Report");
            report.SectionCount.ShouldBe(2);
            report.IsPublished.ShouldBeTrue();
        }
    }
}
