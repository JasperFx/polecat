using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Descriptors;
using JasperFx.Events.Tags;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #575 / jasperfx#801 — how a tag NAME resolves, on the two Polecat paths that take one as a
///     string: the dictionary <c>QueryByTagsAsync</c> overload and <c>EventQuery.TagValues</c>.
/// </summary>
/// <remarks>
///     <para>
///         Both now go through the shared <c>TagTypeRegistrationExtensions.RequireByTagName</c>, and
///         that sharing is the point of these tests. The name matching <em>is</em> the contract: a
///         caller holding a store descriptor and a name/value pair cannot discover which spelling a
///         given engine accepts, so a store matching only the CLR name while its sibling also matched
///         the table suffix makes the same query answer differently per store — as an
///         <see cref="ArgumentException" /> at runtime, per store.
///     </para>
///     <para>
///         That is not hypothetical. Polecat's dictionary overload accepted either spelling and
///         compared values case-insensitively; Marten's copy of the same overload matched the CLR name
///         only and compared case-sensitively. The divergence survived because the dictionary overload
///         has no compliance coverage at all — <c>EventQueryCompliance</c> reaches
///         <c>EventQuery.TagValues</c>, not this. So these are the local pin for the path the shared
///         suite cannot see, and Polecat's behaviour is what the contract was standardized to.
///     </para>
/// </remarks>
public class tag_name_resolution_tests : OneOffConfigurationsContext
{
    private void ConfigureStoreWithTags()
    {
        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });
    }

    private async Task<StudentId> SeedTaggedEventAsync()
    {
        ConfigureStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var studentId = new StudentId(Guid.NewGuid());

        await using var session = theStore.LightweightSession();
        var e = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        e.WithTag(studentId);
        session.Events.Append(Guid.NewGuid(), e);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return studentId;
    }

    /// <summary>
    ///     Both spellings of the same tag type — the CLR simple name and the registered table suffix —
    ///     reach the same rows, on both paths, in any casing.
    /// </summary>
    [Theory]
    [InlineData("StudentId")]
    [InlineData("studentid")]
    [InlineData("STUDENTID")]
    [InlineData("student")]
    [InlineData("Student")]
    public async Task either_spelling_of_a_tag_name_resolves_in_any_casing(string tagName)
    {
        var studentId = await SeedTaggedEventAsync();
        var tags = new Dictionary<string, string> { [tagName] = studentId.Value.ToString() };

        var viaDictionary = new List<EventRecord>();
        await foreach (var e in ((IEventStore)theStore).QueryByTagsAsync(
                           tags, JasperFx.StorageConstants.DefaultTenantId, TestContext.Current.CancellationToken))
        {
            viaDictionary.Add(e);
        }

        var viaQuery = await ((IEventStore)theStore).OpenReadOnlyEventStore().QueryEventsAsync(
            new EventQuery { TagValues = { [tagName] = studentId.Value.ToString() }, PageSize = 100 },
            TestContext.Current.CancellationToken);

        viaDictionary.Count.ShouldBe(1);
        viaQuery.TotalCount.ShouldBe(1);
        viaQuery.Events.Count.ShouldBe(1);
    }

    /// <summary>
    ///     An unregistered name is an error on both paths, not an empty answer. "That tag type does not
    ///     exist here" and "no event carries that tag" must not read alike to a caller filtering events
    ///     — the second is a fact about the data, the first is a bug in the query.
    /// </summary>
    [Fact]
    public async Task an_unregistered_tag_name_is_refused_by_both_paths_and_names_what_is_registered()
    {
        await SeedTaggedEventAsync();
        var tags = new Dictionary<string, string> { ["Nonexistent"] = "whatever" };

        var fromDictionary = await Should.ThrowAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in ((IEventStore)theStore).QueryByTagsAsync(
                               tags, JasperFx.StorageConstants.DefaultTenantId, TestContext.Current.CancellationToken))
            {
            }
        });

        var fromQuery = await Should.ThrowAsync<ArgumentException>(
            () => ((IEventStore)theStore).OpenReadOnlyEventStore().QueryEventsAsync(
                new EventQuery { TagValues = { ["Nonexistent"] = "whatever" }, PageSize = 100 },
                TestContext.Current.CancellationToken));

        // Both list the registered types, in both spellings, so the caller can fix the call from the
        // message alone rather than going to read the store's configuration.
        foreach (var message in new[] { fromDictionary.Message, fromQuery.Message })
        {
            message.ShouldContain("Nonexistent");
            message.ShouldContain(nameof(StudentId));
            message.ShouldContain("student");
        }
    }

    /// <summary>
    ///     The two tag members are alternative spellings of one filter, not a combination, so supplying
    ///     both is refused rather than silently resolved in favour of whichever the store checks first.
    ///     Free from <c>EventQuery.AssertIsWellFormed()</c>, which <c>AssertFiltersAreSupported</c>
    ///     calls first thing — this pins that Polecat's call site actually reaches it.
    /// </summary>
    [Fact]
    public async Task supplying_both_tag_spellings_is_refused()
    {
        var studentId = await SeedTaggedEventAsync();

        var query = new EventQuery
        {
            TagValues = { ["StudentId"] = studentId.Value.ToString() },
            TagConditions = EventTagQuerySpec.From(new EventTagQuery().Or<StudentEnrolled, StudentId>(studentId)),
            PageSize = 100
        };

        var ex = await Should.ThrowAsync<ArgumentException>(
            () => ((IEventStore)theStore).OpenReadOnlyEventStore()
                .QueryEventsAsync(query, TestContext.Current.CancellationToken));

        ex.Message.ShouldContain(nameof(EventQuery.TagConditions));
        ex.Message.ShouldContain(nameof(EventQuery.TagValues));
    }
}
