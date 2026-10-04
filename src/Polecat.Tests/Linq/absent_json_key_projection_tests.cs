using System.Text.Json;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Linq;

/// <summary>
///     #722: whether a SQL NULL in a streamed projection means "no key" or <c>"key": null</c>, which
///     has to be decided per member because neither answer is right for both.
/// </summary>
/// <remarks>
///     <para>
///         <b>Both directions, and the issue is explicit about why.</b> The absent-key direction is
///         marten#5461 — a document type that gained a property after rows were stored projects
///         <c>"key": null</c>, and a consumer deserializing into a non-nullable value type throws.
///         The present-and-null direction is <b>marten#5499, the regression its first fix caused</b>:
///         stripping every null turned a legitimately-null <c>DateOnly?</c> into an absent key,
///         trading a throw for a silently different value.
///     </para>
///     <para>
///         ⚠️ <b>The absent-key direction cannot be faked by setting the property to null.</b> That
///         only ever exercises present-and-null, and a test written that way looks like it covers
///         both. The key has to be removed from the stored JSON — here with a lax
///         <c>JSON_MODIFY(..., NULL)</c>, which deletes it — so the row genuinely has no such
///         property, exactly as a row written before the member existed does.
///     </para>
/// </remarks>
public class absent_json_key_projection_tests: OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;

        /// <summary>Non-nullable value type: null is not a value it can hold.</summary>
        public bool Flag { get; set; }

        /// <summary>Non-nullable value type, second shape.</summary>
        public int Count { get; set; }

        /// <summary>Nullable value type — the marten#5499 member.</summary>
        public DateOnly? When { get; set; }

        /// <summary>Nullable reference.</summary>
        public string? Note { get; set; }
    }

    /// <summary>A DTO whose members are non-nullable, so an explicit null throws on the way in.</summary>
    private sealed record FlagDto(string Name, bool Flag, int Count);

    private sealed record WhenDto(string Name, DateOnly? When);

    private static void requiresNativeJson()
        => Assert.SkipUnless(ConnectionSource.SupportsNativeJson,
            "JSON_OBJECT needs SQL Server 2025 native JSON; the nvarchar(max) path does not use it.");

    private static readonly Guid TheId = Guid.NewGuid();

    private async Task<IDocumentStore> aStoreWithOneDocumentAsync()
    {
        requiresNativeJson();

        ConfigureStore(opts => opts.Schema.For<Doc>());

        await using var session = theStore.LightweightSession();
        session.Store(new Doc
        {
            Id = TheId, Name = "alpha", Flag = true, Count = 7,
            When = new DateOnly(2026, 1, 2), Note = "note"
        });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return theStore;
    }

    /// <summary>
    ///     Remove a key from the stored document, the way a row written before the member existed
    ///     has never had it. A lax <c>JSON_MODIFY</c> with a NULL value DELETES the key rather than
    ///     setting it to null, which is the same mechanism the fix itself leans on.
    /// </summary>
    private async Task RemoveKeyAsync(string jsonKey)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"UPDATE {theStore.Options.DatabaseSchemaName}.pc_doc_doc "
            + $"SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.{jsonKey}', NULL) WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", TheId);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // The precondition the whole class rests on. If JSON_MODIFY did not actually delete the key,
        // every fact below would be testing present-and-null while claiming to test absence.
        await using var check = conn.CreateCommand();
        check.CommandText =
            $"SELECT JSON_PATH_EXISTS(CAST(data AS nvarchar(max)), '$.{jsonKey}') "
            + $"FROM {theStore.Options.DatabaseSchemaName}.pc_doc_doc WHERE id = @id";
        check.Parameters.AddWithValue("@id", TheId);
        (await check.ExecuteScalarAsync(TestContext.Current.CancellationToken))
            .ShouldBe(0, $"'{jsonKey}' is still present, so this test proves nothing about absence.");
    }

    private Task<string> ProjectFlagAsync(IDocumentStore store)
    {
        var query = store.QuerySession();
        AsyncDisposables.Add(query);
        return query.Query<Doc>().Where(x => x.Id == TheId)
            .Select(x => new { x.Name, x.Flag, x.Count })
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);
    }

    // ---- the absent-key direction (marten#5461) --------------------------------------------------

    [Fact]
    public async Task an_absent_key_is_omitted_for_a_non_nullable_member()
    {
        var store = await aStoreWithOneDocumentAsync();
        await RemoveKeyAsync("flag");

        var json = await ProjectFlagAsync(store);

        // Omitted, NOT "flag": null -- null is not a value a bool can hold, so absence is the only
        // representable answer and it is the one the whole-document read already gives.
        json.ShouldNotContain("\"flag\"");
        json.ShouldContain("\"name\"");
    }

    [Fact]
    public async Task an_absent_key_deserializes_into_a_non_nullable_member_without_throwing()
    {
        // ⚠️ THE acceptance criterion. Before #722 this threw: JSON_OBJECT's default NULL ON NULL
        // emitted "flag": null and System.Text.Json cannot convert null into bool.
        var store = await aStoreWithOneDocumentAsync();
        await RemoveKeyAsync("flag");

        var json = await ProjectFlagAsync(store);

        var dto = JsonSerializer.Deserialize<FlagDto[]>(json,
            new JsonSerializerOptions(JsonSerializerDefaults.Web))!.ShouldHaveSingleItem();

        dto.Name.ShouldBe("alpha");
        dto.Flag.ShouldBeFalse();  // the CLR default, which is what absence means
        dto.Count.ShouldBe(7);     // the member that was never removed is untouched
    }

    // ---- the present-and-null direction (marten#5499) --------------------------------------------

    [Fact]
    public async Task a_genuine_null_on_a_nullable_value_type_still_projects_as_null()
    {
        // ⚠️ THE marten#5499 half, and the one a fix is most likely to regress. ABSENT ON NULL would
        // drop this key, and the consumer would see a DateOnly? that "was never set" rather than one
        // deliberately stored as null.
        var store = await aStoreWithOneDocumentAsync();

        await using (var session = store.LightweightSession())
        {
            session.Store(new Doc { Id = TheId, Name = "alpha", Flag = true, Count = 7, When = null });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession();
        var json = await query.Query<Doc>().Where(x => x.Id == TheId)
            .Select(x => new { x.Name, x.When })
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);

        json.ShouldContain("\"when\":null");

        JsonSerializer.Deserialize<WhenDto[]>(json,
                new JsonSerializerOptions(JsonSerializerDefaults.Web))!
            .ShouldHaveSingleItem().When.ShouldBeNull();
    }

    [Fact]
    public async Task a_null_reference_member_still_projects_as_null()
    {
        var store = await aStoreWithOneDocumentAsync();

        await using (var session = store.LightweightSession())
        {
            session.Store(new Doc { Id = TheId, Name = "alpha", Note = null });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession();
        var json = await query.Query<Doc>().Where(x => x.Id == TheId)
            .Select(x => new { x.Name, x.Note })
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);

        json.ShouldContain("\"note\":null");
    }

    [Fact]
    public async Task an_absent_key_on_a_nullable_member_is_null_rather_than_omitted()
    {
        // The two shapes a nullable member cannot tell apart get the SAME answer, deliberately:
        // JSON_VALUE returns SQL NULL for both, and null is a value the member can hold. Pinned so
        // the per-member split is not quietly widened to "omit whenever the key is absent", which
        // would need JSON_PATH_EXISTS per member and would reintroduce marten#5499's asymmetry.
        var store = await aStoreWithOneDocumentAsync();
        await RemoveKeyAsync("when");

        await using var query = store.QuerySession();
        var json = await query.Query<Doc>().Where(x => x.Id == TheId)
            .Select(x => new { x.Name, x.When })
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);

        json.ShouldContain("\"when\":null");
    }

    // ---- the paths that must not change ----------------------------------------------------------

    [Fact]
    public async Task a_whole_document_read_is_unaffected()
    {
        var store = await aStoreWithOneDocumentAsync();
        await RemoveKeyAsync("flag");

        // No projection, so the raw `data` column streams untouched -- it simply has no such key,
        // which is the behaviour the projection now matches rather than diverges from.
        await using var query = store.QuerySession();
        var json = await query.Query<Doc>().Where(x => x.Id == TheId)
            .ToJsonArrayAsync(TestContext.Current.CancellationToken);

        json.ShouldNotContain("\"flag\"");
        json.ShouldContain("\"count\":7");
    }

    [Fact]
    public async Task the_non_native_json_store_still_refuses_a_streamed_projection()
    {
        // The nvarchar(max) path never builds a JSON_OBJECT at all, so #722 cannot reach it -- and a
        // fix that accidentally made it start streaming projections would be a silent behaviour
        // change on exactly the stores that cannot support it. Asserted rather than assumed.
        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = false;
            opts.Schema.For<Doc>();
        });

        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<Doc>().Select(x => new { x.Name, x.Flag })
                .ToJsonArrayAsync(TestContext.Current.CancellationToken));
    }
}
