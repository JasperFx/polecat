using JasperFx;
using JasperFx.Documents;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Metadata;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Documents;

public record struct CityId(string Value);

/// <summary>
///     #919: a string id goes into a <c>varchar(250)</c> primary key, which holds what the code page holds.
///     Under the default collation <c>東京</c> and <c>大阪</c> are both <c>??</c>, so the second write overwrote
///     the first and <c>LoadAsync("東京")</c> returned Osaka, and <c>ā</c> became <c>a</c>, which is a real
///     id somebody else can have. These run on the catalog's default, code page 1252.
/// </summary>
public class string_id_outside_code_page_tests: OneOffConfigurationsContext
{
    private const string Tokyo = "東京";
    private const string Osaka = "大阪";

    /// <summary>
    ///     What <see cref="Osaka" /> left behind before the refusal, and is still there after upgrading.
    /// </summary>
    private const string LostInTheColumn = "??";

    public class City
    {
        public CityId Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class Revised: IRevisioned
    {
        public string Id { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public class Place
    {
        public string Id { get; set; } = string.Empty;
    }

    public class Museum: Place;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static StringDoc Doc(string id, string name = "doc") => new() { Id = id, Name = name };

    private async Task seedAsync(params string[] ids)
    {
        await using var session = theStore.LightweightSession();
        session.Store(ids.Select(x => Doc(x, $"stored as {x}")).ToArray());
        await session.SaveChangesAsync(Ct);
    }

    private async Task<string[]> storedIdsAsync()
    {
        await using var query = theStore.QuerySession();
        var docs = await query.Query<StringDoc>().ToListAsync(Ct);
        return docs.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToArray();
    }

    // ---- writes ----

    [Fact]
    public async Task the_second_of_two_ids_no_longer_overwrites_the_first()
    {
        await seedAsync("plain");

        await using var session = theStore.LightweightSession();
        session.Store(Doc(Tokyo, "tokyo"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        await using var other = theStore.LightweightSession();
        other.Store(Doc(Osaka, "osaka"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => other.SaveChangesAsync(Ct));

        (await storedIdsAsync()).ShouldBe(["plain"]);
    }

    [Fact]
    public async Task the_refusal_names_the_id_the_type_the_reason_and_the_way_out()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Doc(Tokyo));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.Id.ShouldBe(Tokyo);
        refused.Type.ShouldBe(typeof(StringDoc));
        refused.CodePage.ShouldBe(1252);
        refused.Message.ShouldContain(Tokyo);
        refused.Message.ShouldContain(nameof(StringDoc));
        refused.Message.ShouldContain("code page 1252");
        refused.Message.ShouldContain("overwrite");
        refused.Message.ShouldContain("Guid");
    }

    [Fact]
    public async Task insert_is_refused()
    {
        await using var session = theStore.LightweightSession();
        session.Insert(Doc(Tokyo));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task update_is_refused_and_leaves_the_row_it_would_have_landed_on()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        session.Update(Doc(Tokyo, "changed"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<StringDoc>(LostInTheColumn, Ct))!.Name.ShouldBe($"stored as {LostInTheColumn}");
    }

    [Fact]
    public async Task store_objects_is_refused()
    {
        await using var session = theStore.LightweightSession();
        session.StoreObjects([Doc(Tokyo)]);

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(BulkInsertMode.InsertsOnly)]
    [InlineData(BulkInsertMode.IgnoreDuplicates)]
    [InlineData(BulkInsertMode.OverwriteExisting)]
    public async Task bulk_insert_is_refused_before_any_of_it_is_written(BulkInsertMode mode)
    {
        var documents = new[] { Doc("first"), Doc(Tokyo), Doc("last") };

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() =>
            theStore.Advanced.BulkInsertAsync(documents, mode, 1, StorageConstants.DefaultTenantId, Ct));

        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_version_checked_bulk_insert_is_refused_before_any_of_it_is_written()
    {
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() =>
            theStore.Advanced.BulkInsertWithVersionAsync(
                new[] { (new Revised { Id = "first" }, 0L), (new Revised { Id = Tokyo }, 0L) }, 1, Ct));

        await using var query = theStore.QuerySession();
        (await query.Query<Revised>().CountAsync(Ct)).ShouldBe(0);
    }

    [Fact]
    public async Task a_bulk_insert_within_the_code_page_is_not_refused()
    {
        await theStore.Advanced.BulkInsertAsync([Doc("café"), Doc("ñ")], Ct);
        (await storedIdsAsync()).ShouldBe(["café", "ñ"]);
    }

    [Fact]
    public async Task a_strong_typed_id_wrapping_a_string_is_refused()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new City { Id = new CityId(Tokyo), Name = "tokyo" });

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.Id.ShouldBe(Tokyo);
        refused.Type.ShouldBe(typeof(City));
    }

    [Fact]
    public async Task a_strong_typed_id_within_the_code_page_is_not_refused()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new City { Id = new CityId("Málaga"), Name = "malaga" });
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<City>("Málaga", Ct))!.Name.ShouldBe("malaga");
    }

    [Fact]
    public async Task a_subclass_in_a_hierarchy_is_refused()
    {
        ConfigureStore(opts => opts.Schema.For<Place>().AddSubClass<Museum>());

        await using var session = theStore.LightweightSession();
        session.Store(new Museum { Id = Tokyo });

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        refused.Id.ShouldBe(Tokyo);
    }

    [Fact]
    public async Task an_identity_map_session_is_refused()
    {
        await using var session = theStore.IdentitySession();
        session.Store(Doc(Tokyo));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_write_through_a_tenant_view_is_refused()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsAreMultiTenanted());

        await using var session = theStore.LightweightSession();
        session.ForTenant("red").Store(Doc(Tokyo));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task nothing_else_in_the_save_is_written()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Doc("plain"));
        session.QueueSqlCommand($"THIS IS NOT SQL, AND A SAVE THAT REACHED IT WOULD FAIL DIFFERENTLY");
        session.Store(Doc(Tokyo));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Theory]
    [InlineData("ā")] // the column would keep "a", which is somebody else's id
    [InlineData("Ω")] // "O"
    [InlineData("ı")] // "i"
    [InlineData("😀")] // two surrogates, "??"
    [InlineData("café")] // the combining accent has no code point in 1252, unlike the composed é
    public async Task a_value_the_column_would_quietly_change_is_refused(string id)
    {
        await using var session = theStore.LightweightSession();
        session.Store(Doc(id));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Theory]
    [InlineData("café")]
    [InlineData("ñ")]
    [InlineData("Zürich")]
    [InlineData("€100")]
    [InlineData("?")] // a question mark is a question mark, not a sign of anything lost
    public async Task an_id_within_the_code_page_is_not_refused(string id)
    {
        await seedAsync(id);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<StringDoc>(id, Ct))!.Id.ShouldBe(id);
        (await query.LoadManyAsync<StringDoc>([id], Ct)).Single().Id.ShouldBe(id);
        (await query.CheckExistsAsync<StringDoc>(id, Ct)).ShouldBeTrue();
        (await query.Query<StringDoc>().Where(x => x.Id == id).ToListAsync(Ct)).Single().Id.ShouldBe(id);

        await using var deleting = theStore.LightweightSession();
        deleting.Delete<StringDoc>(id);
        await deleting.SaveChangesAsync(Ct);

        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task only_the_first_id_outside_ascii_costs_a_round_trip()
    {
        var saves = new List<int>();
        foreach (var id in new[] { "ascii", "café", "ñ", "plain" })
        {
            await using var session = theStore.LightweightSession();
            session.Store(Doc(id));
            await session.SaveChangesAsync(Ct);
            saves.Add(session.RequestCount);
        }

        // ASCII never asks, the first that isn't asks once, and after that the store knows.
        saves.ShouldBe([1, 2, 1, 1]);
    }

    [Fact]
    public async Task ascii_ids_never_ask_the_database_what_it_holds()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Enumerable.Range(0, 50).Select(x => Doc($"id-{x}")).ToArray());
        await session.SaveChangesAsync(Ct);

        session.RequestCount.ShouldBe(1);
    }

    // ---- reads ----

    [Fact]
    public async Task load_finds_nothing_rather_than_the_row_it_collapses_onto()
    {
        await seedAsync(LostInTheColumn);

        await using var query = theStore.QuerySession();

        (await query.LoadAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
        (await query.LoadAsync<StringDoc>((object)Tokyo, Ct)).ShouldBeNull();
        (await query.LoadJsonAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
        (await query.CheckExistsAsync<StringDoc>(Tokyo, Ct)).ShouldBeFalse();
        (await query.MetadataForAsync(Doc(Tokyo), Ct)).ShouldBeNull();

        // The row itself is still there, under what it actually holds
        (await query.LoadAsync<StringDoc>(LostInTheColumn, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task load_finds_nothing_for_a_value_the_column_would_turn_into_another_id()
    {
        await seedAsync("a");

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<StringDoc>("ā", Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task load_many_leaves_out_the_ids_that_cannot_be_there()
    {
        await seedAsync(LostInTheColumn, "plain");

        await using var query = theStore.QuerySession();

        (await query.LoadManyAsync<StringDoc>([Tokyo], Ct)).ShouldBeEmpty();

        var found = await query.LoadManyAsync<StringDoc>([Tokyo, "plain", Osaka], Ct);
        found.Single().Id.ShouldBe("plain");
    }

    [Fact]
    public async Task an_identity_map_session_loads_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        await using var session = theStore.IdentitySession();

        (await session.LoadAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
        (await session.LoadManyAsync<StringDoc>([Tokyo, "plain"], Ct)).Single().Id.ShouldBe("plain");
    }

    [Fact]
    public async Task a_tenant_view_loads_nothing()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsAreMultiTenanted());
        await using (var seeding = theStore.LightweightSession("red"))
        {
            seeding.Store(Doc(LostInTheColumn));
            await seeding.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var red = session.ForTenant("red");

        (await red.LoadAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
        (await red.LoadManyAsync<StringDoc>([Tokyo], Ct)).ShouldBeEmpty();
        (await red.CheckExistsAsync<StringDoc>(Tokyo, Ct)).ShouldBeFalse();
        (await red.LoadJsonAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_batched_query_finds_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var load = batch.Load<StringDoc>(Tokyo);
        var many = batch.LoadMany<StringDoc>(Tokyo, "plain");
        var exists = batch.CheckExists<StringDoc>(Tokyo);
        await batch.Execute(Ct);

        (await load).ShouldBeNull();
        (await many).Single().Id.ShouldBe("plain");
        (await exists).ShouldBeFalse();
    }

    [Fact]
    public async Task a_batched_load_many_bound_as_a_json_array_finds_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        var ids = Enumerable.Range(0, 150).Select(x => $"missing-{x}").Append(Tokyo).Append("plain").ToArray();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var many = batch.LoadMany<StringDoc>(ids);
        await batch.Execute(Ct);

        (await many).Single().Id.ShouldBe("plain");
    }

    [Fact]
    public async Task a_batched_query_still_finds_an_id_within_the_code_page()
    {
        await seedAsync("café");

        var ids = Enumerable.Range(0, 150).Select(x => $"missing-{x}").Append("café").ToArray();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var load = batch.Load<StringDoc>("café");
        var few = batch.LoadMany<StringDoc>("café");
        var many = batch.LoadMany<StringDoc>(ids);
        var exists = batch.CheckExists<StringDoc>("café");
        await batch.Execute(Ct);

        (await load)!.Id.ShouldBe("café");
        (await few).Single().Id.ShouldBe("café");
        (await many).Single().Id.ShouldBe("café");
        (await exists).ShouldBeTrue();
    }

    [Fact]
    public async Task a_linq_comparison_on_the_id_finds_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        await using var query = theStore.QuerySession();

        (await query.Query<StringDoc>().Where(x => x.Id == Tokyo).ToListAsync(Ct)).ShouldBeEmpty();
        (await query.Query<StringDoc>().FirstOrDefaultAsync(x => x.Id == Tokyo, Ct)).ShouldBeNull();
        (await query.Query<StringDoc>().AnyAsync(x => x.Id == Tokyo, Ct)).ShouldBeFalse();
        (await query.Query<StringDoc>().Where(x => x.Id != Tokyo).CountAsync(Ct)).ShouldBe(2);
    }

    [Fact]
    public async Task a_linq_list_of_ids_finds_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        var ids = new[] { Tokyo, "plain" };
        var list = new List<string> { Tokyo, "plain" };

        await using var query = theStore.QuerySession();

        (await query.Query<StringDoc>().Where(x => x.Id.IsOneOf(ids)).ToListAsync(Ct)).Single().Id.ShouldBe("plain");
        (await query.Query<StringDoc>().Where(x => ids.Contains(x.Id)).ToListAsync(Ct)).Single().Id.ShouldBe("plain");
        (await query.Query<StringDoc>().Where(x => list.Contains(x.Id)).ToListAsync(Ct)).Single().Id.ShouldBe("plain");
    }

    [Fact]
    public async Task a_linq_list_long_enough_to_bind_as_json_finds_nothing()
    {
        await seedAsync(LostInTheColumn, "plain");

        var ids = Enumerable.Range(0, 150).Select(x => $"missing-{x}").Append(Tokyo).Append("plain").ToArray();

        await using var query = theStore.QuerySession();

        (await query.Query<StringDoc>().Where(x => x.Id.IsOneOf(ids)).ToListAsync(Ct)).Single().Id.ShouldBe("plain");
    }

    [Fact]
    public async Task a_strong_typed_id_loads_nothing()
    {
        await using (var seeding = theStore.LightweightSession())
        {
            seeding.Store(new City { Id = new CityId(LostInTheColumn), Name = "osaka" });
            await seeding.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.LoadAsync<City>(Tokyo, Ct)).ShouldBeNull();
        (await query.LoadAsync<City>(new CityId(Tokyo), Ct)).ShouldBeNull();
        (await query.LoadManyAsync<City>([Tokyo], Ct)).ShouldBeEmpty();
        var tokyo = new CityId(Tokyo);
        (await query.Query<City>().Where(x => x.Id == tokyo).ToListAsync(Ct)).ShouldBeEmpty();
    }

    // ---- what a row already holds ----

    [Fact]
    public async Task delete_by_an_id_that_cannot_be_there_deletes_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        session.Delete<StringDoc>(Tokyo);
        session.Delete(Doc(Tokyo));
        session.HardDelete<StringDoc>(Tokyo);
        session.HardDelete(Doc(Tokyo));
        await session.SaveChangesAsync(Ct);

        (await storedIdsAsync()).ShouldBe([LostInTheColumn]);
    }

    [Fact]
    public async Task delete_where_on_an_id_that_cannot_be_there_deletes_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        session.DeleteWhere<StringDoc>(x => x.Id == Tokyo);
        await session.SaveChangesAsync(Ct);

        (await storedIdsAsync()).ShouldBe([LostInTheColumn]);
    }

    [Fact]
    public async Task a_delete_does_not_excuse_a_store_of_the_same_id()
    {
        await using var session = theStore.LightweightSession();
        session.Delete<StringDoc>(Tokyo);
        session.Store(Doc(Tokyo));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_patch_of_an_id_that_cannot_be_there_changes_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        session.Patch<StringDoc>(Tokyo).Set(x => x.Name, "changed");
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<StringDoc>(LostInTheColumn, Ct))!.Name.ShouldBe($"stored as {LostInTheColumn}");
    }

    /// <summary>
    ///     What 5.47.0 left: a key of <c>??</c>, and the id it was given still in the body.
    /// </summary>
    private async Task seedRowLeftBehindAsync()
    {
        await seedAsync(LostInTheColumn);

        var table = theStore.GetProvider(typeof(StringDoc)).Mapping.QualifiedTableName;
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"UPDATE {table} SET data = @data;";
        cmd.Parameters.AddWithValue("@data", theStore.Options.Serializer.ToJson(Doc(Osaka, "osaka")));
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    [Fact]
    public async Task a_row_left_behind_stays_as_it_is_and_answers_to_what_its_key_holds()
    {
        await seedRowLeftBehindAsync();

        await using var query = theStore.QuerySession();

        (await query.LoadAsync<StringDoc>(LostInTheColumn, Ct))!.Id.ShouldBe(Osaka);
        (await query.LoadAsync<StringDoc>(Osaka, Ct)).ShouldBeNull();
        (await query.LoadAsync<StringDoc>(Tokyo, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task the_query_in_the_migration_guide_finds_a_row_left_behind()
    {
        await seedRowLeftBehindAsync();
        await seedAsync("plain", "café");

        var table = theStore.GetProvider(typeof(StringDoc)).Mapping.QualifiedTableName;
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT id, JSON_VALUE(data, '$.id') AS id_in_the_document
            FROM {table}
            WHERE CONVERT(varbinary(1000), CONVERT(nvarchar(250), id))
               <> CONVERT(varbinary(1000), JSON_VALUE(data, '$.id'));
            """;

        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        (await reader.ReadAsync(Ct)).ShouldBeTrue();
        reader.GetString(0).ShouldBe(LostInTheColumn);
        reader.GetString(1).ShouldBe(Osaka);
        (await reader.ReadAsync(Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_row_left_behind_can_be_removed_by_what_its_key_holds()
    {
        await seedRowLeftBehindAsync();

        await using var session = theStore.LightweightSession();
        session.Delete<StringDoc>(LostInTheColumn);
        await session.SaveChangesAsync(Ct);

        (await storedIdsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_row_left_behind_cannot_be_stored_back_under_the_id_in_its_body()
    {
        await seedRowLeftBehindAsync();

        await using var session = theStore.LightweightSession();
        var document = await session.LoadAsync<StringDoc>(LostInTheColumn, Ct);
        document!.Name = "changed";
        session.Store(document);

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task the_diagnostic_read_finds_nothing()
    {
        await seedAsync(LostInTheColumn);

        var diagnostics = (IDocumentStoreDiagnostics)theStore;
        var typeName = typeof(StringDoc).FullName!;

        (await diagnostics.LoadDocumentAsync(typeName, LostInTheColumn, null, Ct)).ShouldNotBeNull();
        (await diagnostics.LoadDocumentAsync(typeName, Tokyo, null, Ct)).ShouldBeNull();
    }
}
