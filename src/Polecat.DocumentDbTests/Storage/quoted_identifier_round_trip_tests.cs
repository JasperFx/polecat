using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Storage;

public class QuotedTenantDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Rank { get; set; }
}

public class QuotedNameDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record QuotedThingHappened(string What);

/// <summary>
///     End-to-end coverage for the polecat#390 audit: values that reach a SQL identifier or
///     string-literal position by way of runtime data or a public-API argument survive a round trip
///     when they contain the characters that terminate those positions — <c>'</c> for a literal and
///     <c>]</c> for a bracketed identifier.
/// </summary>
/// <remarks>
///     These are the cases the audit classified as (c) — supplied at runtime or through a public API
///     rather than fixed at compile time. Categories (a) and (b) are covered by the unit tests in
///     <see cref="sql_escaping_tests" />.
/// </remarks>
[Collection("integration")]
public class quoted_identifier_round_trip_tests : IntegrationContext
{
    // Both terminators, in one value, plus a payload that would be visible if escaping failed open.
    private const string QuoteTenant = "o'brien'; DROP TABLE pc_events--";
    private const string BracketTenant = "acme]corp";

    public quoted_identifier_round_trip_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task documents_round_trip_for_a_tenant_id_containing_a_quote_or_a_bracket()
    {
        await DropTableAsync("quoted_tenant_docs", "pc_doc_quotedtenantdoc");
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "quoted_tenant_docs";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined; // document tenancy is store-wide
        });

        var quoteDoc = new QuotedTenantDoc { Id = Guid.NewGuid(), Name = "Quoted", Rank = 1 };
        var bracketDoc = new QuotedTenantDoc { Id = Guid.NewGuid(), Name = "Bracketed", Rank = 2 };

        theSession.ForTenant(QuoteTenant).Store(quoteDoc);
        theSession.ForTenant(BracketTenant).Store(bracketDoc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Load: the tenant id reaches the read filter, which composes it into a string literal.
        await using var quoteSession = theStore.QuerySession(new SessionOptions { TenantId = QuoteTenant });
        (await quoteSession.LoadAsync<QuotedTenantDoc>(quoteDoc.Id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull()
            .Name.ShouldBe("Quoted");

        await using var bracketSession = theStore.QuerySession(new SessionOptions { TenantId = BracketTenant });
        (await bracketSession.LoadAsync<QuotedTenantDoc>(bracketDoc.Id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull()
            .Name.ShouldBe("Bracketed");

        // And the tenants stay isolated — a broken escape would either error or leak across.
        (await quoteSession.Query<QuotedTenantDoc>().ToListAsync(TestContext.Current.CancellationToken))
            .Select(x => x.Name).ShouldBe(["Quoted"]);
        (await bracketSession.LoadAsync<QuotedTenantDoc>(quoteDoc.Id, TestContext.Current.CancellationToken))
            .ShouldBeNull();
    }

    [Fact]
    public async Task events_round_trip_for_a_tenant_id_containing_a_quote_or_a_bracket()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "quoted_tenant_events";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        var quoteStream = Guid.NewGuid();
        var bracketStream = Guid.NewGuid();

        theSession.ForTenant(QuoteTenant).Events.StartStream(quoteStream, new QuotedThingHappened("quoted"));
        theSession.ForTenant(BracketTenant).Events.StartStream(bracketStream, new QuotedThingHappened("bracketed"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var quoteSession = theStore.QuerySession(new SessionOptions { TenantId = QuoteTenant });
        var quoteEvents = await quoteSession.Events.FetchStreamAsync(
            quoteStream, token: TestContext.Current.CancellationToken);
        quoteEvents.Count.ShouldBe(1);
        quoteEvents[0].Data.ShouldBeOfType<QuotedThingHappened>().What.ShouldBe("quoted");

        await using var bracketSession = theStore.QuerySession(new SessionOptions { TenantId = BracketTenant });
        var bracketEvents = await bracketSession.Events.FetchStreamAsync(
            bracketStream, token: TestContext.Current.CancellationToken);
        bracketEvents.Count.ShouldBe(1);
        bracketEvents[0].Data.ShouldBeOfType<QuotedThingHappened>().What.ShouldBe("bracketed");

        // The pc_events table is still there — proof the `'; DROP TABLE` payload stayed inert data.
        (await ScalarAsync("SELECT OBJECT_ID('[quoted_tenant_events].[pc_events]')")).ShouldNotBeNull();
    }

    /// <summary>
    ///     ⚠️ <b>#684 changed this from "escaped and round-trips" to "refused up front", and that is a
    ///     breaking change worth reading rather than skimming.</b>
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This used to assert that an index name containing <c>]</c> and <c>'</c> was escaped
    ///         correctly in both positions it lands in — the <c>sys.indexes</c> existence probe (a string
    ///         literal) and <c>CREATE INDEX</c> (a bracketed identifier) — by applying the schema twice
    ///         and proving the probe matched the index the first pass created. It did, and #665 is why.
    ///     </para>
    ///     <para>
    ///         Index names are Weasel schema objects now, so Weasel's identifier policy governs them, and
    ///         that policy is to <em>refuse</em> rather than escape: <c>IdentifierValidation</c>
    ///         (weasel#416) rejects <c>;</c>, <c>'</c>, <c>"</c>, <c>[</c> and <c>]</c> outright. The
    ///         refusal happens in <c>ApplyAllConfiguredChangesToDatabaseAsync</c>, so a store declaring
    ///         such a name now fails at startup with a message naming the character.
    ///     </para>
    ///     <para>
    ///         That is the better contract and not merely the one that fell out. Every other identifier
    ///         in Polecat — table names, column names, constraint names — has been subject to this rule
    ///         all along; index names were the exception only because they were raw DDL, which is
    ///         precisely the inconsistency #684 set out to remove. Refusing a hostile name at startup
    ///         also beats escaping it correctly forever, because the escaping only had to be got wrong
    ///         once.
    ///     </para>
    ///     <para>
    ///         Kept as a test rather than deleted: the capability changed, so the assertion changed with
    ///         it, and both halves still matter. A name that is refused must be refused <em>clearly</em>,
    ///         and a name that is merely unusual must still work.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task an_index_name_containing_a_bracket_or_a_quote_is_refused()
    {
        const string schema = "quoted_index_names";

        await DropTableAsync(schema, "pc_doc_quotednamedoc");

        foreach (var hostile in new[] { "idx_qu]oted", "idx_o'brien", "idx_semi;colon" })
        {
            var ex = await Should.ThrowAsync<InvalidOperationException>(() => StoreOptions(opts =>
            {
                opts.DatabaseSchemaName = schema;
                opts.Schema.For<QuotedNameDoc>().Index(x => x.Name, idx => idx.IndexName = hostile);
            }));

            // The message has to name the offending name, or the operator cannot find the declaration.
            ex.Message.ShouldContain(hostile);
        }
    }

    /// <summary>
    ///     The other half: an unusual but safe index name is still created, and applying twice is still
    ///     idempotent.
    /// </summary>
    /// <remarks>
    ///     Without this, "index names are validated now" would be satisfied by a store that refused
    ///     every index name. Interior spaces are the interesting case — <c>IdentifierValidation</c>
    ///     allows them deliberately (a legacy <c>unit price</c> column is somebody's real schema) while
    ///     rejecting a tab or a line break, which could introduce a comment.
    /// </remarks>
    [Fact]
    public async Task an_unusual_but_safe_index_name_round_trips()
    {
        const string schema = "quoted_index_names";
        const string indexName = "idx unusual name";

        await DropTableAsync(schema, "pc_doc_quotednamedoc");

        // Two passes: the second proves the existence check matches the index the first one created,
        // rather than trying to create it again.
        for (var pass = 0; pass < 2; pass++)
        {
            await StoreOptions(opts =>
            {
                opts.DatabaseSchemaName = schema;
                opts.Schema.For<QuotedNameDoc>().Index(x => x.Name, idx => idx.IndexName = indexName);
            });

            var doc = new QuotedNameDoc { Id = Guid.NewGuid(), Name = $"pass-{pass}" };
            theSession.Store(doc);
            await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

            (await theSession.LoadAsync<QuotedNameDoc>(doc.Id, TestContext.Current.CancellationToken))
                .ShouldNotBeNull().Name.ShouldBe($"pass-{pass}");
        }

        (await ScalarAsync(
            $"""
             SELECT COUNT(*) FROM sys.indexes
             WHERE name = '{indexName}'
               AND object_id = OBJECT_ID('[{schema}].[pc_doc_quotednamedoc]')
             """)).ShouldBe(1);
    }

    private async Task<object?> ScalarAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return result == DBNull.Value ? null : result;
    }

    private static async Task DropTableAsync(string schema, string table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS [{schema}].[{table}];";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
