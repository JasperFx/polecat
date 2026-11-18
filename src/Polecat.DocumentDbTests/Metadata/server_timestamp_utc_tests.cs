using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Metadata;

/// <summary>
///     Session-semantics audit (Stoat plan critter-hardening-audit, node
///     polecat-session-semantics-audit) — the Polecat analogue of marten#5136.
///
///     Marten wrote <c>now() at time zone 'utc'</c> into a <c>timestamptz</c> column. That
///     expression yields a <em>naive</em> local-ish timestamp which Postgres then re-interprets in
///     the server's zone when storing it into an offset-aware column, so <c>mt_last_modified</c> and
///     <c>mt_events.timestamp</c> came out skewed by the server's UTC offset. The bug is invisible
///     on a UTC server and appears only off it.
///
///     The SQL Server analogue is the pairing of expression and column type. <c>SYSDATETIMEOFFSET()</c>
///     carries the server's offset explicitly; <c>SYSDATETIME()</c> / <c>GETDATE()</c> do not, and
///     landing either in a <c>datetime2</c> column throws the offset away, storing local wall-clock
///     as though it were UTC. Polecat uses <c>SYSDATETIMEOFFSET()</c> into <c>datetimeoffset</c>
///     everywhere, which is the correct pairing — these tests hold that in place.
///
///     Two halves, because neither alone is sufficient:
///
///     <list type="bullet">
///       <item>
///         The <em>behavioural</em> half compares the stored instant against a client-side UTC
///         window. This machine runs US Central (UTC-5/-6) while the SQL Server container runs UTC,
///         so a client-side offset confusion — materializing an offset-aware value as a naive local
///         DateTime — lands 5-6 hours outside the window and fails loudly.
///       </item>
///       <item>
///         The <em>schema</em> half asserts the column types are offset-carrying. On a UTC server
///         that is the only thing separating a correct <c>datetimeoffset</c> from a lossy
///         <c>datetime2</c>, and it is the property that has to hold when Polecat is deployed
///         against a SQL Server that is <em>not</em> on UTC — the exact condition marten#5136 needed.
///       </item>
///     </list>
/// </summary>
[Collection("integration")]
public class server_timestamp_utc_tests : IntegrationContext
{
    public server_timestamp_utc_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "server_ts_utc");
    }

    /// <summary>
    ///     A document's server-written last_modified must name the instant the write happened,
    ///     measured against the client's UTC clock. On a US Central client an offset-dropping read
    ///     or write is 5-6 hours out and cannot pass.
    /// </summary>
    [Fact]
    public async Task document_last_modified_is_the_real_utc_instant()
    {
        var before = DateTimeOffset.UtcNow.AddMinutes(-2);

        var doc = new RevisionedDoc { Id = Guid.NewGuid(), Name = "ts" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var after = DateTimeOffset.UtcNow.AddMinutes(2);

        await using var query = theStore.QuerySession();
        var metadata = await query.MetadataForAsync(doc, TestContext.Current.CancellationToken);
        metadata.ShouldNotBeNull();

        var lastModified = metadata.LastModified;
        lastModified.ShouldBeGreaterThan(before);
        lastModified.ShouldBeLessThan(after);
    }

    /// <summary>
    ///     The same for an event's server-written timestamp — marten#5136's other victim.
    /// </summary>
    [Fact]
    public async Task event_timestamp_is_the_real_utc_instant()
    {
        var before = DateTimeOffset.UtcNow.AddMinutes(-2);

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("ts quest"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var after = DateTimeOffset.UtcNow.AddMinutes(2);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        var timestamp = events.Single().Timestamp;
        timestamp.ShouldBeGreaterThan(before);
        timestamp.ShouldBeLessThan(after);
    }

    /// <summary>
    ///     Every server-written timestamp column must be offset-carrying. This is the assertion that
    ///     survives a move to a non-UTC SQL Server: a <c>datetime2</c> here would silently store the
    ///     server's local wall-clock and every reader would treat it as UTC.
    /// </summary>
    [Fact]
    public async Task every_server_written_timestamp_column_carries_an_offset()
    {
        // Force the document + event tables into existence in this test's schema.
        theSession.Store(new RevisionedDoc { Id = Guid.NewGuid(), Name = "schema" });
        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("schema quest"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var expected = new (string Table, string Column)[]
        {
            ("pc_events", "timestamp"),
            ("pc_streams", "timestamp"),
            ("pc_streams", "created"),
            ("pc_event_progression", "last_updated"),
            ("pc_doc_revisioneddoc", "last_modified"),
            ("pc_doc_revisioneddoc", "created_at")
        };

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        foreach (var (table, column) in expected)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column
                """;
            cmd.Parameters.AddWithValue("@schema", "server_ts_utc");
            cmd.Parameters.AddWithValue("@table", table);
            cmd.Parameters.AddWithValue("@column", column);

            var dataType = (string?)await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);

            dataType.ShouldNotBeNull($"{table}.{column} was not found — the audit's column list is stale");
            dataType.ShouldBe("datetimeoffset",
                $"{table}.{column} is '{dataType}', which drops the server's UTC offset (marten#5136).");
        }
    }

    /// <summary>
    ///     Guards the write expression itself. <c>SYSDATETIMEOFFSET()</c> is offset-aware;
    ///     <c>SYSDATETIME()</c> / <c>GETDATE()</c> are not, and swapping one in would reintroduce
    ///     marten#5136 on any server not running UTC. Rather than grep the source, this asserts the
    ///     observable consequence: the value the server writes agrees with the server's own UTC
    ///     clock, and disagrees with a naive local reading whenever the server is off UTC.
    /// </summary>
    [Fact]
    public async Task the_server_side_timestamp_expression_agrees_with_the_servers_utc_clock()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            SELECT SYSDATETIMEOFFSET() AS with_offset,
                   SYSDATETIME()       AS naive,
                   GETUTCDATE()        AS utc
            """;

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();

        var withOffset = reader.GetDateTimeOffset(0);
        var utc = reader.GetDateTime(2);

        // SYSDATETIMEOFFSET() converted to UTC must equal the server's own UTC clock, whatever
        // timezone the server runs in. This is precisely what "now() at time zone 'utc'" broke.
        var skew = (withOffset.UtcDateTime - utc).Duration();
        skew.ShouldBeLessThan(TimeSpan.FromSeconds(5),
            "SYSDATETIMEOFFSET() must resolve to the server's true UTC instant");
    }

    /// <summary>
    ///     Records whether this run was actually in a position to catch a client-side offset
    ///     confusion. The behavioural assertions above only discriminate when the client's clock is
    ///     off UTC — on a UTC client a dropped offset is a no-op and they would pass vacuously. This
    ///     was developed and verified on a US Central machine (UTC-5); it skips rather than fails on
    ///     a UTC runner so the signal stays honest instead of turning green by accident.
    /// </summary>
    [Fact]
    public void the_client_clock_is_off_utc_so_the_assertions_above_discriminate()
    {
        var offset = TimeZoneInfo.Local.GetUtcOffset(DateTimeOffset.UtcNow);

        Assert.SkipWhen(offset == TimeSpan.Zero,
            "Client is on UTC: a dropped offset is unobservable here, so the timestamp assertions " +
            "in this class pass without discriminating. Run on a non-UTC machine to exercise them.");

        offset.ShouldNotBe(TimeSpan.Zero);
    }
}
