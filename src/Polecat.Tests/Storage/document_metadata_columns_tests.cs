using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// #241: opt-in document metadata columns (correlation_id / causation_id / last_modified_by /
/// headers) are added to the document table when enabled via the .Metadata(...) DSL (#243) and are
/// populated from the session's metadata on store/update. Previously these values were synced to
/// ITracked in memory and then dropped on the way to SQL.
/// </summary>
public class document_metadata_columns_tests : OneOffConfigurationsContext
{
    public class MetaDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class PlainDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const string Table = "pc_doc_metadoc";
    private string Schema => GetType().Name.ToLowerInvariant();

    private void ConfigureMetadata()
    {
        ConfigureStore(opts => opts.Schema.For<MetaDoc>().Metadata(m =>
        {
            m.CorrelationId.Enabled = true;
            m.CausationId.Enabled = true;
            m.LastModifiedBy.Enabled = true;
            m.Headers.Enabled = true;
        }));
    }

    private async Task<(string? corr, string? cause, string? user, string? headers)> ReadMetadataAsync(Guid id)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT correlation_id, causation_id, last_modified_by, CAST(headers AS nvarchar(max)) " +
            $"FROM [{Schema}].[{Table}] WHERE id = @id";
        var p = cmd.CreateParameter();
        p.ParameterName = "@id";
        p.Value = id;
        cmd.Parameters.Add(p);
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();
        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    [Fact]
    public async Task store_persists_session_metadata_into_columns()
    {
        ConfigureMetadata();

        var doc = new MetaDoc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "corr-1";
            session.CausationId = "cause-1";
            session.LastModifiedBy = "user-1";
            session.SetHeader("k", "v");
            session.Store(doc);
            await session.SaveChangesAsync();
        }

        var (corr, cause, user, headers) = await ReadMetadataAsync(doc.Id);
        corr.ShouldBe("corr-1");
        cause.ShouldBe("cause-1");
        user.ShouldBe("user-1");
        headers.ShouldContain("\"k\"");
        headers.ShouldContain("\"v\"");
    }

    [Fact]
    public async Task update_refreshes_metadata_columns()
    {
        ConfigureMetadata();

        var doc = new MetaDoc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "first";
            session.Store(doc);
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "second";
            session.LastModifiedBy = "editor";
            doc.Name = "B";
            session.Update(doc);
            await session.SaveChangesAsync();
        }

        var (corr, _, user, _) = await ReadMetadataAsync(doc.Id);
        corr.ShouldBe("second");
        user.ShouldBe("editor");
    }

    [Fact]
    public async Task null_session_metadata_persists_as_null()
    {
        ConfigureMetadata();

        var doc = new MetaDoc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc); // no correlation/causation/user/headers set
            await session.SaveChangesAsync();
        }

        var (corr, cause, user, headers) = await ReadMetadataAsync(doc.Id);
        corr.ShouldBeNull();
        cause.ShouldBeNull();
        user.ShouldBeNull();
        headers.ShouldBeNull();
    }

    [Fact]
    public async Task columns_not_created_when_not_configured()
    {
        ConfigureStore(_ => { }); // no metadata config

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new PlainDoc { Id = Guid.NewGuid(), Name = "A" });
            await session.SaveChangesAsync();
        }

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COL_LENGTH('[{Schema}].[pc_doc_plaindoc]', 'correlation_id')";
        var result = await cmd.ExecuteScalarAsync();
        (result == null || result == DBNull.Value).ShouldBeTrue();
    }
}
