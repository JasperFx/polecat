using JasperFx;
using JasperFx.Events.ComplianceTests;
using JasperFx.Events.Documents;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Compliance;

/// <summary>
///     Polecat's implementation of the cross-store <b>document</b> compliance seam (#443 /
///     jasperfx#647). Unlike the event sourcing fixture, this one is deliberately not generic over a
///     session pair: every member of the document contract is reachable through the shared
///     <see cref="IDocumentSessionFactory" />, so there is nothing for a generic to carry.
/// </summary>
public class PolecatDocumentComplianceFixture : DocumentStorageComplianceFixture
{
    private DocumentStore? _store;

    protected override async Task BuildStoreAsync(DocumentComplianceConfig config)
    {
        var schemaName = (config.SchemaName ?? "compliance_documents").ToLowerInvariant();

        _store?.Dispose();

        _store = new DocumentStore(new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = schemaName,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        });

        // Polecat applies schema changes explicitly rather than lazily, and the suite's very first
        // act may be a read against a table nothing has written yet.
        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Every document type the suite declares gets its table up front, so a query-first test does
        // not race table creation.
        foreach (var documentType in config.DocumentTypes)
        {
            await _store.Database.EnsureStorageExistsAsync(documentType, CancellationToken.None);
        }
    }

    /// <summary>
    ///     <see cref="IDocumentStore" /> implements <see cref="IDocumentSessionFactory" /> directly —
    ///     there is no adapter here, which is the point of #443.
    /// </summary>
    public override IDocumentSessionFactory Sessions =>
        _store ?? throw new InvalidOperationException("The store has not been configured yet.");

    public override async Task CleanDocumentDataAsync()
    {
        if (_store is null) return;

        await _store.Advanced.Clean.DeleteAllDocumentsAsync();
    }

    public override ValueTask DisposeAsync()
    {
        _store?.Dispose();
        _store = null;
        return default;
    }
}
