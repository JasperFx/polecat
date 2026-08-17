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

        // #475 / jasperfx#669: every document suite declares the SAME schema name, which is harmless
        // while they are all document-only -- they configure the same tables the same way. The event
        // suite is not: it needs string stream identity (see below), so its pc_streams.id is
        // nvarchar where a Guid-identity store's is uniqueidentifier. Sharing one schema makes
        // whichever suite migrates second try to retype a primary key under a live foreign key, and
        // Weasel fails with "Could not drop constraint" during fixture init -- all five facts red for
        // a reason that has nothing to do with the code under test. Separate schema, no interaction.
        if (config.EventTypes.Count > 0)
        {
            schemaName += "_events";
        }

        _store?.Dispose();

        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = schemaName,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        // jasperfx#665 / #472: the strong-typed identity facts key a document by a wrapper, and the
        // suite declares those wrappers separately from the document types because the identity type
        // is the one thing a document type alone does not tell a store.
        //
        // Replayed here for the same reason StoreOptions.RegisterValueType exists at all (#459):
        // Polecat resolves value types on its own, so this is eager validation rather than a
        // requirement -- measured, the three new facts pass with this loop removed. It is still the
        // seam's intent, and it is what a store that does NOT auto-discover would need, so the
        // fixture honors the declared configuration rather than relying on Polecat's discovery.
        foreach (var valueType in config.ValueTypes)
        {
            options.RegisterValueType(valueType);
        }

        // #475 / jasperfx#669: DocumentSessionEventsCompliance appends through the session's Events
        // accessor, so the event types it names have to be registered before the store is built --
        // otherwise the append writes an event whose type name only resolves by assembly scanning,
        // and the read back is at the mercy of what happens to be loaded. Only the one document suite
        // that is also an event-store suite populates this list; it is empty for the other four.
        if (config.EventTypes.Count > 0)
        {
            // ⚠️ That suite keys every stream by a STRING, and DocumentComplianceConfig carries no
            // StreamIdentity knob the way ComplianceStoreConfig does -- so the requirement is
            // implicit in the suite's body rather than declared. Polecat defaults to AsGuid, under
            // which a string-keyed StartStream is refused, and the failure names a stream collision
            // rather than an identity mismatch: three of the five facts died with
            // ExistingStreamIdCollisionException on freshly minted Guids. Inferred from the suite,
            // not from the config, which is why it is spelled out here. Upstream gap, tracked by the
            // jasperfx side; do NOT "fix" it by editing the shared suite.
            options.Events.StreamIdentity = JasperFx.Events.StreamIdentity.AsString;

            foreach (var eventType in config.EventTypes)
            {
                options.Events.AddEventType(eventType);
            }
        }

        _store = new DocumentStore(options);

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
