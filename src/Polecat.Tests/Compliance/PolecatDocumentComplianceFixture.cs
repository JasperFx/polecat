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
        // while they are all document-only -- they configure the same tables the same way. A suite
        // that needs string stream identity is not: its pc_streams.id is nvarchar where a
        // Guid-identity store's is uniqueidentifier. Sharing one schema makes whichever suite
        // migrates second try to retype a primary key under a live foreign key, and Weasel fails
        // with "Could not drop constraint" during fixture init -- every fact red for a reason that
        // has nothing to do with the code under test. Separate schema, no interaction.
        //
        // #479: keyed off the DECLARED identity rather than off "this suite has event types",
        // because it is the identity that changes the column type. The two happen to coincide today.
        if (config.StreamIdentity == JasperFx.Events.StreamIdentity.AsString)
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

        // #479 / jasperfx#672: the stream identity the suite DECLARES, replacing the inference this
        // fixture used to make.
        //
        // The gap the old comment here called out is closed. A suite that appends by stream key --
        // DocumentSessionEventsCompliance does throughout -- had no way to say so, so this fixture
        // had to work it out from "the config named event types, and the only suite that does keys
        // its streams by string". That is a guess about another repository's source: correct on the
        // day it was written, silently wrong the moment a document suite named an event type without
        // wanting string identity. It also failed loudly and misleadingly when it was missing, since
        // Polecat refuses a string-keyed StartStream under AsGuid with
        // ExistingStreamIdCollisionException, an error naming a stream collision rather than the
        // identity mismatch that caused it.
        //
        // Nullable and null by default, meaning "leave the store on its own default", so this is
        // inert for every document-only suite and no existing behavior changes.
        if (config.StreamIdentity.HasValue)
        {
            options.Events.StreamIdentity = config.StreamIdentity.Value;
        }

        // Event types stay registered from the config: a suite appending through the session's Events
        // accessor needs its types known before the store is built, or the append writes an event
        // whose type name only resolves by assembly scanning and the read back is at the mercy of
        // what happens to be loaded. Empty for the document-only suites.
        foreach (var eventType in config.EventTypes)
        {
            options.Events.AddEventType(eventType);
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
