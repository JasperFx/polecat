using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     marten#5244's Polecat twin: <c>CompactStreamAsync&lt;T&gt;</c> branches on the store's
///     configured <see cref="StreamIdentity" /> rather than on which overload the caller used, and
///     never asserted the two agreed. Calling the Guid overload against a string-identified store
///     took the AsString branch, read a null StreamKey, matched no stream, and returned
///     successfully having compacted NOTHING — no exception, and no way to tell a no-op from a
///     completed compaction. The mirror case failed with "Nullable object must have a value",
///     which names nothing the caller can act on.
/// </summary>
[Collection("integration")]
public class stream_compacting_identity_mismatch_tests
{
    private static DocumentStore CreateStore(StreamIdentity identity, string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.StreamIdentity = identity;
        });
    }

    [Fact]
    public async Task guid_overload_on_a_string_identified_store_throws_instead_of_silently_compacting_nothing()
    {
        using var store = CreateStore(StreamIdentity.AsString, "compact_idmix_str");

        await using var session = store.LightweightSession();

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => session.Events.CompactStreamAsync<QuestParty>(Guid.NewGuid()));

        ex.Message.ShouldContain("identify streams with strings");
    }

    [Fact]
    public async Task string_overload_on_a_guid_identified_store_throws_an_actionable_message()
    {
        using var store = CreateStore(StreamIdentity.AsGuid, "compact_idmix_guid");

        await using var session = store.LightweightSession();

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => session.Events.CompactStreamAsync<QuestParty>("some-stream-key"));

        ex.Message.ShouldContain("identify streams with Guids");
    }
}
