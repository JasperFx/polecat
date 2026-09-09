using System.Diagnostics.CodeAnalysis;
using Polecat.Storage;

namespace Polecat.Internal;

/// <summary>
///     Forces the document providers a store's <em>configuration</em> implies into existence, for the
///     callers that must see them before any session has opened.
/// </summary>
/// <remarks>
///     <para>
///         Polecat materializes a <see cref="DocumentProvider" /> lazily, on the first
///         <c>GetProvider&lt;T&gt;()</c> for that type. That is the right default for a running
///         application — a document type nobody touches costs nothing — but it means
///         <c>Options.Providers.AllProviders</c> is <b>empty</b> in a process that has only
///         configured a store and not yet used it. Two callers see exactly that state and need the
///         configured set anyway:
///     </para>
///     <list type="bullet">
///         <item>
///             <c>PolecatDatabase.BuildFeatureSchemas()</c>, which decides what DDL the store
///             declares. This is polecat#573: <c>db-ef-migration add</c> configures a store, asks it
///             for its schema and exits, so the generated migration carried the event store tables
///             and none of the document tables — a projection's <c>pc_doc_*</c> table was simply
///             absent, while Marten's equivalent migration had it.
///         </item>
///         <item>
///             <c>DocumentStore.TryCreateUsage()</c>, the diagnostic descriptor CritterWatch reads on
///             a freshly booted service, which had grown its own copy of this walk.
///         </item>
///     </list>
///     <para>
///         One helper for both, because the two answers must agree: a document type that appears in
///         the monitoring console and not in the migration (or the reverse) is a bug report either
///         way, and the copies had already drifted — the descriptor's walk covered
///         <c>IAggregateProjection.AggregateType</c> only, so it missed every published type a
///         projection declares beyond its own aggregate.
///     </para>
/// </remarks>
internal static class ConfiguredDocumentProviders
{
    /// <summary>
    ///     Materialize a provider for every document type <paramref name="options" /> configures:
    ///     each <c>Schema.For&lt;T&gt;()</c> registration, and every type a registered projection
    ///     publishes.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Types with custom projection storage are excluded, and that exclusion is
    ///         load-bearing.</b> <c>EfCoreSingleStreamProjection&lt;TDoc, TDbContext&gt;</c> derives
    ///         from <c>SingleStreamProjection&lt;TDoc, Guid&gt;</c>, so its document is published like
    ///         any other and is indistinguishable by shape — but it lives in a
    ///         <c>DbContext</c>, not in a <c>pc_doc_*</c> table. Materializing a provider for it would
    ///         invent a Polecat table for a document Polecat does not store, and #573's whole subject
    ///         is the EF Core migration those tables land in: the fix would have added a spurious
    ///         table to the very migration it exists to correct. <c>CustomProjectionStorageProviders</c>
    ///         is keyed by document type and is the registration that redirects storage, so it is the
    ///         honest test for "Polecat does not own this document". Those entities reach a migration
    ///         by their own route, as <c>ExtendedSchemaObjects</c>.
    ///     </para>
    ///     <para>
    ///         Idempotent: <c>GetProvider</c> is a get-or-add, so repeated calls are cheap and callers
    ///         need not coordinate.
    ///     </para>
    /// </remarks>
    [RequiresDynamicCode("Closes document storage generics over the configured document types via Type.MakeGenericType, the same way a first GetProvider<T>() call would.")]
    [RequiresUnreferencedCode("Materializes document providers, which reflect over the document type's members.")]
    internal static void MaterializeConfigured(StoreOptions options)
    {
        var seen = new HashSet<Type>();

        foreach (var expression in options.Schema.Expressions)
        {
            var expressionType = expression.GetType();
            if (!expressionType.IsGenericType) continue;

            var documentType = expressionType.GetGenericArguments()[0];
            if (seen.Add(documentType))
            {
                options.Providers.GetProvider(documentType);
            }
        }

        foreach (var documentType in options.Projections.All.SelectMany(x => x.PublishedTypes()))
        {
            if (options.CustomProjectionStorageProviders.ContainsKey(documentType)) continue;

            if (seen.Add(documentType))
            {
                options.Providers.GetProvider(documentType);
            }
        }
    }
}
