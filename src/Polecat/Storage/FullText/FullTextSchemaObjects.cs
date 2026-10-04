using Weasel.Core;

namespace Polecat.Storage.FullText;

/// <summary>
///     #685 — the schema objects a document type's full-text declarations amount to, in the order they
///     have to be created in. One place that answers the question so the two paths that ask it (the
///     whole-database feature schema and the lazy first-use ensurer) cannot answer it differently.
/// </summary>
internal static class FullTextSchemaObjects
{
    /// <summary>
    ///     The token table then the trigger, or nothing at all when the mapping declares no full-text
    ///     index. The trigger is second because its body inserts into the table.
    /// </summary>
    public static ISchemaObject[] For(DocumentMapping mapping)
        => mapping.FullTextIndexes.Count == 0
            ? []
            : [new FullTextTokenTable(mapping), new FullTextTrigger(mapping, mapping.FullTextIndexes)];
}
