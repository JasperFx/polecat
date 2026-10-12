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
    ///     The stemmer functions when any index stems (#642), then the token table, then the trigger — or
    ///     nothing at all when the mapping declares no full-text index. The functions come first and the
    ///     trigger last because the trigger's body calls the one and inserts into the other, and a
    ///     generated script renders CREATEs in exactly this order with no migration's deferral.
    /// </summary>
    /// <remarks>
    ///     The functions are per SCHEMA, not per document type, so two Porter-indexed types in one schema
    ///     both yield them. <c>DocumentFeatureSchema</c> drops the repeat; the lazy first-use path migrates
    ///     one type at a time, where a repeat simply finds the function already there.
    /// </remarks>
    public static ISchemaObject[] For(DocumentMapping mapping)
    {
        if (mapping.FullTextIndexes.Count == 0) return [];

        var objects = new List<ISchemaObject>();
        if (mapping.FullTextIndexes.Any(x => x.Tokenizer == FullTextTokenizer.Porter))
        {
            objects.AddRange(PorterStemFunction.For(mapping.DatabaseSchemaName));
        }

        objects.Add(new FullTextTokenTable(mapping));
        objects.Add(new FullTextTrigger(mapping, mapping.FullTextIndexes));
        return objects.ToArray();
    }
}
