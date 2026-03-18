namespace Polecat.Storage;

/// <summary>
///     Represents a registered subclass in a document hierarchy.
///     Stores the alias used in the doc_type discriminator column.
/// </summary>
internal class SubClassMapping
{
    public SubClassMapping(Type documentType, string? alias = null)
    {
        DocumentType = documentType;
        Alias = alias ?? GenerateAlias(documentType);
    }

    public Type DocumentType { get; }
    public string Alias { get; }

    private static string GenerateAlias(Type type)
    {
        // Convert PascalCase to snake_case: "SuperUser" → "super_user"
        var name = type.Name;
        var chars = new List<char>();
        for (var i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i]))
            {
                chars.Add('_');
            }
            chars.Add(char.ToLowerInvariant(name[i]));
        }
        return new string(chars.ToArray());
    }
}
