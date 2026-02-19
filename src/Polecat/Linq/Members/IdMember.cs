namespace Polecat.Linq.Members;

/// <summary>
///     Represents the document's Id property, mapped directly to the "id" column.
/// </summary>
internal class IdMember : IQueryableMember
{
    public IdMember(Type idType)
    {
        MemberType = idType;
    }

    public Type MemberType { get; }
    public string TypedLocator => "id";
    public string RawLocator => "id";
    public bool IsBoolean => false;

    public object? ConvertValue(object? value) => value;
}
