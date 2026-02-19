namespace Polecat.Serialization;

[Flags]
public enum NonPublicMembersStorage
{
    Default = 0,
    NonPublicSetters = 1,
    NonPublicDefaultConstructor = 2,
    NonPublicConstructor = 4,
    All = NonPublicSetters | NonPublicDefaultConstructor | NonPublicConstructor
}
