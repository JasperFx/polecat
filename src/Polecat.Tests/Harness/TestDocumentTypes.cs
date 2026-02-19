using JasperFx;
using Polecat.Attributes;
using Polecat.Metadata;

namespace Polecat.Tests.Harness;

/// <summary>
///     Test document with a Guid Id.
/// </summary>
public class User
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }
}

/// <summary>
///     Test document with a string Id.
/// </summary>
public class StringDoc
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

/// <summary>
///     Test document for various scenarios.
/// </summary>
public class Target
{
    public Guid Id { get; set; }
    public string Color { get; set; } = string.Empty;
    public int Number { get; set; }
    public double Amount { get; set; }
}

/// <summary>
///     Test document with an int Id (HiLo-generated).
/// </summary>
public class IntDoc
{
    public int Id { get; set; }
    public string? Name { get; set; }
}

/// <summary>
///     Test document with a long Id (HiLo-generated).
/// </summary>
public class LongDoc
{
    public long Id { get; set; }
    public string? Name { get; set; }
}

/// <summary>
///     Test document with custom HiLo settings via attribute.
/// </summary>
[HiloSequence(MaxLo = 66, SequenceName = "Entity")]
public class OverriddenHiloDoc
{
    public int Id { get; set; }
}

/// <summary>
///     Soft-deleted via attribute.
/// </summary>
[SoftDeleted]
public class SoftDeletedDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Number { get; set; }
}

/// <summary>
///     Soft-deleted via ISoftDeleted interface (auto-detected).
/// </summary>
public class SoftDeletedWithInterface : ISoftDeleted
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Deleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}

/// <summary>
///     Document with int-based revision tracking via IRevisioned.
/// </summary>
public class RevisionedDoc : IRevisioned
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Version { get; set; }
}

/// <summary>
///     Document with Guid-based optimistic concurrency via IVersioned.
/// </summary>
public class VersionedDoc : IVersioned
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public Guid Version { get; set; }
}
