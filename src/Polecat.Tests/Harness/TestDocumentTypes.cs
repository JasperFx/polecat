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
///     Test document for various scenarios. Extended to support patching tests.
/// </summary>
public class Target
{
    public Guid Id { get; set; }
    public string Color { get; set; } = string.Empty;
    public int Number { get; set; }
    public double Amount { get; set; }

    // String properties for patching tests
    public string? String { get; set; }
    public string? AnotherString { get; set; }
    public string? StringField { get; set; }

    // Numeric properties for increment tests
    public long Long { get; set; }
    public double Double { get; set; }
    public float Float { get; set; }
    public decimal Decimal { get; set; }

    // Recursive nested property for deep patching
    public Target? Inner { get; set; }

    // Array properties for append/insert/remove tests
    public int[] NumberArray { get; set; } = [];
    public Target[] Children { get; set; } = [];

    // Dictionary properties for keyed operations
    public Dictionary<string, Target> ChildrenDictionary { get; set; } = new();
    public Dictionary<string, int> NumberByKey { get; set; } = new();
    public Dictionary<Guid, int> NumberByGuidKey { get; set; } = new();
    public Dictionary<string, long> LongByKey { get; set; } = new();
    public Dictionary<string, double> DoubleByKey { get; set; } = new();
    public Dictionary<string, float> FloatByKey { get; set; } = new();
    public Dictionary<string, decimal> DecimalByKey { get; set; } = new();

    // Nested object for deep collection tests
    public TargetNested? NestedObject { get; set; }

    public static Target Random(bool withChildren = false)
    {
        var random = new Random();
        var target = new Target
        {
            Id = Guid.NewGuid(),
            Color = random.Next(3) switch { 0 => "Blue", 1 => "Green", _ => "Red" },
            Number = random.Next(1, 100),
            Amount = random.NextDouble() * 100,
            String = Guid.NewGuid().ToString().Substring(0, 8),
            AnotherString = Guid.NewGuid().ToString().Substring(0, 8),
            StringField = Guid.NewGuid().ToString().Substring(0, 8),
            Long = random.Next(1, 1000),
            Double = random.NextDouble() * 100,
            Float = (float)(random.NextDouble() * 100),
            Decimal = (decimal)(random.NextDouble() * 100),
            NumberArray = [random.Next(0, 100), random.Next(0, 100), random.Next(0, 100)]
        };

        if (withChildren)
        {
            target.Inner = Random();
            target.Children = [Random(), Random(), Random()];
            target.NestedObject = new TargetNested([Random(), Random()]);
        }

        return target;
    }
}

public record TargetNested(Target[] Targets);

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
