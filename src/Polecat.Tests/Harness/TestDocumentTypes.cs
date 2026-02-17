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
