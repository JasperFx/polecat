namespace Polecat.Tests.Linq;

public class LinqTarget
{
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public int Age { get; set; }
    public bool IsActive { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public TargetColor Color { get; set; }
    public Address? Address { get; set; }
    public decimal Price { get; set; }
    public double Score { get; set; }
    public long BigNumber { get; set; }
    public List<string> Tags { get; set; } = new();
    public int[] Numbers { get; set; } = [];
}

public enum TargetColor
{
    Red,
    Green,
    Blue
}

public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
}
