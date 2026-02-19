namespace Polecat.Schema.Identity.Sequences;

public interface IReadOnlyHiloSettings
{
    int MaxLo { get; }
    string? SequenceName { get; }
    int MaxAdvanceToNextHiAttempts { get; }
}

public class HiloSettings : IReadOnlyHiloSettings
{
    public int MaxLo { get; set; } = 1000;
    public string? SequenceName { get; set; }
    public int MaxAdvanceToNextHiAttempts { get; set; } = 30;
}
