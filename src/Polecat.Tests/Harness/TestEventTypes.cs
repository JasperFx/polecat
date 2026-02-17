namespace Polecat.Tests.Harness;

// Simple event types for testing the event store.
// These will be used across multiple test classes starting in Stage 5.

public record QuestStarted(string Name);

public record MembersJoined(int Day, string Location, string[] Members);

public record MembersDeparted(int Day, string Location, string[] Members);

public record QuestEnded(string Name);

public record MonsterSlain(string Name, int DamageDealt);

public record ArrivedAtLocation(string Location, int Day);

public record MonsterDestroyed(string Name);
