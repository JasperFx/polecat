namespace Polecat.Tests.Harness;

/// <summary>
///     xUnit collection definition that shares a single <see cref="DefaultStoreFixture" />
///     across all test classes in the "integration" collection.
/// </summary>
[CollectionDefinition("integration")]
public class IntegrationCollection : ICollectionFixture<DefaultStoreFixture>;
