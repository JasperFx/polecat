namespace Polecat.Events.TestSupport;

public partial class ProjectionScenario
{
    /// <summary>
    ///     General hook to run custom assertions against projected data.
    /// </summary>
    public void AssertAgainstProjectedData(string description, Func<IQuerySession, CancellationToken, Task> assertions)
    {
        Assertion(assertions).Description = description;
    }

    /// <summary>
    ///     Verify that a document with the supplied Guid id exists.
    /// </summary>
    public void DocumentShouldExist<T>(Guid id, Action<T>? assertions = null) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document == null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' does not exist");
            assertions?.Invoke(document);
        }).Description = $"Document {typeof(T).Name} with id '{id}' should exist";
    }

    /// <summary>
    ///     Verify that a document with the supplied string id exists.
    /// </summary>
    public void DocumentShouldExist<T>(string id, Action<T>? assertions = null) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document == null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' does not exist");
            assertions?.Invoke(document);
        }).Description = $"Document {typeof(T).Name} with id '{id}' should exist";
    }

    /// <summary>
    ///     Verify that a document with the supplied int id exists.
    /// </summary>
    public void DocumentShouldExist<T>(int id, Action<T>? assertions = null) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document == null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' does not exist");
            assertions?.Invoke(document);
        }).Description = $"Document {typeof(T).Name} with id '{id}' should exist";
    }

    /// <summary>
    ///     Verify that a document with the supplied long id exists.
    /// </summary>
    public void DocumentShouldExist<T>(long id, Action<T>? assertions = null) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document == null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' does not exist");
            assertions?.Invoke(document);
        }).Description = $"Document {typeof(T).Name} with id '{id}' should exist";
    }

    /// <summary>
    ///     Asserts that a document with a given Guid id has been deleted or does not exist.
    /// </summary>
    public void DocumentShouldNotExist<T>(Guid id) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document != null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' exists, but should not.");
        }).Description = $"Document {typeof(T).Name} with id '{id}' should not exist or be deleted";
    }

    /// <summary>
    ///     Asserts that a document with a given string id has been deleted or does not exist.
    /// </summary>
    public void DocumentShouldNotExist<T>(string id) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document != null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' exists, but should not.");
        }).Description = $"Document {typeof(T).Name} with id '{id}' should not exist or be deleted";
    }

    /// <summary>
    ///     Asserts that a document with a given int id has been deleted or does not exist.
    /// </summary>
    public void DocumentShouldNotExist<T>(int id) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document != null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' exists, but should not.");
        }).Description = $"Document {typeof(T).Name} with id '{id}' should not exist or be deleted";
    }

    /// <summary>
    ///     Asserts that a document with a given long id has been deleted or does not exist.
    /// </summary>
    public void DocumentShouldNotExist<T>(long id) where T : class
    {
        Assertion(async (session, ct) =>
        {
            var document = await session.LoadAsync<T>(id, ct);
            if (document != null)
                throw new Exception($"Document {typeof(T).Name} with id '{id}' exists, but should not.");
        }).Description = $"Document {typeof(T).Name} with id '{id}' should not exist or be deleted";
    }
}
