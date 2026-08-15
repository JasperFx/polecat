using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

/// <summary>
///     #462: a disposed session must not keep working. Before this guard, Polecat's session had no
///     disposed flag, so after <c>DisposeAsync</c> the same instance still accepted Store/Append and
///     still <em>committed successfully</em> on SaveChangesAsync -- the connection lifetime simply
///     re-established itself lazily. Use-after-dispose is normally a loud, immediate bug; here it was
///     silent, and the silence favoured the worst case: a session captured past the scope that owned
///     it going on writing with nobody owning the transaction boundary.
/// </summary>
[Collection("integration")]
public class disposed_session_tests : IntegrationContext
{
    public disposed_session_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<IDocumentSession> disposedSession()
    {
        var session = theStore.LightweightSession();
        await session.DisposeAsync();
        return session;
    }

    // ===== The reported case: writes on a disposed session used to commit =====

    [Fact]
    public async Task store_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Store(new User { Id = Guid.NewGuid(), FirstName = "After", LastName = "Dispose", Age = 1 }));
    }

    [Fact]
    public async Task save_changes_on_a_disposed_session_throws_instead_of_committing()
    {
        var id = Guid.NewGuid();

        var session = theStore.LightweightSession();
        session.Store(new User { Id = id, FirstName = "Never", LastName = "Committed", Age = 1 });
        await session.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(async () =>
            await session.SaveChangesAsync(TestContext.Current.CancellationToken));

        // And nothing reached the database.
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(id, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task insert_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Insert(new User { Id = Guid.NewGuid(), FirstName = "A", LastName = "B", Age = 1 }));
    }

    [Fact]
    public async Task update_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Update(new User { Id = Guid.NewGuid(), FirstName = "A", LastName = "B", Age = 1 }));
    }

    [Fact]
    public async Task delete_by_document_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Delete(new User { Id = Guid.NewGuid(), FirstName = "A", LastName = "B", Age = 1 }));
    }

    [Fact]
    public async Task delete_by_id_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() => session.Delete<User>(Guid.NewGuid()));
    }

    [Fact]
    public async Task delete_where_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() => session.DeleteWhere<User>(x => x.Age > 10));
    }

    [Fact]
    public async Task queue_sql_command_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() => session.QueueSqlCommand("select 1"));
    }

    // ===== Event operations =====

    [Fact]
    public async Task start_stream_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Too late")));
    }

    [Fact]
    public async Task append_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        Should.Throw<ObjectDisposedException>(() =>
            session.Events.Append(Guid.NewGuid(), new QuestStarted("Too late")));
    }

    // ===== Read operations, on both session flavours =====

    [Fact]
    public async Task load_on_a_disposed_session_throws()
    {
        var session = await disposedSession();

        await Should.ThrowAsync<ObjectDisposedException>(async () =>
            await session.LoadAsync<User>(Guid.NewGuid(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task load_on_a_disposed_identity_session_throws()
    {
        var session = theStore.IdentitySession();
        await session.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(async () =>
            await session.LoadAsync<User>(Guid.NewGuid(), TestContext.Current.CancellationToken));
    }

    // An identity-map hit short-circuits before the base class's guard, so the override needs its own.
    [Fact]
    public async Task load_of_a_mapped_document_on_a_disposed_identity_session_throws()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Mapped", LastName = "Already", Age = 1 });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var session = theStore.IdentitySession();
        (await session.LoadAsync<User>(id, TestContext.Current.CancellationToken)).ShouldNotBeNull();
        await session.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(async () =>
            await session.LoadAsync<User>(id, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task query_on_a_disposed_query_session_throws()
    {
        var session = theStore.QuerySession();
        await session.DisposeAsync();

        Should.Throw<ObjectDisposedException>(() => session.Query<User>());
    }

    [Fact]
    public async Task load_on_a_disposed_query_session_throws()
    {
        var session = theStore.QuerySession();
        await session.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(async () =>
            await session.LoadAsync<User>(Guid.NewGuid(), TestContext.Current.CancellationToken));
    }

    // ===== Disposal itself stays well behaved =====

    [Fact]
    public async Task dispose_is_idempotent()
    {
        var session = theStore.LightweightSession();

        await session.DisposeAsync();
        await Should.NotThrowAsync(async () => await session.DisposeAsync());
    }

    [Fact]
    public async Task a_live_session_is_unaffected()
    {
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new User { Id = id, FirstName = "Still", LastName = "Works", Age = 1 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(id, TestContext.Current.CancellationToken)).ShouldNotBeNull();
    }
}
