using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Linq.SoftDeletes;
using Polecat.Tests.Harness;

namespace Polecat.Tests.SoftDeletes;

/// <summary>
///     gh-558: the four operators on <see cref="SoftDeletedExtensions" /> are refused against a
///     document type that is not soft-deleted, rather than degrading to "no filter".
/// </summary>
/// <remarks>
///     <para>
///         The bug these pin was silent, not loud. Polecat appended <c>is_deleted = 0/1</c> only
///         behind a <c>DeleteStyle == SoftDelete</c> guard, and nothing rejected the call when that
///         guard was false — so <c>Query&lt;User&gt;().IsDeleted()</c> ran as
///         <c>Query&lt;User&gt;()</c> and returned every row. <c>IsDeleted()</c> reads as a lifecycle
///         or authorization filter at the call site, so the failure surfaced as "shows rows that
///         should have been excluded".
///     </para>
///     <para>
///         Marten (<c>AssertDocumentTypeIsSoftDeleted</c>, from each of its four parsers) and Fisher
///         (one filter pass, keyed on <c>UsedSoftDeleteOperator</c>) both refuse. Polecat asserts in
///         <c>LinqQueryParser</c>, which every query shape and every provider passes through.
///     </para>
/// </remarks>
[Collection("integration")]
public class soft_delete_operators_against_hard_deleted_types : IntegrationContext
{
    public soft_delete_operators_against_hard_deleted_types(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "soft_delete_refusal"; });
    }

    private static IQueryable<User> Apply(IQueryable<User> queryable, string operatorName) =>
        operatorName switch
        {
            nameof(SoftDeletedExtensions.IsDeleted) => queryable.IsDeleted(),
            nameof(SoftDeletedExtensions.MaybeDeleted) => queryable.MaybeDeleted(),
            nameof(SoftDeletedExtensions.DeletedSince) => queryable.DeletedSince(DateTimeOffset.UtcNow.AddDays(-1)),
            nameof(SoftDeletedExtensions.DeletedBefore) => queryable.DeletedBefore(DateTimeOffset.UtcNow),
            _ => throw new ArgumentOutOfRangeException(nameof(operatorName), operatorName, null)
        };

    /// <summary>
    ///     All four operators are refused, <c>MaybeDeleted()</c> included.
    /// </summary>
    /// <remarks>
    ///     <c>MaybeDeleted()</c> has the one arguably-defensible reading of the four — "include the
    ///     deleted ones too" is trivially satisfied by a table that has none, so every row really is
    ///     the honest answer. It is refused anyway, because writing it says the author believes the
    ///     type is soft-deleted and that belief is the bug; the next edit is usually
    ///     <c>IsDeleted()</c>, which is not defensible at all. Marten and Fisher refuse all four too.
    /// </remarks>
    [Theory]
    [InlineData(nameof(SoftDeletedExtensions.IsDeleted))]
    [InlineData(nameof(SoftDeletedExtensions.MaybeDeleted))]
    [InlineData(nameof(SoftDeletedExtensions.DeletedSince))]
    [InlineData(nameof(SoftDeletedExtensions.DeletedBefore))]
    public async Task each_operator_is_refused_on_a_hard_deleted_type(string operatorName)
    {
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Apply(query.Query<User>(), operatorName).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain(typeof(User).FullName!);
        ex.Message.ShouldContain(operatorName + "()");
        ex.Message.ShouldContain("not configured for soft deletes");
    }

    /// <summary>
    ///     The message says how to fix it, in Polecat's own configuration vocabulary.
    /// </summary>
    [Fact]
    public async Task the_refusal_names_the_remedy()
    {
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted().ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("[SoftDeleted]");
        ex.Message.ShouldContain("ISoftDeleted");
        ex.Message.ShouldContain("SoftDeleted()");
    }

    /// <summary>
    ///     The refusal is not attached to one execution path. Every public entry that builds a
    ///     statement goes through <c>LinqQueryParser.Parse</c>, so all six of the provider's paths
    ///     refuse — which is the whole reason the assertion lives in the parser rather than beside
    ///     each of the six places that append the <c>is_deleted</c> predicate.
    /// </summary>
    [Fact]
    public async Task every_execution_path_refuses()
    {
        await using var query = theStore.QuerySession();

        // ExecuteAsync — the list, single-value and aggregate path
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted().ToListAsync(TestContext.Current.CancellationToken));
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted().CountAsync(TestContext.Current.CancellationToken));

        // BuildStatement — ToSql, which never reaches the database at all
        Should.Throw<BadLinqExpressionException>(() => query.ToSql(query.Query<User>().IsDeleted()));

        // ExecuteJsonArrayAsync
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted().ToJsonArrayAsync(TestContext.Current.CancellationToken));

        // ExecuteJsonFirstWithVersionAsync
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted()
                .ToJsonFirstWithVersionAsync(TestContext.Current.CancellationToken));

        // StreamPagedJsonArrayAsync
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
        {
            using var stream = new MemoryStream();
            await query.Query<User>().IsDeleted()
                .StreamPagedJsonArray(1, 10, stream, TestContext.Current.CancellationToken);
        });

        // ExecuteCursorPageJsonAsync
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>().IsDeleted().OrderBy(x => x.Id)
                .ToJsonPageByCursorAsync(null, 10, TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     The refusal survives being buried in a longer chain — it is keyed on the operator having
    ///     been seen anywhere in the expression tree, not on its being the outermost call.
    /// </summary>
    [Fact]
    public async Task refused_when_the_operator_is_not_the_outermost_call()
    {
        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<User>()
                .IsDeleted()
                .Where(x => x.Age > 10)
                .OrderBy(x => x.LastName)
                .Take(5)
                .ToListAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     The events table has no deletion state either, and cannot be given one.
    /// </summary>
    [Fact]
    public async Task refused_over_an_event_query()
    {
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Events.QueryAllRawEvents().IsDeleted()
                .ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("The event store");
    }

    /// <summary>
    ///     A soft-deleted type is untouched — the assertion only fires where there is no column.
    /// </summary>
    [Fact]
    public async Task a_soft_deleted_type_still_works()
    {
        var alive = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "alive-558" };
        var dead = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "dead-558" };

        theSession.Store(alive, dead);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var deleting = theStore.LightweightSession();
        deleting.Delete(dead);
        await deleting.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();

        var deletedOnly = await query.Query<SoftDeletedDoc>().IsDeleted()
            .ToListAsync(TestContext.Current.CancellationToken);
        deletedOnly.Count.ShouldBe(1);
        deletedOnly[0].Id.ShouldBe(dead.Id);

        var everything = await query.Query<SoftDeletedDoc>().MaybeDeleted()
            .ToListAsync(TestContext.Current.CancellationToken);
        everything.Count.ShouldBe(2);
    }

    /// <summary>
    ///     gh-558, the non-LINQ half: <c>UndoDeleteWhere</c> issues an UPDATE against
    ///     <c>is_deleted</c> / <c>deleted_at</c>, columns <c>DocumentTable</c> only adds for a
    ///     soft-deleted type. Unguarded it reached SQL Server as "Invalid column name 'is_deleted'"
    ///     at <c>SaveChangesAsync</c> — loud, but from the wrong layer and naming a column the caller
    ///     never mentioned.
    /// </summary>
    [Fact]
    public async Task undo_delete_where_is_refused_on_a_hard_deleted_type()
    {
        await using var session = theStore.LightweightSession();

        var ex = Should.Throw<InvalidOperationException>(() =>
            session.UndoDeleteWhere<User>(x => x.Age > 0));

        ex.Message.ShouldContain(typeof(User).FullName!);
        ex.Message.ShouldContain("nothing to undo");
    }

    [Fact]
    public async Task undo_delete_where_still_works_on_a_soft_deleted_type()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "undo-558" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var deleting = theStore.LightweightSession();
        deleting.Delete(doc);
        await deleting.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var undoing = theStore.LightweightSession();
        undoing.UndoDeleteWhere<SoftDeletedDoc>(x => x.Id == doc.Id);
        await undoing.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
    }
}
