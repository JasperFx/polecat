using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Documents;

/// <summary>
///     #922: tenant_id is varchar, so under the default collation (code page 1252) the tenants 東京 and 大阪
///     are both <c>??</c> and read each other's documents. Refused where the tenant is used.
/// </summary>
public class tenant_id_outside_code_page_tests: OneOffConfigurationsContext
{
    private const string Tokyo = "東京";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public tenant_id_outside_code_page_tests()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsAreMultiTenanted());
    }

    [Fact]
    public void a_session_for_such_a_tenant_is_refused_when_it_is_opened()
    {
        var ex = Should.Throw<TenantIdOutsideCodePageException>(() => theStore.LightweightSession(Tokyo));
        ex.TenantId.ShouldBe(Tokyo);
        ex.CodePage.ShouldBe(1252);

        Should.Throw<TenantIdOutsideCodePageException>(() => theStore.QuerySession(Tokyo));
        Should.Throw<TenantIdOutsideCodePageException>(() => theStore.IdentitySession(new SessionOptions { TenantId = Tokyo }));
        Should.Throw<TenantIdOutsideCodePageException>(
            () => theStore.LightweightSession(new SessionOptions { TenantId = Tokyo }));
    }

    [Fact]
    public async Task a_tenant_view_is_refused_when_it_is_named_and_the_session_is_unharmed()
    {
        await using var session = theStore.LightweightSession("red");
        Should.Throw<TenantIdOutsideCodePageException>(() => session.ForTenant(Tokyo));
        Should.Throw<TenantIdOutsideCodePageException>(() => session.ForTenant(Tokyo));

        session.Store(new StringDoc { Id = "a", Name = "x" });
        await session.SaveChangesAsync(Ct);
        (await session.LoadAsync<StringDoc>("a", Ct))!.Name.ShouldBe("x");
    }

    [Fact]
    public async Task ascii_and_in_code_page_tenants_are_unaffected()
    {
        await using var session = theStore.LightweightSession("café");
        session.Store(new StringDoc { Id = "a", Name = "x" });
        await session.SaveChangesAsync(Ct);
        (await session.LoadAsync<StringDoc>("a", Ct))!.Name.ShouldBe("x");
    }
}
