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
    public async Task a_session_for_such_a_tenant_is_refused_before_its_first_command()
    {
        await using var session = theStore.LightweightSession(Tokyo);
        var ex = await Should.ThrowAsync<TenantIdOutsideCodePageException>(
            () => session.LoadAsync<StringDoc>("a", Ct));
        ex.TenantId.ShouldBe(Tokyo);
        ex.CodePage.ShouldBe(1252);
    }

    [Fact]
    public async Task a_save_for_such_a_tenant_is_refused()
    {
        await using var session = theStore.LightweightSession(Tokyo);
        session.Store(new StringDoc { Id = "a", Name = "x" });
        await Should.ThrowAsync<TenantIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_tenant_view_is_refused_on_the_next_command_and_stays_refused()
    {
        await using var session = theStore.LightweightSession("red");
        var view = session.ForTenant(Tokyo);
        await Should.ThrowAsync<TenantIdOutsideCodePageException>(() => view.LoadAsync<StringDoc>("a", Ct));
        await Should.ThrowAsync<TenantIdOutsideCodePageException>(() => session.LoadAsync<StringDoc>("a", Ct));
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
