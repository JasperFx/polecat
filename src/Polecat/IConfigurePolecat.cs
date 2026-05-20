namespace Polecat;

/// <summary>
///     Implement this interface and register in DI to apply additional
///     configuration to StoreOptions after construction. Re-based on the lifted
///     <see cref="JasperFx.IConfigureStore{TOptions}"/> (jasperfx#334) closed over
///     Polecat's <see cref="StoreOptions"/>; this marker preserves the
///     <c>Polecat.IConfigurePolecat</c> name. Implementers'
///     <c>Configure(IServiceProvider, StoreOptions)</c> satisfies the base.
/// </summary>
public interface IConfigurePolecat : JasperFx.IConfigureStore<StoreOptions>
{
}
