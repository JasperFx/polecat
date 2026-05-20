namespace Polecat;

/// <summary>
///     Interface for seeding initial data into the document store on startup.
///     Implement this interface and register via StoreOptions.InitialData.
///     Re-based on the lifted <see cref="JasperFx.IInitialData{TStore}"/>
///     (jasperfx#334) closed over Polecat's <see cref="IDocumentStore"/>; this
///     marker preserves the <c>Polecat.IInitialData</c> name. Implementers'
///     <c>Populate(IDocumentStore, CancellationToken)</c> satisfies the base.
/// </summary>
public interface IInitialData : JasperFx.IInitialData<IDocumentStore>
{
}

/// <summary>
///     Collection of <see cref="IInitialData"/> instances executed on startup.
///     Inherits the lifted <see cref="JasperFx.InitialDataCollection{TStore}"/>
///     (which carries the lambda <c>Add</c> overload Polecat originally
///     contributed), closed over <see cref="IDocumentStore"/>.
/// </summary>
public class InitialDataCollection : JasperFx.InitialDataCollection<IDocumentStore>
{
}
