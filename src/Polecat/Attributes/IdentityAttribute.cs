#nullable enable
using System;

namespace Polecat.Attributes;

/// <summary>
///     Use to designate an Id property or field on a document type that doesn't follow the
///     id/Id naming convention. This attribute takes priority over the conventional "Id" property lookup.
/// </summary>
/// <example>
///     <code>
///     public class Customer
///     {
///         [Identity]
///         public string CustomerCode { get; set; }
///
///         public string Name { get; set; }
///     }
///     </code>
/// </example>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public class IdentityAttribute : Attribute
{
}
