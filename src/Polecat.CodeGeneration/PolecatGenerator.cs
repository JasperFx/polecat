using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Polecat.CodeGeneration
{
    /// <summary>
    ///     Incremental source generator that finds classes annotated with [Document]
    ///     and emits typed document provider classes with pre-computed SQL strings.
    /// </summary>
    [Generator]
    public class PolecatGenerator : IIncrementalGenerator
    {
        private const string DocumentAttributeFullName = "Polecat.Attributes.DocumentAttribute";

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Find all class declarations with the [Document] attribute
            var classDeclarations = context.SyntaxProvider
                .CreateSyntaxProvider(
                    predicate: static (node, _) => IsClassWithAttributes(node),
                    transform: static (ctx, _) => GetDocumentClassInfo(ctx))
                .Where(static info => info != null);

            var compilationAndClasses = context.CompilationProvider
                .Combine(classDeclarations.Collect());

            context.RegisterSourceOutput(compilationAndClasses,
                static (spc, source) => Execute(source.Left, source.Right, spc));
        }

        private static bool IsClassWithAttributes(SyntaxNode node)
        {
            return node is ClassDeclarationSyntax classDecl
                   && classDecl.AttributeLists.Count > 0;
        }

        private static DocumentClassInfo GetDocumentClassInfo(GeneratorSyntaxContext context)
        {
            var classDecl = (ClassDeclarationSyntax)context.Node;
            var model = context.SemanticModel;
            var classSymbol = model.GetDeclaredSymbol(classDecl) as INamedTypeSymbol;

            if (classSymbol == null) return null;

            // Check for [Document] attribute
            foreach (var attr in classSymbol.GetAttributes())
            {
                var attrName = attr.AttributeClass?.ToDisplayString();
                if (attrName == DocumentAttributeFullName)
                {
                    var idProperty = FindIdProperty(classSymbol);
                    if (idProperty == null) return null;

                    return new DocumentClassInfo(
                        classSymbol.Name,
                        classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                        idProperty.Type.ToDisplayString(),
                        idProperty.Name);
                }
            }

            return null;
        }

        private static IPropertySymbol FindIdProperty(INamedTypeSymbol classSymbol)
        {
            // Convention: look for a public property named "Id"
            foreach (var member in classSymbol.GetMembers())
            {
                if (member is IPropertySymbol prop
                    && prop.Name == "Id"
                    && prop.DeclaredAccessibility == Accessibility.Public
                    && !prop.IsStatic)
                {
                    return prop;
                }
            }

            return null;
        }

        private static void Execute(
            Compilation compilation,
            ImmutableArray<DocumentClassInfo> classes,
            SourceProductionContext context)
        {
            if (classes.IsDefaultOrEmpty) return;

            var seen = new System.Collections.Generic.HashSet<string>();
            foreach (var info in classes)
            {
                if (info == null) continue;
                if (!seen.Add(info.FullName)) continue;

                var source = DocumentProviderEmitter.Emit(info);
                context.AddSource(info.ClassName + "DocumentProvider.g.cs",
                    SourceText.From(source, Encoding.UTF8));
            }
        }
    }

    /// <summary>
    ///     Metadata about a class annotated with [Document].
    /// </summary>
    internal sealed class DocumentClassInfo
    {
        public DocumentClassInfo(string className, string namespaceName, string idTypeName, string idPropertyName)
        {
            ClassName = className;
            NamespaceName = namespaceName;
            IdTypeName = idTypeName;
            IdPropertyName = idPropertyName;
        }

        public string ClassName { get; }
        public string NamespaceName { get; }
        public string IdTypeName { get; }
        public string IdPropertyName { get; }

        public string FullName => string.IsNullOrEmpty(NamespaceName)
            ? ClassName
            : NamespaceName + "." + ClassName;

        public string TableName => "pc_doc_" + ClassName.ToLowerInvariant();

        public string SqlIdType
        {
            get
            {
                if (IdTypeName == "System.Guid" || IdTypeName == "Guid")
                    return "uniqueidentifier";
                return "varchar(250)";
            }
        }
    }
}
