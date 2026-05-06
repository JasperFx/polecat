using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.ProjectModel;
using Nuke.Common.Tooling;
using Nuke.Common.Tools.DotNet;
using Nuke.Common.Tools.Npm;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

class Build : NukeBuild
{
    public static int Main() => Execute<Build>(x => x.Compile);

    [Parameter("Configuration to build - Default is 'Debug' (local) or 'Release' (server)")]
    readonly Configuration Configuration = IsLocalBuild ? Configuration.Debug : Configuration.Release;

    [Solution(GenerateProjects = true)] readonly Solution Solution;

    [Parameter] readonly string Framework;

    [Parameter("SQL Server connection string used for integration tests.")]
    readonly string ConnectionString =
        "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

    [Parameter] readonly string Project;

    // Project references for Attach/Detach. Map sibling repository folder name -> project names.
    private Dictionary<string, string[]> ReferencedProjects = new()
    {
        { "jasperfx", ["JasperFx", "JasperFx.Events"] },
        { "weasel", ["Weasel.Core", "Weasel.SqlServer", "Weasel.EntityFrameworkCore"] }
    };

    // Subset of projects above that are also referenced as NuGet packages from src projects.
    string[] Nugets = ["JasperFx", "JasperFx.Events", "Weasel.SqlServer", "Weasel.EntityFrameworkCore"];

    // Map a NuGet package name -> the Polecat csproj(s) that reference it. Used by Attach/Detach.
    private Dictionary<string, string[]> NugetConsumers = new()
    {
        { "JasperFx", ["src/Polecat/Polecat.csproj"] },
        { "JasperFx.Events", ["src/Polecat/Polecat.csproj"] },
        { "Weasel.SqlServer", ["src/Polecat/Polecat.csproj", "src/Polecat.EntityFrameworkCore.Tests/Polecat.EntityFrameworkCore.Tests.csproj"] },
        { "Weasel.EntityFrameworkCore", ["src/Polecat.EntityFrameworkCore/Polecat.EntityFrameworkCore.csproj"] }
    };

    Target Test => _ => _
        .DependsOn(TestPolecat)
        .DependsOn(TestAspnetcore)
        .DependsOn(TestEfCore);

    Target Init => _ => _
        .Executes(() =>
        {
            Clean();
        });

    Target NpmInstall => _ => _
        .Executes(() => NpmTasks.NpmInstall(c => c
            .AddProcessAdditionalArguments("--loglevel=error")));

    Target Compile => _ => _
        .DependsOn(Restore)
        .Executes(() =>
        {
            DotNetBuild(s => s
                .SetProjectFile(Solution)
                .SetConfiguration(Configuration)
                .EnableNoRestore());
        });

    Target Restore => _ => _
        .DependsOn(Init)
        .Executes(() =>
        {
            DotNetRestore(s => s
                .SetProjectFile(Solution));
        });

    Target CompileProject => _ => _
        .DependsOn(Init)
        .Executes(() =>
        {
            if (string.IsNullOrEmpty(Project))
            {
                Log.Error("Project parameter is required. Usage: --project <path-to-project>");
                throw new ArgumentException("Project parameter must be specified");
            }

            Log.Information($"Restoring project: {Project}");
            DotNetRestore(s => s
                .SetProjectFile(Project));

            Log.Information($"Compiling project: {Project}");
            DotNetBuild(s => s
                .SetProjectFile(Project)
                .SetConfiguration(Configuration)
                .SetFramework(Framework)
                .EnableNoRestore());
        });

    Target TestPolecat => _ => _
        .ProceedAfterFailure()
        .Executes(() =>
        {
            DotNetTest(c => c
                .SetProjectFile("src/Polecat.Tests")
                .SetConfiguration(Configuration)
                .EnableNoBuild()
                .EnableNoRestore()
                .SetFramework(Framework));
        });

    Target TestAspnetcore => _ => _
        .ProceedAfterFailure()
        .Executes(() =>
        {
            DotNetTest(c => c
                .SetProjectFile("src/Polecat.AspNetCore.Testing")
                .SetConfiguration(Configuration)
                .EnableNoBuild()
                .EnableNoRestore()
                .SetFramework(Framework));
        });

    Target TestEfCore => _ => _
        .ProceedAfterFailure()
        .Executes(() =>
        {
            DotNetTest(c => c
                .SetProjectFile("src/Polecat.EntityFrameworkCore.Tests")
                .SetConfiguration(Configuration)
                .EnableNoBuild()
                .EnableNoRestore()
                .SetFramework(Framework));
        });

    Target RebuildDb => _ => _
        .Executes(() =>
        {
            ProcessTasks.StartProcess("docker", "compose down");
            ProcessTasks.StartProcess("docker", "compose up -d");
        });

    Target InitDb => _ => _
        .Executes(async () =>
        {
            ProcessTasks.StartProcess("docker", "compose up -d");
            await WaitForDatabaseToBeReady();
        });

    Target InstallMdSnippets => _ => _
        .ProceedAfterFailure()
        .Executes(() =>
        {
            const string toolName = "markdownSnippets.tool";

            if (IsDotNetToolInstalled(toolName))
            {
                Log.Information($"{toolName} is already installed, skipping this step.");
                return;
            }

            DotNetToolInstall(c => c
                .SetPackageName(toolName)
                .EnableGlobal());
        });

    Target Docs => _ => _
        .DependsOn(NpmInstall, InstallMdSnippets)
        .Executes(() => NpmTasks.NpmRun(s => s.SetCommand("docs")));

    Target DocsBuild => _ => _
        .DependsOn(NpmInstall, InstallMdSnippets)
        .Executes(() => NpmTasks.NpmRun(s => s.SetCommand("docs-build")));

    Target PublishDocs => _ => _
        .DependsOn(NpmInstall, InstallMdSnippets, DocsBuild)
        .Executes(() => NpmTasks.NpmRun(s => s.SetCommand("deploy")));

    Target Pack => _ => _
        .DependsOn(Compile)
        .Executes(() =>
        {
            var projects = new[]
            {
                "./src/Polecat",
                "./src/Polecat.AspNetCore",
                "./src/Polecat.CodeGeneration",
                "./src/Polecat.EntityFrameworkCore"
            };

            foreach (var project in projects)
            {
                DotNetPack(s => s
                    .SetProject(project)
                    .SetOutputDirectory("./artifacts")
                    .SetConfiguration(Configuration.Release));
            }
        });

    /// <summary>
    /// Switch Polecat from NuGet PackageReferences for JasperFx and Weasel
    /// to local ProjectReferences pointing at sibling repositories
    /// (~/code/jasperfx and ~/code/weasel). Used for cross-repo development.
    /// </summary>
    Target Attach => _ => _.Executes(() =>
    {
        foreach (var pair in ReferencedProjects)
        {
            foreach (var projectName in pair.Value)
            {
                addProject(pair.Key, projectName);
            }
        }

        foreach (var nuget in Nugets)
        {
            if (!NugetConsumers.TryGetValue(nuget, out var consumers)) continue;
            foreach (var consumer in consumers)
            {
                DotNet($"remove {consumer} package {nuget}");
            }
        }
    });

    /// <summary>
    /// Inverse of Attach. Restore Polecat to using NuGet PackageReferences for
    /// JasperFx and Weasel (latest prerelease) and remove the local sibling
    /// repository ProjectReferences.
    /// </summary>
    Target Detach => _ => _.Executes(() =>
    {
        foreach (var pair in ReferencedProjects)
        {
            foreach (var projectName in pair.Value)
            {
                removeProject(pair.Key, projectName);
            }
        }

        // dotnet sln add can transitively pull in additional projects
        // (e.g. JasperFx.SourceGeneration when adding JasperFx) that we
        // never explicitly attached. Strip any remaining /Attached/ folder
        // from the slnx so detach is fully reversible.
        StripAttachedFolderFromSlnx();

        foreach (var nuget in Nugets)
        {
            if (!NugetConsumers.TryGetValue(nuget, out var consumers)) continue;
            foreach (var consumer in consumers)
            {
                DotNet($"add {consumer} package {nuget} --prerelease");
            }
        }
    });

    private void StripAttachedFolderFromSlnx()
    {
        var slnxPath = (string)Solution.Path;
        if (!File.Exists(slnxPath)) return;

        var content = File.ReadAllText(slnxPath);
        // Match the entire <Folder Name="/Attached/"> ... </Folder> block, including any
        // surrounding whitespace + line breaks so the file stays clean.
        var pattern = @"\s*<Folder Name=""/Attached/"">(?:[\s\S]*?)</Folder>";
        var stripped = System.Text.RegularExpressions.Regex.Replace(content, pattern, string.Empty);

        if (stripped != content)
        {
            File.WriteAllText(slnxPath, stripped);
            Log.Information("Removed /Attached/ folder from {Slnx}", slnxPath);
        }
    }

    private void addProject(string repository, string projectName)
    {
        var path = Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");
        var slnPath = Solution.Path;
        DotNet($"sln {slnPath} add {path} --solution-folder Attached");

        if (Nugets.Contains(projectName) && NugetConsumers.TryGetValue(projectName, out var consumers))
        {
            foreach (var consumer in consumers)
            {
                DotNet($"add {consumer} reference {path}");
            }
        }
    }

    private void removeProject(string repository, string projectName)
    {
        var path = Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");

        if (Nugets.Contains(projectName) && NugetConsumers.TryGetValue(projectName, out var consumers))
        {
            foreach (var consumer in consumers)
            {
                DotNet($"remove {consumer} reference {path}");
            }
        }

        var slnPath = Solution.Path;
        DotNet($"sln {slnPath} remove {path}");
    }

    private async Task WaitForDatabaseToBeReady()
    {
        var attempt = 0;
        while (attempt < 10)
        {
            try
            {
                await using var conn = new SqlConnection(ConnectionString);
                await conn.OpenAsync();

                var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteNonQueryAsync();

                Log.Information("SQL Server is up and ready!");
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error while waiting for the database to be ready");
                Thread.Sleep(250);
                attempt++;
            }
        }
    }

    bool IsDotNetToolInstalled(string toolName)
    {
        var process = ProcessTasks.StartProcess("dotnet", "tool list -g", logOutput: false);
        process.AssertZeroExitCode();
        var output = process.Output.Select(x => x.Text).ToList();

        return output.Any(line => line.Contains(toolName, StringComparison.OrdinalIgnoreCase));
    }

    static void Clean()
    {
        var results = AbsolutePath.Create("results");
        var artifacts = AbsolutePath.Create("artifacts");
        results.CreateOrCleanDirectory();
        artifacts.CreateOrCleanDirectory();
    }
}
