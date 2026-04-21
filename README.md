# Polecat

SQL Server-Backed Event Store inside the Critter Stack.

[Polecat](https://polecat.jasperfx.net/) is live and ready for real usage! See [Jeremy's announcement on the 1.0 release](https://jeremydmiller.com/2026/03/22/announcing-polecat-event-sourcing-with-sql-server/).

[![Discord](https://img.shields.io/discord/1074998995086225460?color=blue&label=Chat%20on%20Discord)](https://discord.gg/WMxrvegf8H)
[![Nuget Package](https://badgen.net/nuget/v/polecat)](https://www.nuget.org/packages/Polecat/)
[![Nuget](https://img.shields.io/nuget/dt/polecat)](https://www.nuget.org/packages/Polecat/)

<div align="center">
    <img src="./docs/polecat-logo.png" alt="polecat logo" width="40%">
</div>

## Support Plans

<div align="center">
    <img src="https://www.jasperfx.net/logo.png" alt="JasperFx logo" width="70%">
</div>

While Polecat is open source, [JasperFx Software offers paid support and consulting contracts](https://jasperfx.net/support-plans/) for Marten.

## Help us keep working on this project 💚

[Become a Sponsor on GitHub](https://github.com/sponsors/JasperFX) by sponsoring monthly or one time.

## Working with the Code

Before getting started you will need the following in your environment:

### 1. .NET SDK 10.0+

Available [here](https://dotnet.microsoft.com/download)

### 2. SQL Server 2025 or above database

The fastest possible way to develop with Polecat is to run SQL Server in a Docker container. Assuming that you have Docker running on your local box, type:
`docker-compose up -d`. This will run SQL Server with native JSON support required for full Polecat functionality (JSON_OBJECT, GENERATE_SERIES, etc.).

> On Apple Silicon Macs, SQL Server 2025 under Rosetta may crash due to memory constraints.
> Azure SQL Edge can be used as a fallback for basic development with `docker-compose -f docker-compose.yml -f docker-compose.azure-sql-edge.yml up -d`. 
> Be aware about limited functionality in this case.

## Documentation

All the documentation is expected to mimic how other JasperFx projects are handled, which is written in Markdown and the docs are published as a static site.

## License

Copyright © Jeremy D. Miller and contributors.

Polecat is provided as-is under the MIT license. For more information see [LICENSE](LICENSE).

## Code of Conduct

This project has adopted the code of conduct defined by the [Contributor Covenant](http://contributor-covenant.org/) to clarify expected behavior in our community.
