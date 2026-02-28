# Polecat

SQL Server-Backed Event Store inside the Critter Stack.

First off, don't get too excited because it's going to be a little while before there's too much happening here.

[![Discord](https://img.shields.io/discord/1074998995086225460?color=blue&label=Chat%20on%20Discord)](https://discord.gg/WMxrvegf8H)
[![Nuget Package](https://badgen.net/nuget/v/polecat)](https://www.nuget.org/packages/Polecat/)
[![Nuget](https://img.shields.io/nuget/dt/polecat)](https://www.nuget.org/packages/Polecat/)

<div align="center">
    <img src="./docs/polecat-logo.png" alt="polecat logo" width="40%">
</div>

## Goals and Approach

* Unless stated otherwise, try to support every feature present in the Marten library as long as it is possible with SQL Server
* Robust Event Store functionality using SQL Server as the storage mechanism
* Closely based on Marten's event sourcing functionality, i.e., basically Marten's `IEventStore` service
* Will depend on JasperFx, JasperFx.Events for event abstractions and projection or subscription base types
* Will use Weasel.SqlServer for automatic database migrations similar to Marten
* Opt into the bigger Critter Stack "stateful resource" model with Weasel to build out schema objects
* Support both conjoined and separate database multi-tenancy
* Projections will be based on the model in JasperFx.Events and supply `SingleStreamProjection`, `MultiStreamProjection`, `EventProjection`, and `FlatTableProjection` right out of the box
* STJ only for the serialization. No Newtonsoft support this time
* `QuickAppend` will be the default event appending approach
* Only support .NET 10
* Only support Sql Server 2025 (v17)
* Utilize the new Sql Server `JSON` type much like Marten uses the PostgreSQL `JSONB`
* Should mimic the test structure of Marten (https://github.com/jasperfx/marten)
* Completely support an "Async Daemon" 

## Documentation

All the documentation is expected to mimic how other JasperFx projects are handled, which is written in Markdown and the docs are published as a static site.

More details coming soon as development progresses.

## License

Copyright © Jeremy D. Miller and contributors.

Polecat is provided as-is under the MIT license. For more information see [LICENSE](LICENSE).

## Code of Conduct

This project has adopted the code of conduct defined by the [Contributor Covenant](http://contributor-covenant.org/) to clarify expected behavior in our community.
