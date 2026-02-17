# Polecat

SQL Server-Backed Event Store inside the Critter Stack

First off, don't get too excited because it's going to be a little while before there's too much happening here.

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

