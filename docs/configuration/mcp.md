# MCP Server

Polecat ships with a built-in [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that exposes your event store configuration as read-only tools. This lets AI assistants and agents introspect your Polecat setup for diagnostics, code generation guidance, and operational visibility.

The MCP server uses the **Streamable HTTP** transport (stateless POST-based JSON-RPC) and is implemented as ASP.NET Core Minimal API endpoints in the `Polecat.AspNetCore` package.

## Installation

Install the `Polecat.AspNetCore` NuGet package:

```bash
dotnet add package Polecat.AspNetCore
```

## Setup

Register the MCP endpoints in your ASP.NET Core application:

```csharp
using Polecat.AspNetCore;

var app = builder.Build();

app.MapPolecatMcp();
```

This registers a single POST endpoint at `/polecat/mcp/` that handles all MCP JSON-RPC requests.

### Custom Route Prefix

You can change the default route prefix:

```csharp
app.MapPolecatMcp("/api/polecat-mcp");
```

## Authorization

`MapPolecatMcp()` returns a `RouteGroupBuilder`, so you can chain standard ASP.NET Core endpoint configuration including authorization policies:

```csharp
app.MapPolecatMcp()
    .RequireAuthorization("AdminPolicy");
```

Or with a specific authorization policy:

```csharp
app.MapPolecatMcp()
    .RequireAuthorization(policy =>
    {
        policy.RequireRole("admin");
    });
```

::: warning
The MCP endpoints expose internal configuration details about your event store schema, projections, and event types. You **should** apply authorization to these endpoints in production environments.
:::

## Available Tools

The MCP server exposes three read-only tools:

### get_event_store_configuration

Returns the full event store options snapshot including:

| Property | Description |
| :--- | :--- |
| `streamIdentity` | `AsGuid` or `AsString` |
| `tenancyStyle` | `Single` or `Conjoined` |
| `databaseSchemaName` | Schema for event tables |
| `enableCorrelationId` | Correlation ID tracking enabled |
| `enableCausationId` | Causation ID tracking enabled |
| `enableHeaders` | Custom event headers enabled |

### list_known_event_types

Lists all event types registered with the event store. Each entry includes:

| Property | Description |
| :--- | :--- |
| `eventTypeName` | The alias stored in the database `type` column (e.g. `members_joined`) |
| `dotNetTypeName` | The full .NET type name |

### list_projections

Lists all projections and subscriptions. Each entry includes:

| Property | Description |
| :--- | :--- |
| `name` | Projection class name |
| `implementationType` | Full .NET type name |
| `type` | Subscription type (e.g. `Snapshot`, `MultiStream`, `FlatTableProjection`) |
| `shards` | Array of shard identifiers |

## MCP Protocol Details

The endpoint implements the MCP [Streamable HTTP transport](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http). All requests are JSON-RPC 2.0 POST requests to the single endpoint.

### Supported Methods

| Method | Description |
| :--- | :--- |
| `initialize` | Handshake — returns server capabilities |
| `tools/list` | Lists available tools with descriptions and input schemas |
| `tools/call` | Executes a tool by name and returns results |

### Example Request

```bash
curl -X POST http://localhost:5000/polecat/mcp/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": { "name": "get_event_store_configuration" }
  }'
```

### Example Response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"streamIdentity\":\"AsGuid\",\"tenancyStyle\":\"Single\",\"databaseSchemaName\":\"dbo\",...}"
      }
    ]
  }
}
```
