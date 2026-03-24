using JasperFx.Events;
using Polecat;
using Polecat.AspNetCore;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

builder.Services.AddPolecat(opts =>
{
    opts.ConnectionString = "Server=localhost,11433;Database=polecat_mcp_test;User Id=sa;Password=P@55w0rd;TrustServerCertificate=true";
    opts.Events.StreamIdentity = StreamIdentity.AsGuid;
    opts.Events.EnableCorrelationId = true;
    opts.Events.EnableCausationId = true;
});

var app = builder.Build();

app.MapPolecatMcp();

app.Run();
