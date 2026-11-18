using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Daemon;

// Regression for Polecat#146 (jasperfx#356): under SkipApplyErrors (the
// JasperFx.Events 2.0 default), a projection Project/Apply failure is recorded as
// a DeadLetterEvent and the shard keeps advancing. DeadLetterEvent is a Polecat
// document (pc_doc_deadletterevent); this exercises that document-backed storage
// plus the IEventDatabase count reads CountDeadLetterEventsAsync /
// FetchDeadLetterCountsAsync (LINQ queries over the document). Mirrors Marten's
// when_skipping_events_in_daemon recipe (EventProjection that throws on a poison
// event, both error options set to skip).

public record DeadLetterThing(Guid Id, string Name, bool Poison);

public class DeadLetterView
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public partial class PoisonEventProjection : EventProjection
{
    public void Project(DeadLetterThing e, IDocumentSession ops)
    {
        if (e.Poison)
        {
            throw new InvalidOperationException("poison event — boom");
        }

        ops.Store(new DeadLetterView { Id = e.Id, Name = e.Name });
    }
}
