<h1>Reasoning Integration Specifications - 3</h1>

> Feedback raised during review of [`reasoningIntegrationSpecs-2.md`](./reasoningIntegrationSpecs-2.md); to be revisited as implementation progresses.

---

**Contents**:

- [Needs Sharpening](#needs-sharpening)
- [Missing](#missing)
- [Structural Note](#structural-note)

---

# Needs Sharpening

**Point 1 - Trigger condition parameters unnamed**: The trigger condition is accepted as a configurable parameter but its structure is not defined. Without at minimum a placeholder definition (e.g. "call executor every N ticks; every M ticks if a disruption is active"), Point 5 (trigger condition as a UC artefact) cannot be implemented.

**Point 4 - UC function list is incomplete**: Read tools for `AgentContext` assembly (`ops_warehouse_state`, `hist_demand_actuals`, `ops_pending_orders`, `ops_cost_accumulator`, `ops_active_disruptions`) are not mentioned but are the first UC functions needed. Write functions (reorder placement) come after the context assembly loop is working. Ordering matters.

**Point 6 - Pull consumers' source not stated**: The document names pull consumers (MLflow, LangFuse, dashboards) but not what they pull from. The answer - event log and ops/hist tables - should be stated explicitly to prevent the implementation from adding direct engine-to-consumer push calls that would couple the engine to those systems unnecessarily.

**Points 7 & 8 - Deferred without a minimum viable resolution**: For the simulation, a non-Lakebase resolution should be named: history injected into `AgentContext` from `hist_*` tables by the engine; feedback latency handled by including `expected_arrival_tick` in `hist_reorder_decisions` and only attributing outcomes to decisions where `current_tick >= expected_arrival_tick + 1`.

---

# Missing

**Message queue message schema**: The queue carries something - at minimum: trigger tick, trigger condition met, and a state pointer or snapshot sufficient for the executor to assemble `AgentContext`. This schema is a design decision, not an implementation detail.

**Point 5 - Trigger condition governance unresolved**: "Preferably through UC?" is not a decision. Pick UC function or config parameter; state why. Leaving it open risks it being resolved inconsistently during implementation.

**Job schedule vs. tick loop in simulation**: How does the "job schedule" trigger mode manifest inside the simulation? Is the tick loop retained as the scheduler (with the job schedule framing being a production analogy only), or is the simulation runner restructured to actually call a scheduler? This determines whether the simulation genuinely demonstrates the production pattern.

**LangFuse trace structure not defined**: Where is the LangFuse client initialised? What is the trace hierarchy (`run → tick → item`)? How are `sim_id` and `tick` passed into trace metadata? This must be decided before agent code is written so traces are comparable across runs.

---

# Structural Note

Resolved decisions (Points 2, 3, 6, 9) and open decisions (Points 1, 5, 7, 8) are formatted uniformly in the source document. A "Resolved / Open" split would prevent open items from being carried silently into implementation.