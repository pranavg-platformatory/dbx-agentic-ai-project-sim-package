<h1>Reasoning Integration Specifications - 1</h1>

***Outlining Concerns and Tentative Approaches***

**NOTE**: **FLAG** sections indicate clarifications, issues, gaps, etc. Their format:

> **FLAG: [Flag Label]**: [Content]

---

**Contents**:

- [Context](#context)
- [Operations Mechanism vs. Reasoning Mechanism](#operations-mechanism-vs-reasoning-mechanism)
- [Structure for Incorporating Reasoning](#structure-for-incorporating-reasoning)
- [Key Questions w.r.t. Operation-Reasoning Interaction](#key-questions-wrt-operation-reasoning-interaction)
  - [Suggestion 1](#suggestion-1)
  - [Suggestion 2](#suggestion-2)
  - [Tentative Approach](#tentative-approach)
- [Exploring Suggestion 2](#exploring-suggestion-2)
  - [Core Decision-Making Loop](#core-decision-making-loop)
  - [Simulation-Based Architecture](#simulation-based-architecture)
  - [Production-Level (PROD) Considerations](#production-level-prod-considerations)
  - [Generalised Architecture (*applicable for PROD*)](#generalised-architecture-applicable-for-prod)
    - [Overview](#overview)
    - [Key Architectural Points to Address](#key-architectural-points-to-address)
      - [1. AgentContext Assembly: Query Live vs. Maintain Cached View](#1-agentcontext-assembly-query-live-vs-maintain-cached-view)
      - [2. The Engine Does 2 Different Things](#2-the-engine-does-2-different-things)
      - [3. Y's Directionality is Underspecified](#3-ys-directionality-is-underspecified)
      - [4. `env` Carries Too Much Semantic Weight](#4-env-carries-too-much-semantic-weight)
      - [5. \[IMPORTANT\] Agent is Stateless, Engine is Stateful](#5-important-agent-is-stateless-engine-is-stateful)
  - [Suggestion to Enable Experiential Learning](#suggestion-to-enable-experiential-learning)

---

# Context
Currently, we have the following simulation setup:

```
Sim Tables
- env       (internal)    | not exposed
- ops       (operational) | exposed
- hist      (validation)  | exposed
- event_log (auditing)    | exposed
||
Sim Engine
- Tick 1
- Tick 2
- ...
||
Agent Wrapper
```

> **Reference**: [`implementation`](../implementation/)

> **FLAG: Simulation as Reference Implementation**: The simulation engine is not merely a test harness - it is the validated reference implementation of the operations mechanism. The tick loop's sub-step ordering, state write semantics, and event log structure should directly inform the design of the production monitor process and decision executor. When those components are built, the simulation spec is the first document to consult, not an afterthought.

# Operations Mechanism vs. Reasoning Mechanism
In a sense, the real-world, production-level agent would involve some aspects of the simulation engine, namely the aspects involving regular monitoring of the environment (specifically, the operational tables) and involving the performing or scheduling of regular interactions with the environment.

Hence, it is advantageous that our current setup has decoupled the scheduling/operations mechanism and the LLM/reasoning mechanism, since both have different requirements, development lifecycles and architectures. That being said, in production, we would need some interface between the environment and the reasoning mechansim; currently, the simulation engine fits this role, but we would need to define an interface that generalises for any production-level environment.

> **FLAG: Context Quality Bounds Reasoning Quality**: The decoupling is only as valuable as the quality of the interface between the two mechanisms. The reasoning mechanism's output is bounded by the richness and accuracy of the context it receives. Poor context assembly - stale data, missing history, incomplete disruption state - produces poor decisions regardless of the reasoning mechanism's capability. Context assembly quality should be treated as a first-class design concern alongside the reasoning mechanism itself, not subordinated to it.

# Structure for Incorporating Reasoning
Since the operations mechanism is rule-based, the reasoning mechanism would:

- Receive structured context
- Reply with a structured response

The operations mechanism would be a rule-based system that:

- Directly interacts with the environment
- Encapsulates context into a well-defined structure
- Processes responses as per a well-defined procedure

> **FLAG: Response Processing is a Design Surface**: "Processes responses as per a well-defined procedure" is load-bearing but underspecified here. The operations mechanism must handle: (1) structurally invalid responses (parsing failures), (2) logically invalid responses (quantities outside `[min_order_qty, max_order_qty]`, budget violations), and (3) LLM call failures or timeouts. Each case requires a defined fallback - either retry, degrade to rule-based policy, or halt with an alert. This fallback design is absent from the plan and must be resolved before implementation begins in production.

# Key Questions w.r.t. Operation-Reasoning Interaction
> **NOTE**: *We shall not be consider reasoning-specific aspects for now.*

How often should the reasoning be leveraged by the operations mechanism?

This is important, since every reasoning call has a cost.

## Suggestion 1
To be flexible w.r.t. the above question while also being effective, we should make reasoning something that the system does not depend on for fine-grained decision-making (i.e. for exact decision sequences). Rather, the system uses reasoning to optimise a decision-making procedural loop, by tweaking the loop's parameters to minimise/maximise certain objective functions (i.e. certain metrics that we would be using to judge performance).

For this, we need:

- Performance evaluation metrics (a.k.a. objective functions)
- Well-defined procedural decision-making loop
- A set of configurable parameters

Parameterisation should be able to optimise decision-making indefinitely, in theory.

> **FLAG: Parameterisation Scope is Undefined**: The set of "configurable parameters" that Suggestion 1 would tune is not named. In the simulation, natural candidates are `reorder_point`, `order_qty`, `agent_history_window_ticks`, and the trigger condition that determines when the agent is called. Defining this parameter set now - even tentatively - would make Suggestion 1 a concrete design option rather than an abstract aspiration, and would allow Suggestion 2's implementation to be structured so that these parameters are already externalised and tunable when the time comes to layer Suggestion 1 on top.

## Suggestion 2
Assume that decision-making is infrequent enough for direct integration of reasoning mechanism with the operations mechanism. This is architecturally easier, but has limitations in configuring the reasoning frequency to more feasible levels if the decision-making needs to be more frequent or if reasoning costs need to be cut down.

> **FLAG: The Frequency Assumption Should Be Made Explicit**: Suggestion 2 rests on an assumption - that decisions are infrequent enough to justify a direct LLM call per decision event - but does not quantify "infrequent enough." Before this assumption is accepted, it is worth estimating: given the expected tick rate (or real-world reorder review cadence), the per-call LLM latency, and the per-call token cost, what is the implied call frequency and total cost over a representative evaluation period? If the numbers are acceptable, the assumption holds. If not, Suggestion 1 or a sparse-calling hybrid (see the scalability alternatives in `mosaicAiArchitecture-applicationSpecific.md`, Section 6.3) becomes necessary sooner than anticipated.

> **FLAG: Suggestion 2 and Suggestion 1 Are Composable, Not Mutually Exclusive**: Suggestion 2 implemented with externalised, configurable parameters is structurally compatible with Suggestion 1. Suggestion 1 is then simply a meta-loop that adjusts those parameters based on observed outcome metrics - it does not require redesigning the agent or the operations mechanism. The plan should reflect this: the guidance is not "start with 2, switch to 1 later" but "implement 2 with 1 in mind, so that the transition requires only an additional layer, not a redesign."

## Tentative Approach
Start with suggestion 2, while keeping an eye on suggestion 1.

> **FLAG: Make "Keeping an Eye On" Concrete**: As stated, this is too vague to act on. A more actionable formulation: implement Suggestion 2, but (a) externalise the decision trigger condition as a configurable parameter from the start, (b) log LLM call frequency and token cost per simulation run in MLflow, and (c) define a threshold at which the Suggestion 1 meta-loop becomes worth implementing (e.g. token cost exceeds X per run, or call frequency exceeds Y per hour in production). This turns "keep an eye on" into a measurable criterion.

# Exploring Suggestion 2
## Core Decision-Making Loop
- **Input**: `AgentContext` instance for the current tick
- **Processing**: LLM-reasoning
- **Output**: `list[ReorderDecision]` for the next tick

> **FLAG: Output Timing Precision**: The output is described as decisions "for the next tick." More precisely: decisions are made at sub-step 4 of tick T and placed as orders within tick T. Their effect on stock is felt at tick T + lead_time, not tick T + 1. The phrasing "for the next tick" implies a one-tick lag that is not how the simulation works and could mislead future implementers. Suggest rewording to: "decisions placed at tick T, taking effect at tick T + lead_time."

## Simulation-Based Architecture

```
            +-----> AgentContext
            |       |
            |       v
env <--> engine     LLMAgentWrapper <--(inherits contract from)-- BaseAgent
            ^       |
            |       v
            +------ list[ReorderDecision]
```

Currently:

- `engine` and `env` are part of the same simulation setup
- In production, `engine` and `env` would naturally be decoupled

## Production-Level (PROD) Considerations
**Scheduling**:

- Currently, agent decision is made synchronously per tick
- In PROD, environment and agent are decoupled
- Hence, there may be no "tick loop" equivalent in PROD
- Hence, agent decision would be triggered in one of the following ways:
    - Event-driven action
    - Scheduled jobs
    - Real-time API call by a separate monitoring mechanism

> **FLAG: Trigger Mode Has Downstream Architectural Consequences**: The three scheduling modes (event-driven, scheduled, real-time API call) are not equivalent in their implications. Event-driven requires a message broker or event bus; scheduled jobs require a job orchestrator (e.g. Databricks Jobs, Airflow); real-time API call requires the monitor process to be a persistent service with low-latency access to environment state. The choice of trigger mode determines the infrastructure around the decision executor and should be made deliberately rather than left open. In the simulation-to-production transition, starting with scheduled jobs is the lowest-complexity option and the closest analogue to the tick loop.

**`engine` interface**:

- In the simulation, it ensures events take place in the environment
- However, in PROD, environment is dynamic on its own
- Hence, `engine` cannot be an event generator
- Instead, it would serve as:
    - A monitor for the environment
    - A tool to perform tasks (e.g. place orders, send alerts)
    - A way to trigger reasoning and coordinate the execution of responses

> **FLAG: Resilience Gap**: Neither the scheduling section nor the engine interface section addresses what happens when the LLM call fails, times out, or returns an unparseable response. In the simulation, a failed call crashes the tick and is acceptable during development. In production, the operations mechanism needs a defined fallback for each failure mode: at minimum, a retry policy and a graceful degradation to the rule-based policy. This is a production-readiness requirement, not an optional enhancement, and its absence here is a gap that should be filled before any production deployment design is finalised.

## Generalised Architecture (*applicable for PROD*)
### Overview

```
                             +--(assembles)--> AgentContext
       Y <--(call)------+    |                 |
                        |    |                 v
+----- X <--(reorder)-- engine                 LLMAgentWrapper
|                       |    ^                 |
+--> env <--(monitors)--+    |                 v
                             +--(to execute)-- list[ReorderDecision]
```

- X: Placeholder for one or more of the following:
    - API for placing reorders (that ultimately change env)
    - Functions for altering env (e.g. runtime parameters, history, event log, etc.)
- Y: Placeholder for one or more of the following:
    - Tools to perform various actions (e.g. alerting, visualisation, etc.)
    - API(s) to interact with external systems
    - Other systems (e.g. dashboards, analytics engine, etc.)

> **FLAG: This Diagram Is Superseded**: The refined diagram in Point 2 below (monitor process / decision executor split) is architecturally more accurate than this overview diagram. This diagram should be retained as the conceptual starting point, but the refined version should be treated as the working architecture going forward. Consider updating this overview diagram to reflect the monitor/executor split to avoid confusion between the two representations.

### Key Architectural Points to Address
#### 1. AgentContext Assembly: Query Live vs. Maintain Cached View
In the simulation, the runner assembles `AgentContext` from Delta tables before calling `agent.decide()`. In the generalised architecture, the diagram shows `engine` assembling `AgentContext`, which is correct. But there is a latent question: does the engine assemble context by *querying* the environment directly, or by *maintaining its own view* of environment state?

In the simulation these are equivalent; the engine reads Delta tables that it also writes. In production they may not be. If `env` is an ERP (Enterprise Resource Planning) system with API rate limits or high query latency, the engine may need to maintain a cached or materialised view of relevant state rather than querying live on every decision trigger. This distinction should be flagged even if not yet resolved, because it affects how the engine is designed and what consistency guarantees the assembled `AgentContext` carries.

> **FLAG: Cache Invalidation Strategy Is Environment-Dependent**: The resolution of live-query vs. cached-view depends on the nature of the production environment. A WMS exposing a streaming event bus (e.g. Kafka) favours event-sourced cache maintenance - the engine subscribes to state-change events and maintains a current view. An ERP with polling APIs favours a materialised view refreshed on a configurable schedule. The choice also determines the staleness bound on `AgentContext`: a cached view may be N seconds or minutes behind reality, and the agent should ideally be aware of this (e.g. via a `context_assembled_at` timestamp in `AgentContext`). This is worth designing for even if the production environment is not yet known.

#### 2. The Engine Does 2 Different Things
The engine performs 2 distinct activities in the generalised architecture:

1. **Continuous monitoring**: Watching `env` for state changes <br> (*e.g. stock levels, incoming demand, active disruptions*)
2. **Triggered execution**:
   1. Assembling context
   2. Calling the agent when a decision is needed
   3. Executing the response

These are not the same process. Monitoring is ongoing and potentially high-frequency; execution is episodic and triggered. In the simulation they are synchronised by the tick loop. In production they would naturally be separate processes, namely a monitoring process and a decision executor. Conflating them into a single `engine` node obscures this.

A more precise generalised architecture separates these concerns:

```
env <--(monitors continuously)-- monitor process
                                      |
                                      v (on trigger condition)
                                 decision executor
                                      |
                                      +---(assembles)---> AgentContext
                                      |                        |
                                      |                        v
                                      |                  LLMAgentWrapper
                                      |                        |
                                      |                        v
                                      +<--(to execute)--- list[ReorderDecision]
                                      |
                                      +--(calls X, Y as needed)
```

> **FLAG: Trigger Condition Must Be a Governed Artefact**: The trigger condition that causes the monitor process to hand off to the decision executor is left implicit. In the simulation, this is "every tick." In production, it is a named, configurable rule (e.g. "stock falls below `reorder_point` for any item," or "N hours have elapsed since the last decision"). This trigger condition is itself a tunable parameter - it is the primary lever for controlling LLM call frequency (connecting back to Suggestion 1). It should be: version-controlled, logged per activation, and testable independently of the decision executor. Designing it as a hardcoded condition inside the monitor process would be a mistake.

> **FLAG: Inter-Process Communication Is Unspecified**: The refined diagram shows the monitor process handing off to the decision executor "on trigger condition," but the communication mechanism between them is not defined. Options include: a shared message queue (decoupled, async), a direct function call (coupled, sync), or a database flag that the executor polls. The choice affects fault tolerance - if the decision executor is unavailable when the monitor fires a trigger, what happens? This must be resolved before the production architecture is finalised.

#### 3. Y's Directionality is Underspecified
In the original diagram, Y is shown as receiving a call from `engine`, i.e. the engine pushes to Y. But some of the Y candidates (dashboards, analytics engines) are pull-based, not push-based. The engine does not call a dashboard, but rather, the dashboard queries the engine or the event log. The arrow direction implies push where pull would be more accurate for some Y instances. This should be clarified, either by splitting Y into push targets and pull consumers, or by annotating the interaction mode per Y candidate.

> **FLAG: Suggested Resolution**: Split Y explicitly into two categories in the diagram: **push targets** (alerting systems, external APIs that the engine notifies on decision events) and **pull consumers** (dashboards, analytics engines, LangFuse, MLflow - systems that query the event log or ops tables on their own schedule). This also clarifies that the engine's write responsibility is to the event log and ops tables; pull consumers are downstream of those, not of the engine directly.

#### 4. `env` Carries Too Much Semantic Weight
In the simulation, `env` is well-defined: a set of Delta tables with known schemas. In the generalised architecture, `env` implicitly encompasses everything that is not the engine or the agent: the ERP system, the warehouse management system, supplier APIs, stock levels, demand signals, etc. These are heterogeneous systems with different access patterns, authentication models, and consistency guarantees.

The original diagram would benefit from acknowledging that `env` in production is a *composite* of multiple systems, each with its own interface, rather than a single coherent entity. Even a rough decomposition (e.g. `env = {WMS, ERP, supplier APIs}`) would make the architecture more honest about what the engine is actually required to monitor and interact with.

> **FLAG: Tool Abstraction Layer as the Mitigation**: The practical response to `env`'s heterogeneity is a tool abstraction layer - a set of named, versioned functions (Unity Catalog functions in the Databricks context) that each encapsulate one interaction with one component of `env`. The monitor process and decision executor interact with `env` only through these functions, never directly. This isolates the heterogeneity behind a stable interface, which is the same pattern used for the agent's read tools (see `mosaicAiArchitecture-applicationSpecific.md`, Section 2.1). Defining this tool layer is a prerequisite for implementing the production engine, and its scope is determined by the decomposition of `env` into its constituent systems.

#### 5. [IMPORTANT] Agent is Stateless, Engine is Stateful
In the generalised architecture, the engine monitors `env`, assembles context, and executes decisions. The agent receives context and returns decisions. All state (i.e. what the environment looks like, what decisions have been made, what is pending, etc.) lives in the engine or in `env` itself. The agent is a pure function: same context in, same decisions out. This is a reasonable and defensible choice, but it has implications worth naming explicitly:

<details><summary><b>Cross-tick reasoning depends on context assembly</b></summary>
<p>
The agent has no memory of prior ticks unless the engine includes history in the assembled <code>AgentContext</code>. If cross-tick reasoning matters. For example, "last time there was a demand spike I over-ordered and paid excessive holding costs" - the engine must include enough history in the context to support it, or the agent must be given an external memory mechanism.
</p>
</details>

<details><summary><b>Statelessness enables horizontal scaling</b></summary>
<p>
No shared state lives in the agent.

=> Multiple agent instances can run in parallel without coordination.

<i>This is an advantage in production.</i>
</p>
</details>


<details><summary><b>Statelessness limits experiential learning</b></summary>
<p>
Without external memory, the agent cannot accumulate knowledge across runs. This connects directly to the Lakebase discussion in the document <a href="../knowledgeBase/agenticAiWithDatabricks/mosaicAiArchitecture-applicationSpecific.md"><code>knowledgeBase/agenticAiWithDatabricks/mosaicAiArchitecture-applicationSpecific.md</code></a>: if cross-tick or cross-run memory is needed, it must be managed explicitly - either the engine injects it into <code>AgentContext</code>, or the agent maintains it via an external store (e.g. Lakebase).
</p>
</details>

---

Naming this decision explicitly in the plan would:

- Clarify its implications
- Flag where it may need to be revisited as the agent's requirements grow

> **FLAG: Decisions That Cannot Be Deferred**: The stateless/stateful split has two immediate implementation consequences that must be decided before the LLM agent is built, not after. First: what history does the engine include in `AgentContext`, and how many ticks back? This is the `agent_history_window_ticks` parameter already defined in the simulation spec - it must be set and logged as a configurable parameter from the first LLM agent run, because it directly affects both reasoning quality and token cost. Second: if cross-run memory is anticipated (see the Experiential Learning section below), the external store (e.g. Lakebase) must be provisioned and its schema defined before the agent accumulates decisions it cannot later retrieve. Retrofitting a memory store after runs have been discarded is not recoverable.

## Suggestion to Enable Experiential Learning
For an agentic AI system, adaptive learning is a major potential enabled by data-driven high-level reasoning. As stated in the section ["5. [IMPORTANT] Agent is Stateless, Engine is Stateful"](#5-important-agent-is-stateless-engine-is-stateful), the architecture proposed by suggestion 2 makes the agent stateless, leaving state-management to the engine. As discussed in this section, while experiential learning is limited by this architecture, this limitation can be mitigated by the agent keeping its own knowledge-accumulation.

Specifically, since our aim can be boiled down to optimising certain metrics ("objective functions") (as talked about in the section ["Suggestion 1"](#suggestion-1)), we can make the agent keep a record of how its decisions affected these metrics, thereby adding experience to its context in a way that is easier to define and interpret than a wider range of historical data.

***However, this would make both the agent and the engine stateful.***

> **FLAG: This can significantly affect architectural considerations!**

> **FLAG: The Statelessness Contract Is Being Broken - Name It Explicitly**: The preceding section established agent statelessness as a deliberate architectural decision with named benefits (horizontal scaling, predictability) and named costs (no experiential learning). This suggestion revises that decision. Before accepting that revision, the tradeoff must be re-evaluated explicitly: does the gain from experiential learning outweigh the loss of statelessness guarantees? Two architecturally distinct paths preserve the stateless agent while enabling experiential learning, and should be considered before making the agent stateful:
>
> - **Path A - Engine-managed experience injection**: The engine maintains the decision-outcome record and injects a summarised version into `AgentContext` each time. The agent remains a pure function; the engine's state grows, but the engine is already stateful. This is the lower-disruption option.
> - **Path B - External memory store (e.g. Lakebase)**: The agent reads from and writes to an external store at the boundary of `decide()`. The agent is technically stateless between calls but has durable external state. Horizontal scaling is preserved (all instances share the same store), but the store becomes a coordination point and a potential bottleneck.
>
> Making the agent itself stateful (i.e. accumulating state inside the agent object across calls) is a third path, but it is the most disruptive: it breaks horizontal scaling, complicates testing (agent behaviour depends on call history), and is the hardest to reason about. It should be the last resort, not the default.

> **FLAG: Feedback Latency Is Unaddressed**: "A record of how its decisions affected these metrics" assumes that the outcome of a decision is observable promptly. It is not. A reorder placed at tick T arrives at tick T + lead_time; its effect on stockout rate and holding cost is only fully observable several ticks later. The experience record must account for this lag - it cannot attribute the cost at tick T to the decision at tick T. The data structure for the record must include at minimum: `(decision_tick, item_id, decision, order_qty, expected_arrival_tick, observed_outcome_tick, outcome_metrics)`. Without the `expected_arrival_tick` / `observed_outcome_tick` pairing, the record conflates decision time with outcome time and produces misleading experience signals.

> **FLAG: Record Format Determines Reasoning Utility**: "A record of how its decisions affected these metrics" is underspecified as a data structure. The format determines what the LLM can usefully reason over. A flat list of `(tick, decision, total_cost_delta)` tuples gives limited signal. A structured summary - e.g. "in the last 10 disruption events of type `demand_spike`, holding decisions led to stockouts 7 times; reorder decisions led to excess stock 2 times" - gives the agent something it can reason from directly. The record format should be designed alongside the `AgentContext` schema, not left as an implementation detail, because it is part of the agent's reasoning surface.