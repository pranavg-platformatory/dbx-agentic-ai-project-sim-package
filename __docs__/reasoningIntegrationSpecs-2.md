<h1>Reasoning Integration Specifications - 2</h1>

> **Context**: [`reasoningIntegrationSpecs-1.md`](./reasoningIntegrationSpecs-1.md)
>
> This document explores "Suggestion 2" (as proposed in the above-linked document) in greater architectural and practical detail, with the aim being to reach an actionable plan for implementation that enables agentic system development that is both achievable and extensible to production-level environments, as per the considerations made in the above-linked document.

---

**Contents**:

- [Overview](#overview)
  - [Overall Solution Structure](#overall-solution-structure)
  - [Clarifications](#clarifications)
  - [KEY POINT 1: Working toward PROD while in Simulation](#key-point-1-working-toward-prod-while-in-simulation)
  - [KEY POINT 2: Design Approach](#key-point-2-design-approach)
- [Raising Decision-Making Points (DMP)](#raising-decision-making-points-dmp)
  - [DMP 1. Parameterisation Scope](#dmp-1-parameterisation-scope)
  - [DMP 2. Trigger Mode](#dmp-2-trigger-mode)
  - [DMP 3. Resilience Measures](#dmp-3-resilience-measures)
  - [DMP 4. Tool Abstraction Layer](#dmp-4-tool-abstraction-layer)
  - [DMP 5. Making Trigger Condition a Governable Artefact](#dmp-5-making-trigger-condition-a-governable-artefact)
  - [DMP 6. Define push targets and pull consumers](#dmp-6-define-push-targets-and-pull-consumers)
  - [DMP 7. Agent Reasoning Context Enrichment](#dmp-7-agent-reasoning-context-enrichment)
  - [DMP 8. Incorporate Feedback Latency in Design](#dmp-8-incorporate-feedback-latency-in-design)
  - [DMP 9. Inter-Process Communication between Monitoring \& Executor](#dmp-9-inter-process-communication-between-monitoring--executor)
- [Addressing Decision-Making Points (DMP)](#addressing-decision-making-points-dmp)

---

# Overview
## Overall Solution Structure

```
----+<--sc--+-------
LLM |       | engine
----+--sr-->+-------
```

- sc = Structured context
- sr = Structured response

Engine must handle:

- Structurally invalid responses
- Logically invalid responses
- LLM call failures/timeouts

Each case requires a well-defined fallback (retry, rule-based policy, halt with alert, etc.). Defining this is IMPORTANT for the implementation.

## Clarifications
As flagged in ["Tentative Approach", `reasoningIntegation-1.md`](./reasoningIntegration-1.md#tentative-approach), the following statement is too vague to act upon: "Start with suggestion 2, while keeping an eye on suggestion 1." Hence, specifically, we must consider:

1. Make decision trigger condition a configurable parameter from the start
2. Log LLM call frequency and token cost per simulation run in MLflow
3. Define a threshold at which suggestion 1's meta-loop becomes worth implementing <br> E.g.: token cost exceeds X per run, call frequency exceeds Y/hour in PROD, etc.

## KEY POINT 1: Working toward PROD while in Simulation
Instead of the simulation's tick engine enacting the Delta table writes based on decisions, we could define Unity Catalog (UC) functions to do the same, thereby moving the implementation closer to PROD level.

## KEY POINT 2: Design Approach
The focus should be on implementing a simulation-based agent, leaving PROD options open where possible and specifying PROD-related decisions/considerations where necessary.

# Raising Decision-Making Points (DMP)
## DMP 1. Parameterisation Scope
- What can be parameterised for the simulation
- What need not be done for the simulation but must be considered for PROD

## DMP 2. Trigger Mode
In simulation, we must consider what to implement to enable this trigger mode in PROD.

> Choice of trigger mode determines executor design.

## DMP 3. Resilience Measures
To be defined.

## DMP 4. Tool Abstraction Layer
- Unity Catalog function definitions (tools)
- Depends on environment decomposition <br> I.e.: What are the different environment components to interact with

## DMP 5. Making Trigger Condition a Governable Artefact
Preferably through Unity Catalog?

## DMP 6. Define push targets and pull consumers
For downstream use-cases, e.g. dashboard, alerting, tracing, etc.

## DMP 7. Agent Reasoning Context Enrichment
The agent as such is stateless, but we can consider adding a shared state using Lakebase to enrich the reasoning context in a way that can be shared by multiple agents (allowing for horizontal scaling).

## DMP 8. Incorporate Feedback Latency in Design
E.g.: Make sure the reasoning context records account for lag between decision made and the decision's effects.

## DMP 9. Inter-Process Communication between Monitoring & Executor
To ensure decoupling between monitoring and decision-execution processes.

# Addressing Decision-Making Points (DMP)
**DMP 1**:

Parameterise trigger condition. No need to define query-live vs maintained cache view for simulation, since that is only relevant for PROD, and no matter the implementation of this, the outcome has to be the same in quality.

---

> **KEY POINT**: *While a tick shall remain the unit of time, I want to be able to configure when the executor is called with respect to ticks (for now, let monitoring be per tick).*

---

**DMP 2**:

For now, let trigger mode be a job schedule, as it most closely aligns with the simulation's tick-based approach and would thus be easier to implement/demonstrate using the simulation.

---

**DMP 3**:

Resilience measure: Upon timeout/failure, fall back to rule-based agent. Also ensure that the event log's reasoning section identifies this fallback and records the specific error due to LLM call (timeout, failures, bad structure, etc.).

---

**DMP 4**:

UC functions needed for:

- Placing reorder (executor does this)
- Retrieving metrics for evaluation (monitoring setup + context packaging setup does this)

---

**DMP 6**:

Pull consumers:

- MLflow
- Langfuse (IMPORTANT)
- Dashboards

Push targets: None for now.

---

**DMP 7 & 8**:: To be defined/implemented as such. 

---

**DMP 9**:

Message queue (async, decoupled) for executor fault-tolerance and monitoring information/context persistence.