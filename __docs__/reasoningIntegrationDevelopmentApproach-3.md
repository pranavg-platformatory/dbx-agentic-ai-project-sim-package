<h1>Reasoning Integration Development Approach - 3</h1>

<h1>LLMAgentWrapper - Implementation Stages</h1>

> **Context**:
> 
> - [`reasoningIntegrationDevelopmentApproach-1.md`](./reasoningIntegrationDevelopmentApproach-1.md)
> - [`reasoningIntegrationDevelopmentApproach-2.md`](./reasoningIntegrationDevelopmentApproach-2.md)

---

**Contents**:

- [Guiding Principles](#guiding-principles)
- [Stages](#stages)
  - [Stage 1 - `RuleBasedAgent`](#stage-1---rulebasedagent)
  - [Stage 2 - `LLMAgentWrapperConfig`](#stage-2---llmagentwrapperconfig)
  - [Stage 3 - `runner.py` resilience wrap](#stage-3---runnerpy-resilience-wrap)
  - [Stage 4 - `hist_eval_metrics` DDL](#stage-4---hist_eval_metrics-ddl)
  - [Stage 5 - `LLMAgentWrapper`: monitoring loop](#stage-5---llmagentwrapper-monitoring-loop)
  - [Stage 6 - `LLMAgentWrapper`: executor thread and shared result slot](#stage-6---llmagentwrapper-executor-thread-and-shared-result-slot)
  - [Stage 7 - MLflow integration](#stage-7---mlflow-integration)
- [Dependency Graph](#dependency-graph)

---

# Guiding Principles

The stage ordering follows four rules applied in priority order:

- **Dependencies before dependents** - nothing is built before the things it relies on exist
- **Sync before async** - the tick-synchronous monitoring loop is proven before the background executor thread is introduced
- **Tables before writers** - Delta tables are created before the code that writes to them
- **Observability last** - MLflow logging has no effect on simulation correctness and is added only once the machinery it observes is complete

---

# Stages

## Stage 1 - `RuleBasedAgent`

**File**: `warehouse_sim/agent/rule_based.py`

Implement the `RuleBasedAgent(BaseAgent)` that was deferred at simulation Stage 6. Rule: reorder when `stock_on_hand < reorder_point`, quantity = `min_order_qty`.

**Why first**: The `RuleBasedAgent` is a hard dependency of the LLMAgentWrapper's fallback path - both `FALLBACK_STRUCTURAL` and `FALLBACK_LOGICAL` substitute its decisions. Nothing in the LLMAgentWrapper can be meaningfully tested without it. It has no dependencies of its own beyond `agent/base.py`, which already exists, making it the most isolated and immediately buildable piece.

**Test**: run the agent standalone against a hand-constructed `AgentContext`; assert correct reorder/hold decisions and that no new RNG is introduced (reproducibility).

---

## Stage 2 - `LLMAgentWrapperConfig`

**File**: `warehouse_sim/agent/llm_agent_wrapper_config.py`

Define the Pydantic config model with all LLMAgentWrapper-specific parameters: `executor_trigger_every_n_ticks`, `context_obsolescence_threshold_k`, `queue_size`, and stub mode.

**Why here**: Every parameter referenced anywhere in the LLMAgentWrapper must be typed, validated, and defaulted before any LLMAgentWrapper logic is written. Defining config after logic produces implicit parameter contracts that are hard to audit and log. Keeping it before everything else also makes `LLMAgentWrapperConfig` independently testable without any LLMAgentWrapper machinery present.

**Test**: construct valid and invalid configs; assert cross-field validation (e.g. `context_obsolescence_threshold_k > 0`, valid stub mode values).

---

## Stage 3 - `runner.py` resilience wrap

**File**: `warehouse_sim/engine/runner.py`

Add a `try/except` around the `agent.decide(context)` call in `_run_tick`, with a defined fallback (log `AGENT_ERROR` event, substitute hold decisions for all items).

**Why here**: The LLMAgentWrapper must be wired into the runner before it can be tested end-to-end, and the runner currently halts the simulation on any unhandled exception from the agent layer. This wrap must be in place before the LLMAgentWrapper is plugged in - not after - so the simulation is never in a state where an agent exception can cause an unrecoverable halt. This is a narrow, targeted change with no effect on any other runner logic.

**Test**: inject a `BaseAgent` subclass whose `decide()` always raises; assert the simulation continues and an `AGENT_ERROR` event is written.

---

## Stage 4 - `hist_eval_metrics` DDL

**File**: `warehouse_sim/data_store/` (new Delta table definition)

Create the `hist_eval_metrics` append-only Delta table with the schema defined in the implementation approach.

**Why here**: The monitoring loop (Stage 5) writes to this table on every tick. Following the same pattern established throughout the simulation build - tables before writers - the DDL must exist before the writer is implemented. Creating it later would require the Stage 5 implementation to stub or skip writes, obscuring test failures.

**Test**: write a single hand-constructed row; assert schema, append-only property, and that a re-read returns the correct values.

---

## Stage 5 - `LLMAgentWrapper`: monitoring loop

**File**: `warehouse_sim/agent/llm_agent_wrapper.py` (partial)

Implement the sync, tick-bound half of `LLMAgentWrapper.decide()` only:

- Assemble `QueueMessage` from the current `AgentContext`
- Push to the in-process `deque` (respecting `queue_size`)
- Write evaluation metrics to `hist_eval_metrics`
- Return `_last_committed` (hold-all at this stage, since no executor exists yet)

**Why here**: Introducing async complexity (the executor thread) before the monitoring loop is proven independently adds two sources of failure at once. By implementing and validating the sync half first, the queue contents, `hist_eval_metrics` writes, and `QueueMessage` schema are all confirmed correct before the executor ever reads from them.

**Test**: wire the partial LLMAgentWrapper into the runner for a short finite run; assert `QueueMessage` objects are enqueued every tick with correct fields, `hist_eval_metrics` receives one row per tick, and the runner completes without error (returning hold-all decisions throughout).

---

## Stage 6 - `LLMAgentWrapper`: executor thread and shared result slot

**File**: `warehouse_sim/agent/llm_agent_wrapper.py` (complete)

Add the async half:

- Shared result slot (`_result_slot`, `_executor_busy`, `_last_committed`)
- Trigger condition check and executor thread dispatch (queue snapshot on dispatch)
- Executor thread: drain logic, `StubLLMAgent` call, pre-flight validation, fallback routing, `ExecutorResult` write to slot
- Slot consumption on the sync side at the top of each `decide()` call

Implement `StubLLMAgent` with all three modes (`valid`, `structural_fail`, `logical_fail`) as a parameter in `LLMAgentWrapperConfig`.

**Why here**: All dependencies are now in place - `RuleBasedAgent`, `LLMAgentWrapperConfig`, the resilience-wrapped runner, the table, and the proven monitoring loop. Async complexity is introduced only once the sync foundation is solid. The three stub modes provide full code path coverage without requiring a real LLM call.

**Tests**:
- `valid` mode: assert `_last_committed` is updated after the first trigger tick, decisions reach the runner, no fallback events logged
- `structural_fail` mode: assert `FALLBACK_STRUCTURAL` event logged, `RuleBasedAgent` decisions substituted
- `logical_fail` mode: assert `FALLBACK_LOGICAL` event logged, `RuleBasedAgent` decisions substituted
- Executor busy: assert no second dispatch while thread is running
- Obsolescence: set short K and slow trigger; assert stale contexts are discarded
- Reproducibility: two runs with the same seed and `structural_fail` mode produce identical decisions

---

## Stage 7 - MLflow integration

**File**: `warehouse_sim/agent/llm_agent_wrapper.py` or runner entry point

Log the six parameters to MLflow once at run start: `agent_history_window_ticks`, `executor_trigger_every_n_ticks`, `context_obsolescence_threshold_k`, `queue_size`, `agent_version`, `random_seed`.

**Why last**: MLflow logging has no effect on simulation correctness or the behaviour of any other component. Adding it before the machinery is complete would log parameters for a system that is still changing. Done last, it captures the final parameter values for a fully working LLMAgentWrapper run and establishes the baseline for future comparison runs.

**Test**: run the full LLMAgentWrapper in `valid` stub mode; assert all six parameters are present in the MLflow run with correct values.

---

# Dependency Graph

```
agent/base.py  (already exists)
      |
      v
Stage 1   rule_based.py          (no dependencies beyond base.py)
      |
      +----> Stage 2   llm_agent_wrapper_config.py      (no deps dependencies base.py)
      |             |
      |             v
      |       Stage 3   runner.py wrap     (no LLMAgentWrapper dependencies; isolated runner change)
      |             |
      |             v
      |       Stage 4   hist_eval_metrics  (no code dependencies; table only)
      |             |
      |             v
      |       Stage 5   llm_agent_wrapper.py (monitoring loop)
      |             |
      +------------>|
                    v
              Stage 6   llm_agent_wrapper.py (executor + stub)
                    |
                    v
              Stage 7   MLflow logging
```