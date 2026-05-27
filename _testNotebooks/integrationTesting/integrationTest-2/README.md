<h1>Integration Test 2: Continuous Simulation Plan:<br><i>Live Agent Run + Real-Time Dashboard</i></h1>

---

**Contents**:

- [Overview](#overview)
- [Notebook 1: Agent Runner](#notebook-1-agent-runner)
  - [Shared Parameters](#shared-parameters)
  - [Agent Selection](#agent-selection)
  - [Structure](#structure)
- [Notebook 2: Live Dashboard](#notebook-2-live-dashboard)
  - [Shared Parameters](#shared-parameters-1)
  - [The Plot](#the-plot)
  - [Structure](#structure-1)
- [Design Notes](#design-notes)

---

# Overview

Two notebooks, meant to be run simultaneously in separate browser tabs against the same `sim_id`.

**Notebook 1** runs the simulation using `ContinuousRunner` with a configurable agent - either `RuleBasedAgent` or `LLMAgentWrapper` selected by a single parameter at the top. It pages between ticks at a configurable wall-clock rate and prints live console progress.

**Notebook 2** runs an independent polling loop that periodically queries the Delta tables written by Notebook 1 and re-renders a single composite plot in the cell output. It has no awareness of the simulation engine - it only reads tables.

The two notebooks are decoupled by design. Notebook 2 can be started before, during, or after Notebook 1. It renders whatever data exists at each poll interval.

---

# Notebook 1: Agent Runner

**File**: `_testNotebooks/continuousSimulation/continuousSim-agentRunner.py`

## Shared Parameters

Set once at the top. Everything below references these variables.

| Parameter | Type | Description |
|---|---|---|
| `SIM_ID` | string | Unique ID for this run. Notebook 2 must use the same value. |
| `AGENT_TYPE` | `"rule_based"` or `"llm"` | Selects the agent. The only parameter that changes agent behaviour. |
| `RUN_MODE` | `"infinite"` or `"cyclic"` | Passed to `SimConfig`. |
| `TICK_UNIT` | `"hour"`, `"day"`, `"week"` | Cosmetic and passed to `SimConfig`. |
| `TICK_DURATION_SECONDS` | float or None | Wall-clock seconds per tick via `ProgressConfig`. `None` = full speed. Should be long enough for Notebook 2 to poll meaningfully - 2–5s recommended. |
| `PRINT_EVERY_N_TICKS` | int | Console progress frequency. |
| `SIM_SEED` | int | Passed to `PatternSampler` and `SimConfig`. |
| `EXECUTOR_TRIGGER_N` | int | LLM path only: `executor_trigger_every_n_ticks` on `LLMAgentWrapperConfig`. Ignored when `AGENT_TYPE = "rule_based"`. |
| `LLM_AGENT_PACKAGE_PATH` | string | LLM path only: path inserted into `sys.path`. Ignored when `AGENT_TYPE = "rule_based"`. |
| `LLM_AGENT_CONFIG_OVERRIDE` | dict or `{}` | LLM path only: forwarded to `LLMAgentWrapperConfig.llm_agent_config_override`. |
| `CATALOG` | string | `"hackathon_of_the_century"` |

## Agent Selection

The `AGENT_TYPE` parameter drives a single branching cell that instantiates the agent and any required wrapper config. Everything downstream (`ContinuousRunner` construction, `run()`) is identical regardless of which branch was taken.

**`"rule_based"` branch**:
```python
agent = RuleBasedAgent()
```

**`"llm"` branch**:
```python
sys.path.insert(0, LLM_AGENT_PACKAGE_PATH)
llm_config = LLMAgentWrapperConfig(
    executor_trigger_every_n_ticks = EXECUTOR_TRIGGER_N,
    stub_mode                      = None,
    suppress_write_tools           = True,
    llm_agent_config_override      = LLM_AGENT_CONFIG_OVERRIDE or None,
)
agent = LLMAgentWrapper(spark=spark, world=world, config=llm_config, logger=logger)
```

No other cell is aware of which agent was chosen.

## Structure

```
[Cell 1]   Parameters
[Cell 2]   Imports and sys.path setup
[Cell 3]   Clean up prior data for SIM_ID
[Cell 4]   Build and write SimWorld
[Cell 5]   Load world, instantiate logger and sampler
[Cell 6]   Agent selection (AGENT_TYPE branch) → agent
[Cell 7]   Instantiate ContinuousRunner and run()
             ↑ runs until Interrupt; SIM_ENDED written on exit
[Cell 8]   (Optional) Post-run console summary - already printed by ContinuousRunner
```

Cell 7 is the blocking cell. All cells above it are setup; nothing below it runs until Interrupt is pressed.

---

# Notebook 2: Live Dashboard

**File**: `_testNotebooks/continuousSimulation/continuousSim-liveDashboard.py`

## Shared Parameters

| Parameter | Type | Description |
|---|---|---|
| `SIM_ID` | string | Must match Notebook 1. |
| `POLL_INTERVAL_SECONDS` | float | Seconds between table fetches and plot refreshes. 5–10s is a reasonable starting point. |
| `MAX_TICKS_TO_SHOW` | int or None | Rolling window: only the last N ticks are shown. `None` = show all. Keeps the plot readable as the run extends. |
| `CATALOG` | string | `"hackathon_of_the_century"` |

## The Plot

A single figure with four stacked subplots, all sharing the same x-axis (tick number). The figure is cleared and re-rendered on every poll.

---

**Subplot 1 - Stock and demand** *(top, largest)*

Two y-axes:
- Left: `stock_on_hand` per item (solid lines). Source: `ops_warehouse_state`.
- Right: `disrupted_demand` per item (dashed lines). Source: `hist_demand_actuals`.

Reorder events overlaid as vertical markers: a downward triangle at the tick the order was placed, sized by `order_qty`. Source: `hist_reorder_decisions` where `decision = 'reorder'`.

Stockout ticks shaded in translucent red (per item, separately). Source: `event_log` where `event_type = 'STOCKOUT_OCCURRED'`.

---

**Subplot 2 - Fulfilment**

Three lines per item: `fulfilled_demand`, `unmet_demand`, and `raw_demand` (faint). Source: `hist_demand_actuals`. Unmet demand is filled red to the zero axis.

---

**Subplot 3 - Disruptions**

One horizontal bar per active disruption event, showing `effective_magnitude` at each tick it was active. Deterministic disruptions in solid colour; stochastic in hatched. Source: `ops_active_disruptions`.

---

**Subplot 4 - Costs** *(bottom)*

Stacked area chart per item: `holding`, `stockout`, `order`, `transit_loss` cost components per tick. Source: `hist_cost_by_tick`. Cumulative total shown as a step line on a secondary y-axis.

---

**Plot header**: shows `sim_id`, last tick seen, wall-clock time of last refresh, and total ticks polled so far.

## Structure

```
[Cell 1]   Parameters
[Cell 2]   Imports
[Cell 3]   Polling loop (blocking)
             ├─ query all source tables for SIM_ID and MAX_TICKS_TO_SHOW window
             ├─ clear and re-render the four-subplot figure
             ├─ display(fig) - Databricks re-renders in place
             ├─ sleep(POLL_INTERVAL_SECONDS)
             └─ repeat until Interrupt
[Cell 4]   (Optional) Final static render after Interrupt
```

Cell 3 is the blocking cell. The poll loop runs until the user presses Interrupt. It does not know or care whether Notebook 1 is still running - it renders whatever data is in the tables at each poll time.

**Display behaviour**: `matplotlib.pyplot.clf()` + `display(fig)` inside the loop causes Databricks to append a new figure output each poll rather than update in place. To get true in-place update, the loop uses `IPython.display.clear_output(wait=True)` before each `display(fig)` call, which replaces the previous output with the new figure.

---

# Design Notes

**Decoupled by table**: Notebook 2 has no import from `warehouse_sim` and no awareness of the simulation engine. It only reads Delta tables. This means it can be run against any completed or in-progress sim run, not just a live one.

**`TICK_DURATION_SECONDS` drives poll usefulness**: If Notebook 1 runs at full speed, Notebook 2 will see large batches of ticks per poll rather than a live feed. A `TICK_DURATION_SECONDS` of 3–5s gives Notebook 2 a meaningful real-time view with a `POLL_INTERVAL_SECONDS` of 5–10s.

**LLM agent timing**: With `AGENT_TYPE = "llm"`, the executor thread makes LLM calls in the background. The tick loop is not blocked - `TICK_DURATION_SECONDS` still controls pacing. The live dashboard will show hold decisions (from `_last_committed`) for ticks where the LLM has not yet returned a result, and will show LLM-sourced reorder decisions once they are committed and written by the runner. `agent_reasoning` in `hist_reorder_decisions` distinguishes LLM decisions from fallback decisions in the table, though this is not surfaced in the live plot.

**`MAX_TICKS_TO_SHOW` rolling window**: For a long-running infinite simulation, showing all ticks makes the plot unreadable. A rolling window of 50–100 ticks keeps the visual meaningful. The window is applied in the query (`WHERE tick >= MAX(tick) - MAX_TICKS_TO_SHOW`) so only the relevant rows are fetched each poll.

**Interrupt behaviour**: Both notebooks catch `KeyboardInterrupt` cleanly. Notebook 1 writes `SIM_ENDED` via `ContinuousRunner`'s `finally` block. Notebook 2 exits the poll loop and optionally renders a final static figure.

**Shared `SIM_ID` discipline**: The two notebooks must use the same `SIM_ID`. This is the only coupling between them. No shared state, no imports between notebooks, no Spark streaming.