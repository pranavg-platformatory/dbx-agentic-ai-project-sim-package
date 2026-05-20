<h1>Toy Simulation</h1>

*A simplified test setup for testing that structurally resembles [`warehouse_sim`](../warehouse_sim/).*

---

# Goal
Test LLM agent capabilities in a simulation environment similar to [`warehouse_sim`](../warehouse_sim/).

# Problem Statement
- 1 table with 2 variables and 2 predictions
- Each variable is drawn randomly from a different statistical distribution
- The LLM agent has to to use historical data to predict the next value

---

Other details:

- Agent performance should be scored using mean-squared error (MSE)
- The simulation should output:
    - Event logs (predictions made for the given values at a time step)
    - Prediction vs. actual value graphs

# Key Components of the Solution
- `PredictionContext` + `PredictionDecision` dataclasses mirroring the warehouse pattern
- `LLMPredictionAgent(BaseAgent)` calls Databricks Model Serving <br> ... *via the OpenAI-compatible endpoint `/serving-endpoints/databricks-dbrx-instruct/invocations`*
- The agent:
    - Receives a window of historical values in its context
    - Returns 2 predictions
- Simulation loop:
    - Generates Gaussian samples
    - Feeds history to the agent
    - Scores with MSE
- Results logged to a Delta table + plotted inline <br> ... *all in a single Databricks notebook*

# Directory Structure

```
warehouse_sim/
  agent/
    base.py              # (existing)
    reorder_agent.py     # (existing)
    prediction_context.py   # new - PredictionContext, PredictionDecision
    llm_prediction_agent.py # new - LLMPredictionAgent(BaseAgent)
  simulation/
    prediction_sim.py    # new - simulation loop + MSE scoring
notebooks/
  run_prediction_sim.ipynb  # new - Databricks notebook
```