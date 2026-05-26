# Databricks notebook source
# MAGIC %md
# MAGIC # SETUP FOR DATA STORE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog & Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hackathon_of_the_century;
# MAGIC create schema if not exists tables4env;
# MAGIC create schema if not exists tables4ops;
# MAGIC create schema if not exists tables4hist;
# MAGIC create schema if not exists tables4eventlog;

# COMMAND ----------

# MAGIC %md
# MAGIC # Environment Tables (`tables4env`)
# MAGIC
# MAGIC - These tables define the static world configuration
# MAGIC - They are written once before the simulation runs
# MAGIC - They do not change during a run

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_item_types`
# MAGIC
# MAGIC Defines all item types in the simulation.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `supplier_id` | string PK | Unique supplier identifier |
# MAGIC | `supplier_name` | string | Human-readable label |
# MAGIC | `base_lead_time_ticks` | integer | Baseline lead time for orders placed with this supplier |
# MAGIC | `lead_time_variability` | float | Standard deviation of lead time noise |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_item_types (
# MAGIC
# MAGIC  item_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this item type, used as a foreign key across all simulation tables.',
# MAGIC  item_name STRING NOT NULL COMMENT 'Human-readable label for this item type (e.g. "Widget A"). Not used in computation.',
# MAGIC  unit_value DOUBLE NOT NULL COMMENT 'Monetary value of one unit of this item. Used to compute holding cost when no explicit holding_cost_per_unit_per_tick is set.',
# MAGIC  initial_stock INT NOT NULL COMMENT 'Number of units on hand at tick 0. Holding cost accrues on this quantity from tick 0 onward.',
# MAGIC  reorder_point INT NOT NULL COMMENT 'Stock threshold surfaced to the agent as a reference signal. Agent evaluation runs every tick regardless; this is advisory context, not a suppression gate.',
# MAGIC  min_order_qty INT NOT NULL COMMENT 'Minimum order quantity (MOQ). Any reorder placed must be at least this many units. Must be >= 1.',
# MAGIC  max_order_qty INT NOT NULL COMMENT 'Maximum units the agent may order in a single reorder event. Must be >= min_order_qty.',
# MAGIC  holding_cost_per_unit_per_tick DOUBLE NOT NULL COMMENT 'Cost incurred per unit held in the warehouse per tick. Applied to stock_on_hand at end of each tick (post-arrival, post-demand).',
# MAGIC  stockout_cost_per_unit_per_tick DOUBLE NOT NULL COMMENT 'Penalty cost per unit of unmet demand per tick. Applied to unmet_demand after stock is exhausted.',
# MAGIC  order_fixed_cost DOUBLE NOT NULL COMMENT 'Fixed cost charged once per reorder event, regardless of quantity (e.g. processing fee). Added to order cost at placement time.',
# MAGIC  order_variable_cost_per_unit DOUBLE NOT NULL COMMENT 'Variable cost per unit ordered, representing the price paid to the supplier. Multiplied by order_qty at placement time.',
# MAGIC  transit_loss_cost_per_unit DOUBLE NOT NULL COMMENT 'Cost per unit lost in transit during a transit_loss disruption. Applied at the time of arrival when lost_qty > 0.',
# MAGIC
# MAGIC  CONSTRAINT pk_env_item_types PRIMARY KEY (item_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Defines all item types in the simulation. One row per item type. Written once at simulation setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_suppliers`
# MAGIC
# MAGIC Defines all supplier entities.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `supplier_id` | string PK | Unique supplier identifier |
# MAGIC | `supplier_name` | string | Human-readable label |
# MAGIC | `base_lead_time_ticks` | integer | Baseline lead time for orders placed with this supplier |
# MAGIC | `lead_time_variability` | float | Standard deviation of lead time noise |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_suppliers (
# MAGIC
# MAGIC  supplier_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this supplier entity, referenced by env_supplier_item_map and ops_pending_orders.',
# MAGIC  supplier_name STRING NOT NULL COMMENT 'Human-readable label for this supplier (e.g. "Acme Corp"). Not used in computation.',
# MAGIC  base_lead_time_ticks INT NOT NULL COMMENT 'Baseline number of ticks between when an order is placed and when it arrives. Used as the mean of the lead time sampling distribution. Must be >= 1.',
# MAGIC  lead_time_variability DOUBLE NOT NULL COMMENT 'Standard deviation (in ticks) of the lead time noise. Set to 0 for deterministic lead times. Sampled as: max(1, round(Normal(base_lead_time_ticks, lead_time_variability))) before disruption multiplier is applied.',
# MAGIC
# MAGIC  CONSTRAINT pk_env_suppliers PRIMARY KEY (supplier_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Defines all supplier entities in the simulation. One row per supplier. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_supplier_item_map`
# MAGIC
# MAGIC Many-to-many mapping between suppliers and item types.
# MAGIC
# MAGIC **NOTE**: *Constrained to 1 supplier per item type per simulation run (this is conceptual, not enforced).*
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run identifier |
# MAGIC | `supplier_id` | string FK | Supplier |
# MAGIC | `item_id` | string FK | Item type served by this supplier |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_supplier_item_map (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Scopes this mapping to a specific simulation run.',
# MAGIC  supplier_id STRING NOT NULL COMMENT 'Foreign key to env_suppliers.supplier_id. The supplier fulfilling orders for the mapped item.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type supplied by this supplier. Unique per (sim_id, item_id) - each item has exactly one supplier per run.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Maps each item type to its single designated supplier for a given simulation run. Although the underlying schema supports many-to-many, in practice we may have a one-supplier-per-item rule for simpliciity. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_consumers`
# MAGIC
# MAGIC Defines all consumer entities.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `consumer_id` | string PK | Unique consumer identifier |
# MAGIC | `consumer_name` | string | Human-readable label |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_consumers (
# MAGIC
# MAGIC  consumer_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this consumer entity, referenced by env_consumer_item_map and hist_demand_actuals.',
# MAGIC  consumer_name STRING NOT NULL COMMENT 'Human-readable label for this consumer (e.g. "Retail Division"). Not used in computation.',
# MAGIC
# MAGIC  CONSTRAINT pk_env_consumers PRIMARY KEY (consumer_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Defines all consumer entities in the simulation. One row per consumer. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_consumer_item_map`
# MAGIC
# MAGIC Many-to-many mapping between consumers and item types.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run identifier |
# MAGIC | `consumer_id` | string FK | Consumer |
# MAGIC | `item_id` | string FK | Item type demanded by this consumer |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_consumer_item_map (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Scopes this mapping to a specific simulation run.',
# MAGIC  consumer_id STRING NOT NULL COMMENT 'Foreign key to env_consumers.consumer_id. The consumer generating demand for the mapped item.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type demanded by this consumer. Unique per (sim_id, item_id) - each item has exactly one consumer per run.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Maps each item type to its single designated consumer for a given simulation run. Although the underlying schema supports many-to-many, in practice we may have a one-supplier-per-item rule for simpliciity. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_patterns`
# MAGIC
# MAGIC Demand and supply pattern configurations.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `pattern_id` | string PK | Unique pattern identifier |
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `item_id` | string FK | Item type this pattern applies to |
# MAGIC | `role` | enum | `demand` or `supply` |
# MAGIC | `pattern_type` | enum | `statistical` or `custom` |
# MAGIC | `distribution` | string | Distribution name; null if custom |
# MAGIC | `dist_params` | json | Distribution parameters; null if custom |
# MAGIC | `custom_schedule` | array\[float\] | Per-tick values; null if statistical |
# MAGIC | `seasonal_multiplier_schedule` | array\[float\] | Optional seasonal overlay |
# MAGIC | `noise_std` | float | Optional Gaussian noise; 0 if none |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (critical)** - *The `supply` role is present in the schema but the engine behaviour it drives is never defined in the spec. Additionally, this table has no FK to `env_suppliers`, so a `supply`-role pattern cannot be attributed to a specific supplier. Suggest either: (a) removing `supply` from the `role` enum until the mechanism is fully defined, or (b) adding a `supplier_id` FK column (nullable) that is required when `role = supply`.*
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_patterns (
# MAGIC
# MAGIC  pattern_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this pattern record.',
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Scopes this pattern to a specific simulation run.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type this pattern governs.',
# MAGIC  role STRING NOT NULL COMMENT 'Which signal this pattern controls. Allowed values: "demand" (consumer demand drawn each tick), "supply" (supplier capacity or rate - mechanism TBD; see spec section 3.5 before use). Enforced unique per (sim_id, item_id, role).',
# MAGIC  pattern_type STRING NOT NULL COMMENT 'How the signal is generated. Allowed values: "statistical" (drawn from a named distribution each tick), "custom" (read from a fixed schedule, cycled if shorter than num_ticks).',
# MAGIC  distribution STRING COMMENT 'Name of the statistical distribution to sample from. Required when pattern_type = "statistical". Allowed values: "poisson", "normal", "uniform", "negative_binomial", "zero_inflated_poisson". NULL when pattern_type = "custom".',
# MAGIC  dist_params STRING COMMENT 'JSON object of distribution-specific parameters (e.g. {"mu": 50} for Poisson). Required when pattern_type = "statistical". NULL when pattern_type = "custom". Stored as STRING; parse as JSON at read time.',
# MAGIC  custom_schedule ARRAY<DOUBLE> COMMENT 'Ordered list of per-tick float values forming a fixed schedule. Required when pattern_type = "custom". Cycled from index 0 if shorter than num_ticks. NULL when pattern_type = "statistical".',
# MAGIC  seasonal_multiplier_schedule ARRAY<DOUBLE> COMMENT 'Optional per-tick multiplier applied on top of the base pattern output (e.g. [1.0, 1.2, 0.8, ...] for weekly seasonality). NULL means no seasonal overlay is applied.',
# MAGIC  noise_std DOUBLE NOT NULL COMMENT 'Standard deviation of optional Gaussian noise added after the base pattern and seasonal multiplier are evaluated. 0 = no noise. The noisy float sample is floored at 0 then converted to integer via floor() before use.',
# MAGIC  supplier_id STRING COMMENT 'Foreign key to env_suppliers.supplier_id. Required when role = "supply" to associate this capacity pattern with a specific supplier. NULL when role = "demand".',
# MAGIC
# MAGIC  CONSTRAINT pk_env_patterns PRIMARY KEY (pattern_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Stores demand and supply pattern configurations for each item type in a simulation run. One row per (sim_id, item_id, role). Pattern type and parameters are swappable without engine changes (NFR-02). Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_disruption_schedule`
# MAGIC
# MAGIC Pre-defined disruption events for a simulation run.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `disruption_id` | string PK | Unique disruption identifier |
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `item_id` | string FK | Affected item type |
# MAGIC | `disruption_type` | enum | `demand_spike`, `demand_suppression`, `transit_delay`, `transit_loss` |
# MAGIC | `start_tick` | integer | First tick of effect |
# MAGIC | `end_tick` | integer | Last tick of effect (inclusive) |
# MAGIC | `magnitude` | float | Effect multiplier or fraction |
# MAGIC | `is_stochastic` | boolean | Whether activation is probabilistic per tick |
# MAGIC | `trigger_probability` | float | Per-tick activation probability; null if not stochastic |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_disruption_schedule (
# MAGIC
# MAGIC  disruption_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this disruption record, referenced by ops_active_disruptions and the event log.',
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Scopes this disruption to a specific simulation run.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type affected by this disruption.',
# MAGIC  disruption_type STRING NOT NULL COMMENT 'Category of disruption. Allowed values: "demand_spike" (multiplies drawn demand upward), "demand_suppression" (multiplies drawn demand downward), "transit_delay" (multiplies effective lead time), "transit_loss" (destroys a fraction of an arriving order).',
# MAGIC  start_tick INT NOT NULL COMMENT 'First simulation tick at which this disruption is active (inclusive). For stochastic disruptions, the trigger_probability check begins from this tick.',
# MAGIC  end_tick INT NOT NULL COMMENT 'Last simulation tick at which this disruption is active (inclusive). Must be >= start_tick.',
# MAGIC  magnitude DOUBLE NOT NULL COMMENT 'Effect strength. Interpretation by disruption_type: "demand_spike" and "demand_suppression" - demand multiplier, must be > 0; "transit_delay" - lead time multiplier, must be > 0; "transit_loss" - fraction of in-transit units destroyed on arrival, must be in [0.0, 1.0].',
# MAGIC  is_stochastic BOOLEAN NOT NULL COMMENT 'If TRUE, the disruption is evaluated probabilistically each tick within [start_tick, end_tick] using trigger_probability. If FALSE, it is deterministically active for the entire window.',
# MAGIC  trigger_probability DOUBLE COMMENT 'Per-tick probability (in [0.0, 1.0]) that a stochastic disruption activates for that tick. Required when is_stochastic = TRUE; NULL when is_stochastic = FALSE. The RNG draw occurs at sub-step (0) of each tick before any other events, in disruption_id alphabetical order to ensure reproducibility.',
# MAGIC
# MAGIC  CONSTRAINT pk_env_disruption_schedule PRIMARY KEY (disruption_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pre-defined disruption events for a simulation run. Each row describes a disruption window and its effect on demand, lead time, or transit. Stochastic disruptions use trigger_probability to activate per-tick within the window. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## `env_sim_config`
# MAGIC
# MAGIC Top-level simulation run configuration.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string PK | Simulation run identifier |
# MAGIC | `random_seed` | integer | Global RNG seed |
# MAGIC | `num_ticks` | integer | Total ticks to simulate; null = infinite |
# MAGIC | `tick_unit` | enum | `hour`, `day`, `week` |
# MAGIC | `budget_limit` | float | Total budget cap; null = unlimited |
# MAGIC | `agent_history_window_ticks` | integer | (Added as per suggestion) Number of historical demand ticks exposed to the agent via hist_demand_actuals. NULL = agent receives full history from tick 0. Must be >= 1 when set |
# MAGIC | `budget_warning_threshold` | integer | (Added as per suggestion) Fraction of budget_limit below which a BUDGET_WARNING event fires (e.g. 0.10 = warn at < 10% remaining). Default 0.10. Fires once per threshold crossing, not every tick. Ignored when budget_limit is NULL |
# MAGIC | `start_timestamp` | datetime | Wall-clock anchor for tick 0 |
# MAGIC | `created_at` | datetime | When this config was registered |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (important)** - *Add `agent_history_window_ticks` (integer, null = unlimited) and `budget_warning_threshold` (float, default 0.10) to this table to match the parameter additions suggested in section 3.1. These suggestions are given below...*
# MAGIC
# MAGIC > **⚠️ SUGGESTION (important)** - *Add `agent_history_window_ticks` (integer, null = unlimited) as a simulation-level parameter controlling how many ticks of demand history are exposed to the agent (see FR-04 note above).*
# MAGIC
# MAGIC > **⚠️ SUGGESTION (minor)** - *Add `budget_warning_threshold` (float, default 0.10) as a simulation-level parameter. The 10% budget warning threshold is currently hardcoded in the `BUDGET_WARNING` event definition. Making it configurable avoids a hidden magic number.*
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4env.env_sim_config (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Primary key. Unique identifier for this simulation run. Referenced as a foreign key by every other table in the schema.',
# MAGIC  random_seed BIGINT NOT NULL COMMENT 'Global RNG seed initialising the single shared random number generator for the run. All stochastic draws - disruption activation, demand sampling, lead time sampling - use this RNG in a fixed per-tick order to guarantee full reproducibility (FR-07).',
# MAGIC  num_ticks INT COMMENT 'Total number of ticks to simulate. NULL means the simulation runs indefinitely per run_mode. Must be >= 1 when set.',
# MAGIC  run_mode STRING NOT NULL COMMENT 'Termination and cycling behaviour. Allowed values: "finite" (stop after num_ticks), "infinite" (run forever, no cycle), "cyclic" (run forever, cycling world state).',
# MAGIC  tick_unit STRING NOT NULL COMMENT 'Real-world duration represented by one simulation tick. Allowed values: "hour", "day", "week". Used for labelling and wall-clock alignment only; does not affect engine logic.',
# MAGIC  budget_limit DOUBLE COMMENT 'Maximum total spend across all reorders and all items for the entire run. A reorder may only be placed if its cost does not exceed remaining_budget. NULL = no budget constraint.',
# MAGIC  budget_warning_threshold DOUBLE NOT NULL COMMENT 'Fraction of budget_limit below which a BUDGET_WARNING event fires (e.g. 0.10 = warn at < 10% remaining). Default 0.10. Fires once per threshold crossing, not every tick. Ignored when budget_limit is NULL.',
# MAGIC  agent_history_window_ticks INT COMMENT 'Number of historical demand ticks exposed to the agent via hist_demand_actuals. NULL = agent receives full history from tick 0. Must be >= 1 when set.',
# MAGIC  start_timestamp TIMESTAMP NOT NULL COMMENT 'Wall-clock datetime corresponding to tick 0. Used to map simulation ticks to real-world calendar dates for reporting purposes.',
# MAGIC  created_at TIMESTAMP NOT NULL COMMENT 'Wall-clock datetime at which this simulation config was registered. Set by the engine at config write time.',
# MAGIC
# MAGIC  CONSTRAINT pk_env_sim_config PRIMARY KEY (sim_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Top-level simulation run configuration. One row per simulation run. Acts as the root record referenced by every other table. Written once at setup; immutable during a run.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'environment'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Operational Tables (`tables4ops`)
# MAGIC
# MAGIC - These tables hold live state during the simulation run
# MAGIC - They are read and written by the simulation engine each tick

# COMMAND ----------

# MAGIC %md
# MAGIC ## `ops_warehouse_state`
# MAGIC
# MAGIC Current stock level per item type, updated at the end of each tick.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Current tick |
# MAGIC | `item_id` | string FK | Item type |
# MAGIC | `stock_on_hand` | integer | Units currently in warehouse |
# MAGIC | `stock_in_transit` | integer | Units on order, not yet arrived |
# MAGIC | `expected_arrivals_next_tick` | integer | Units due to arrive next tick |
# MAGIC | `updated_at` | datetime | Timestamp of last write |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (critical)** - *The write semantics of this table are ambiguous. "Updated at the end of each tick" implies upsert (one live row per `(sim_id, item_id)`), but the presence of a `tick` column implies append (one row per `(sim_id, tick, item_id)`). These require different query patterns to read current state. Suggest stating explicitly: this table is **append-only**, with one row per `(sim_id, tick, item_id)`. Current state is always the row with the maximum `tick`. If upsert semantics are preferred instead, the `tick` column becomes an audit field and a separate query convention must be documented.*
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4ops.ops_warehouse_state (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'The simulation tick this row represents. One row appended per item per tick. To read current state, query MAX(tick) for a given (sim_id, item_id).',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type whose stock this row describes.',
# MAGIC  stock_on_hand INT NOT NULL COMMENT 'Units physically in the warehouse at the end of this tick, after supply arrivals (step 3a) and demand depletion (step 3b) have both been applied. Floored at 0 - never negative. This is the value the agent reads in step (4).',
# MAGIC  stock_in_transit INT NOT NULL COMMENT 'Total units across all pending (undelivered) orders for this item at the end of this tick. Sum of order_qty for all ops_pending_orders rows with status = "pending" for this (sim_id, item_id).',
# MAGIC  expected_arrivals_next_tick INT NOT NULL COMMENT 'Units due to arrive at the start of the next tick (orders with expected_arrival_tick = current tick + 1). Surfaced to the agent as forward-looking context.',
# MAGIC  updated_at TIMESTAMP NOT NULL COMMENT 'Wall-clock timestamp at which this row was written by the simulation engine.',
# MAGIC
# MAGIC  CONSTRAINT pk_ops_warehouse_state PRIMARY KEY (sim_id, tick, item_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only record of warehouse stock levels per item per tick. One row is inserted at the end of every tick for every item. Current live state = MAX(tick) per (sim_id, item_id). Used by the agent (FR-04) and the observability layer.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'operational'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `ops_pending_orders`
# MAGIC
# MAGIC Reorders that have been placed but not yet received.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `order_id` | string PK | Unique order identifier |
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `item_id` | string FK | Item type ordered |
# MAGIC | `supplier_id` | string FK | Supplier fulfilling the order |
# MAGIC | `order_tick` | integer | Tick at which the order was placed |
# MAGIC | `expected_arrival_tick` | integer | Tick at which goods are expected to arrive |
# MAGIC | `order_qty` | integer | Units ordered |
# MAGIC | `status` | enum | `pending`, `arrived`, `partially_lost`, `fully_lost` |
# MAGIC | `disruptions_active_at_order` | array[string] | Disruption IDs active when the order was placed |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (important)** - *`fully_lost` has been added to the `status` enum here to handle the case where `transit_loss` magnitude = 1.0 destroys the entire shipment. If a maximum magnitude cap below 1.0 is enforced instead, remove this value.*
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4ops.ops_pending_orders (
# MAGIC
# MAGIC  order_id STRING NOT NULL COMMENT 'Primary key. Unique UUID for this reorder event. Referenced by hist_supply_arrivals, hist_reorder_decisions, and the event log.',
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type being ordered.',
# MAGIC  supplier_id STRING NOT NULL COMMENT 'Foreign key to env_suppliers.supplier_id. The supplier fulfilling this order.',
# MAGIC  order_tick INT NOT NULL COMMENT 'Simulation tick at which the agent placed this order (step 4 of that tick).',
# MAGIC  expected_arrival_tick INT NOT NULL COMMENT 'Simulation tick at which this order is expected to arrive. Computed as: order_tick + max(1, round(Normal(base_lead_time_ticks, lead_time_variability))) × max(1.0, disruption_multiplier).',
# MAGIC  order_qty INT NOT NULL COMMENT 'Number of units ordered. Must satisfy min_order_qty <= order_qty <= max_order_qty for the item. Must be a positive integer.',
# MAGIC  status STRING NOT NULL COMMENT 'Lifecycle status of this order. Allowed values: "pending" (placed, not yet arrived), "arrived" (received with no or partial transit loss and arrived_qty > 0), "partially_lost" (arrived with 0 < lost_qty < ordered_qty), "fully_lost" (entire shipment destroyed, arrived_qty = 0; occurs when transit_loss magnitude = 1.0).',
# MAGIC  disruptions_active_at_order ARRAY<STRING> COMMENT 'Array of disruption_id values that were active (is_active_this_tick = TRUE) when this order was placed. Used for audit and causal analysis. NULL or empty when no disruptions were active.',
# MAGIC
# MAGIC  CONSTRAINT pk_ops_pending_orders PRIMARY KEY (order_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Records every reorder placed by the agent. Rows are inserted at order placement; the status column is updated on arrival. Pending orders are queried by the agent each tick to compute stock_in_transit and expected_arrivals_next_tick.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'false',
# MAGIC  'simulation.layer' = 'operational'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `ops_cost_accumulator`
# MAGIC
# MAGIC Running cost totals per item type, updated each tick.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Current tick |
# MAGIC | `item_id` | string FK | Item type |
# MAGIC | `cumulative_holding_cost` | float | Total holding cost accrued so far |
# MAGIC | `cumulative_stockout_cost` | float | Total stockout penalty accrued so far |
# MAGIC | `cumulative_order_cost` | float | Total order cost (fixed + variable) accrued so far |
# MAGIC | `cumulative_transit_loss_cost` | float | Total transit loss cost accrued so far |
# MAGIC | `cumulative_total_cost` | float | Sum of all cost components |
# MAGIC | `remaining_budget` | float | Budget remaining for the run; null if unlimited |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (critical)** - *Same append-vs-upsert ambiguity as `ops_warehouse_state`. Suggest the same resolution: state the write semantics explicitly and document the query convention for reading current totals.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4ops.ops_cost_accumulator (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'The simulation tick this row represents. Append-only: one row per (sim_id, tick, item_id). Current totals = MAX(tick) per (sim_id, item_id).',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type whose costs this row accumulates.',
# MAGIC  cumulative_holding_cost DOUBLE NOT NULL COMMENT 'Running total of holding costs accrued for this item from tick 0 through this tick. Per-tick holding cost = stock_on_hand (end of tick) × holding_cost_per_unit_per_tick.',
# MAGIC  cumulative_stockout_cost DOUBLE NOT NULL COMMENT 'Running total of stockout penalties accrued for this item from tick 0 through this tick. Per-tick stockout cost = unmet_demand × stockout_cost_per_unit_per_tick.',
# MAGIC  cumulative_order_cost DOUBLE NOT NULL COMMENT 'Running total of order costs (fixed + variable) accrued for this item from tick 0 through this tick. Per-order cost = order_fixed_cost + (order_qty × order_variable_cost_per_unit), charged at placement.',
# MAGIC  cumulative_transit_loss_cost DOUBLE NOT NULL COMMENT 'Running total of transit loss costs accrued for this item from tick 0 through this tick. Per-arrival cost = lost_qty × transit_loss_cost_per_unit, charged at arrival.',
# MAGIC  cumulative_total_cost DOUBLE NOT NULL COMMENT 'Sum of all four cumulative cost components: cumulative_holding_cost + cumulative_stockout_cost + cumulative_order_cost + cumulative_transit_loss_cost.',
# MAGIC  remaining_budget DOUBLE COMMENT 'Global budget remaining for the entire run (across all items) as of this tick. NULL when budget_limit is NULL. Shared across all items - not a per-item figure.',
# MAGIC
# MAGIC  CONSTRAINT pk_ops_cost_accumulator PRIMARY KEY (sim_id, tick, item_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only running cost totals per item per tick. One row inserted per item at the end of every tick. Current totals = MAX(tick) per (sim_id, item_id). Exposed read-only to the agent (FR-05) and the observability layer.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'operational'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `ops_active_disruptions`
# MAGIC
# MAGIC Disruptions currently in effect at the current tick.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Current tick |
# MAGIC | `disruption_id` | string FK | Disruption record |
# MAGIC | `item_id` | string FK | Affected item type |
# MAGIC | `disruption_type` | enum | Type of disruption |
# MAGIC | `effective_magnitude` | float | Actual magnitude applied this tick (may differ from scheduled if stochastic) |
# MAGIC | `is_active_this_tick` | boolean | Whether stochastic disruption triggered this tick |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4ops.ops_active_disruptions (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'The simulation tick this row describes. All disruptions within their [start_tick, end_tick] window appear here every tick - use is_active_this_tick to filter to those that had a real effect.',
# MAGIC  disruption_id STRING NOT NULL COMMENT 'Foreign key to env_disruption_schedule.disruption_id. The disruption record this row tracks.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. Denormalised from env_disruption_schedule for query convenience.',
# MAGIC  disruption_type STRING NOT NULL COMMENT 'Denormalised from env_disruption_schedule.disruption_type. Allowed values: "demand_spike", "demand_suppression", "transit_delay", "transit_loss".',
# MAGIC  effective_magnitude DOUBLE NOT NULL COMMENT 'The magnitude actually applied this tick. For deterministic disruptions: equals env_disruption_schedule.magnitude. For stochastic disruptions that triggered: equals magnitude. For stochastic disruptions that did not trigger: 0.0 (no effect applied).',
# MAGIC  is_active_this_tick BOOLEAN NOT NULL COMMENT 'TRUE if this disruption had an effect this tick. Always TRUE for deterministic disruptions within their window. For stochastic disruptions, reflects the outcome of the trigger_probability RNG draw made at sub-step (0) of this tick.',
# MAGIC
# MAGIC  CONSTRAINT pk_ops_active_disruptions PRIMARY KEY (sim_id, tick, disruption_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Records the activation state of every in-window disruption for every tick. Written at sub-step (0) before other tick events. Exposed to the agent (FR-04) as the active disruption context view.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'operational'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC # Historical Tables (`tables4hist`)
# MAGIC
# MAGIC - These are append-only summaries used for agent context and observability
# MAGIC - They accumulate across ticks
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hist_demand_actuals`
# MAGIC
# MAGIC Realised demand drawn each tick, before stockout clipping.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick |
# MAGIC | `item_id` | string FK | Item type |
# MAGIC | `consumer_id` | string FK | Consumer that generated the demand |
# MAGIC | `raw_demand` | float | Demand sampled from pattern before disruption |
# MAGIC | `disrupted_demand` | float | Demand after disruption multipliers applied |
# MAGIC | `fulfilled_demand` | integer | Units actually issued from stock |
# MAGIC | `unmet_demand` | integer | Units of demand not fulfilled (stockout volume) |
# MAGIC | `pattern_id` | string FK | Pattern used to generate this demand |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (minor)** - *`raw_demand` is typed as float (the raw sample) while `fulfilled_demand` and `unmet_demand` are integer. The rounding/floor step that converts the float sample to an integer is not documented here or in section 3.5. Suggest adding a note: `disrupted_demand` is the float value after multipliers; `fulfilled_demand = min(floor(disrupted_demand), stock_on_hand)`; `unmet_demand = floor(disrupted_demand) - fulfilled_demand`.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4hist.hist_demand_actuals (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'Simulation tick at which this demand was drawn (step 2 of the tick sequence).',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type for which demand was drawn.',
# MAGIC  consumer_id STRING NOT NULL COMMENT 'Foreign key to env_consumers.consumer_id. The consumer entity that generated this demand.',
# MAGIC  raw_demand DOUBLE NOT NULL COMMENT 'Float value sampled from the demand pattern (including any noise_std noise) before any disruption multipliers are applied.',
# MAGIC  disrupted_demand DOUBLE NOT NULL COMMENT 'Float value after all active demand disruption multipliers (demand_spike, demand_suppression) have been applied to raw_demand. Equal to raw_demand when no demand disruptions are active this tick.',
# MAGIC  fulfilled_demand INT NOT NULL COMMENT 'Integer units actually issued from stock. Computed as min(floor(disrupted_demand), stock_on_hand). Never negative.',
# MAGIC  unmet_demand INT NOT NULL COMMENT 'Integer units of demand not fulfilled due to insufficient stock (stockout). Computed as floor(disrupted_demand) - fulfilled_demand. 0 when stock is sufficient.',
# MAGIC  pattern_id STRING NOT NULL COMMENT 'Foreign key to env_patterns.pattern_id. The demand pattern used to generate raw_demand this tick.',
# MAGIC
# MAGIC  CONSTRAINT pk_hist_demand_actuals PRIMARY KEY (sim_id, tick, item_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only record of realised demand drawn each tick per item, before and after disruptions and stockout clipping. The agent receives the last agent_history_window_ticks rows per item (or all rows if the window is NULL) as demand history context (FR-04).'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'historical'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hist_supply_arrivals`
# MAGIC
# MAGIC Record of every order arrival.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick of arrival |
# MAGIC | `order_id` | string FK | Order that arrived |
# MAGIC | `item_id` | string FK | Item type |
# MAGIC | `supplier_id` | string FK | Supplier |
# MAGIC | `ordered_qty` | integer | Original order quantity |
# MAGIC | `arrived_qty` | integer | Units that actually arrived (after transit loss) |
# MAGIC | `lost_qty` | integer | Units lost in transit (0 if no disruption) |
# MAGIC | `actual_lead_time_ticks` | integer | Ticks between order placement and this arrival |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4hist.hist_supply_arrivals (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'Simulation tick at which this order arrived (step 1 of the tick sequence).',
# MAGIC  order_id STRING NOT NULL COMMENT 'Foreign key to ops_pending_orders.order_id. The order that arrived this tick.',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type received.',
# MAGIC  supplier_id STRING NOT NULL COMMENT 'Foreign key to env_suppliers.supplier_id. The supplier that fulfilled this order.',
# MAGIC  ordered_qty INT NOT NULL COMMENT 'Original number of units ordered at placement time, before any transit loss.',
# MAGIC  arrived_qty INT NOT NULL COMMENT 'Number of units that actually entered warehouse stock after transit loss was applied. Equal to ordered_qty when no transit_loss disruption was active at arrival.',
# MAGIC  lost_qty INT NOT NULL COMMENT 'Number of units destroyed in transit by an active transit_loss disruption. 0 when no transit loss occurred. Computed as ordered_qty - arrived_qty.',
# MAGIC  actual_lead_time_ticks INT NOT NULL COMMENT 'Actual number of ticks between order placement (ops_pending_orders.order_tick) and this arrival tick. May differ from (expected_arrival_tick - order_tick) due to stochastic lead time sampling.',
# MAGIC
# MAGIC  CONSTRAINT pk_hist_supply_arrivals PRIMARY KEY (sim_id, tick, order_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only record of every order arrival. Written at step 1 of the arrival tick. Captures ordered vs. actually received quantities, making transit loss fully observable.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'historical'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hist_reorder_decisions`
# MAGIC
# MAGIC Every reorder decision made by the agent, including decisions not to reorder.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick of decision |
# MAGIC | `item_id` | string FK | Item type evaluated |
# MAGIC | `supplier_id` | string FK | (Added as per suggestion) Supplier for the item type |
# MAGIC | `stock_on_hand_at_decision` | integer | Stock level seen by agent |
# MAGIC | `stock_in_transit_at_decision` | integer | In-transit units seen by agent |
# MAGIC | `decision` | enum | `reorder` or `hold` |
# MAGIC | `order_qty` | integer | Units ordered; 0 if hold |
# MAGIC | `order_id` | string FK | Created order ID; null if hold |
# MAGIC | `agent_reasoning` | string | Free-text or structured reasoning from agent (for LLM agents) |
# MAGIC | `agent_version` | string | Version/identifier of the agent policy used |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (minor)** - *This table records decisions per item but not per supplier. If the 1-supplier-per-item constraint is ever relaxed, decisions become ambiguous. Suggest adding `supplier_id` (string FK, nullable) as a forward-compatibility column.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4hist.hist_reorder_decisions (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'Simulation tick at which the agent made this decision (step 4 of the tick sequence).',
# MAGIC  item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type evaluated by the agent.',
# MAGIC  supplier_id STRING COMMENT 'Foreign key to env_suppliers.supplier_id. The supplier mapped to this item for this run. Included for forward compatibility in case the one-supplier-per-item constraint is relaxed in future.',
# MAGIC  stock_on_hand_at_decision INT NOT NULL COMMENT 'The stock_on_hand value from ops_warehouse_state as seen by the agent at decision time - end of step 3, reflecting both arrivals and demand depletion.',
# MAGIC  stock_in_transit_at_decision INT NOT NULL COMMENT 'The stock_in_transit value from ops_warehouse_state as seen by the agent at decision time.',
# MAGIC  decision STRING NOT NULL COMMENT 'The agent outcome for this item this tick. Allowed values: "reorder" (an order was placed), "hold" (no order was placed).',
# MAGIC  order_qty INT NOT NULL COMMENT 'Number of units ordered. 0 when decision = "hold". Must satisfy min_order_qty <= order_qty <= max_order_qty when decision = "reorder".',
# MAGIC  order_id STRING COMMENT 'Foreign key to ops_pending_orders.order_id. The order created by this decision. NULL when decision = "hold".',
# MAGIC  agent_reasoning STRING COMMENT 'Free-text or structured reasoning produced by the agent. Populated by LLM-based agents for observability and audit; may be NULL for rule-based agents.',
# MAGIC  agent_version STRING NOT NULL COMMENT 'Version or identifier string of the agent policy that produced this decision. Used for reproducibility and A/B comparison across agent versions.',
# MAGIC
# MAGIC  CONSTRAINT pk_hist_reorder_decisions PRIMARY KEY (sim_id, tick, item_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only record of every agent reorder decision, including hold decisions. Written at step 4 of each tick for every item the agent evaluates. Captures the full agent context (stock, in-transit, reasoning) at decision time for audit and analysis.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'historical'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hist_cost_by_tick`
# MAGIC
# MAGIC Snapshot of cost components accrued in each individual tick (not cumulative).
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick |
# MAGIC | `item_id` | string FK | Item type |
# MAGIC | `holding_cost` | float | Holding cost this tick |
# MAGIC | `stockout_cost` | float | Stockout cost this tick |
# MAGIC | `order_cost` | float | Order cost this tick (0 if no order placed) |
# MAGIC | `transit_loss_cost` | float | Transit loss cost this tick |
# MAGIC | `total_cost` | float | Sum of above |

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4hist.hist_cost_by_tick (
# MAGIC  
# MAGIC   sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC   tick INT NOT NULL COMMENT 'Simulation tick for which these costs were incurred.',
# MAGIC   item_id STRING NOT NULL COMMENT 'Foreign key to env_item_types.item_id. The item type whose per-tick costs this row records.',
# MAGIC   holding_cost DOUBLE NOT NULL COMMENT 'Holding cost incurred this tick. Computed as stock_on_hand (end of tick, post-arrival and post-demand) × holding_cost_per_unit_per_tick. Non-zero from tick 0 (accrues on initial_stock).',
# MAGIC   stockout_cost DOUBLE NOT NULL COMMENT 'Stockout penalty incurred this tick. Computed as unmet_demand × stockout_cost_per_unit_per_tick. 0 when demand is fully fulfilled.',
# MAGIC   order_cost DOUBLE NOT NULL COMMENT 'Total order cost (fixed + variable) charged this tick. 0 if no order was placed. Computed at placement as order_fixed_cost + (order_qty × order_variable_cost_per_unit).',
# MAGIC   transit_loss_cost DOUBLE NOT NULL COMMENT 'Transit loss cost charged this tick. 0 if no order arrived with lost units. Computed at arrival as lost_qty × transit_loss_cost_per_unit.',
# MAGIC   total_cost DOUBLE NOT NULL COMMENT 'Sum of all four cost components for this item this tick: holding_cost + stockout_cost + order_cost + transit_loss_cost.',
# MAGIC  
# MAGIC   CONSTRAINT pk_hist_cost_by_tick PRIMARY KEY (sim_id, tick, item_id)
# MAGIC  )
# MAGIC  USING DELTA
# MAGIC  COMMENT 'Append-only record of cost components incurred per item per tick (not cumulative). Complements ops_cost_accumulator, which holds running totals. Use this table for per-tick cost analysis, trend charts, and debugging cost spikes.'
# MAGIC  TBLPROPERTIES (
# MAGIC   'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'historical'
# MAGIC  )
# MAGIC  PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## `hist_eval_metrics`
# MAGIC
# MAGIC > **Added during LLM agent development (LLMAgentWrapper - Stage 4)**:
# MAGIC > 
# MAGIC > - Written by the LLMAgentWrapper monitoring loop on every tick
# MAGIC > - Pull consumers (LangFuse, MLflow, dashboards) read from this table on their own schedule <br> *The monitoring loop has no direct dependency on any of them*
# MAGIC > - Evaluation metrics are also queryable by the reasoning system via UC read functions over this table, consistent with the tool abstraction layer <br> See "DMP 4. Tool Abstraction Layer" from `__docs__/reasoningIntegrationSpecs-2.md`
# MAGIC
# MAGIC **NOTE: Narrow/tall table schema: one row per metric per tick (per item where applicable)**: This keeps the schema stable as new metrics are added - a new metric means a new row, not a column alter. `item_id` is nullable: NULL means the metric is run-level; a populated value means it is item-level.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick at which the metric was evaluated |
# MAGIC | `item_id` | string FK, nullable | Item the metric applies to; NULL for run-level metrics |
# MAGIC | `metric_name` | string | Metric identifier (e.g. `stockout_rate`, `holding_cost_delta`) |
# MAGIC | `metric_value` | float | Computed metric value |
# MAGIC | `logged_at` | timestamp | Wall-clock time the row was written |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hackathon_of_the_century.tables4hist.hist_eval_metrics (
# MAGIC
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run.',
# MAGIC  tick INT NOT NULL COMMENT 'Simulation tick at which this metric was evaluated by the LLMAgentWrapper monitoring loop.',
# MAGIC  item_id STRING COMMENT 'Foreign key to env_item_types.item_id. NULL for run-level metrics; populated for item-level metrics.',
# MAGIC  metric_name STRING NOT NULL COMMENT 'Metric identifier. Examples: stockout_rate, holding_cost_delta, unmet_demand_pct. One row per metric per tick; add new metrics as new rows without altering this schema.',
# MAGIC  metric_value DOUBLE NOT NULL COMMENT 'Computed value of the metric for this tick and item (or run level if item_id is NULL). All metrics are numeric; non-numeric evaluation outputs warrant a separate design decision.',
# MAGIC  logged_at TIMESTAMP NOT NULL COMMENT 'Wall-clock timestamp at which this row was written by the LLMAgentWrapper monitoring loop. Not a simulation time - use tick for simulation-time ordering.',
# MAGIC
# MAGIC  CONSTRAINT pk_hist_eval_metrics PRIMARY KEY (sim_id, tick, item_id, metric_name)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Append-only record of evaluation metrics computed by the LLMAgentWrapper monitoring loop, one row per metric per tick. Pull consumers (LangFuse, MLflow, dashboards) read from this table downstream - the monitoring loop writes here and nowhere else. item_id is nullable: NULL indicates a run-level metric; a populated value indicates an item-level metric.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'historical'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);

# COMMAND ----------

# MAGIC %md
# MAGIC # Event Log (`tables4eventlog`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `event_log`
# MAGIC
# MAGIC Single unified event stream; all event types write here.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `event_id` | string PK | Unique event identifier (UUID) |
# MAGIC | `sim_id` | string FK | Simulation run |
# MAGIC | `tick` | integer | Tick at which the event occurred |
# MAGIC | `event_type` | enum | See event types below |
# MAGIC | `item_id` | string FK | Item type involved; null for sim-level events |
# MAGIC | `entity_id` | string | ID of the primary entity (supplier, consumer, order, disruption); null if not applicable |
# MAGIC | `payload` | json | Event-specific data (quantities, costs, magnitudes, reasoning) |
# MAGIC | `logged_at` | datetime | Wall-clock time the event was written |
# MAGIC
# MAGIC **Event types**:
# MAGIC
# MAGIC | Event Type | Fired When | Key Payload Fields |
# MAGIC |---|---|---|
# MAGIC | `SIM_STARTED` | Tick 0 initialisation | `config_snapshot` |
# MAGIC | `SIM_ENDED` | Final tick completes | `total_cost`, `total_stockout_ticks`, `total_reorders` |
# MAGIC | `TICK_STARTED` | Beginning of each tick, before any sub-steps | `tick` |
# MAGIC | `TICK_ENDED` | End of each tick, after all sub-steps | `tick` |
# MAGIC | `DEMAND_DRAWN` | Demand sampled each tick per item | `raw_demand`, `disrupted_demand`, `fulfilled`, `unmet` |
# MAGIC | `SUPPLY_ARRIVED` | Pending order arrives | `order_id`, `ordered_qty`, `arrived_qty`, `lost_qty` |
# MAGIC | `REORDER_PLACED` | Agent places a reorder | `order_id`, `order_qty`, `expected_arrival_tick`, `order_cost` |
# MAGIC | `REORDER_HELD` | Agent evaluates and decides not to reorder | `stock_on_hand`, `stock_in_transit`, `reasoning` |
# MAGIC | `DISRUPTION_ACTIVATED` | Disruption begins or stochastic disruption triggers | `disruption_id`, `disruption_type`, `effective_magnitude` |
# MAGIC | `DISRUPTION_DEACTIVATED` | Disruption window ends | `disruption_id` |
# MAGIC | `STOCKOUT_OCCURRED` | Unmet demand > 0 in a tick | `unmet_demand`, `stockout_cost` |
# MAGIC | `BUDGET_WARNING` | Remaining budget falls below `budget_warning_threshold` | `remaining_budget`, `budget_limit`, `threshold` |
# MAGIC | `BUDGET_EXHAUSTED` | Budget reaches zero | `tick`, `remaining_budget` |
# MAGIC | `COST_ACCRUED` | End-of-tick cost accumulation | `holding_cost`, `stockout_cost`, `order_cost`, `transit_loss_cost`, `tick_total` |
# MAGIC | `TRANSIT_LOSS_APPLIED` | Units lost from an in-transit order | `order_id`, `lost_qty`, `arrived_qty`, `disruption_id` |
# MAGIC | `LEAD_TIME_EXTENDED` | Transit delay disruption increases lead time of a placed order | `order_id`, `original_lead_time`, `extended_lead_time`, `disruption_id` |
# MAGIC | `EXECUTOR_ALL_STALE` | Only stale agent context instances are available for the LLM agent wrapper's executor block | `queue_size`, `oldest_tick`, `newest_tick`, `current_tick` |
# MAGIC | `FALLBACK_STRUCTURAL` | Fallback to rule-based agent due to error in the structure of the LLM's response | `raw_response`, `error` |
# MAGIC | `FALLBACK_LOGICAL` | Fallback to rule-based agent due to error with respect to the logical constraints of the LLM's response | `violations` |
# MAGIC
# MAGIC > **⚠️ SUGGESTION (important)** - *`TICK_STARTED` and `TICK_ENDED` events have been added to the table above. These bookend every tick in the log, making it possible to distinguish quiet ticks (no demand, no arrivals, no orders) from gaps or missing log entries during replay.*
# MAGIC
# MAGIC > **⚠️ SUGGESTION (minor)** - *`BUDGET_WARNING` previously hardcoded the 10% threshold in its description. It now references the configurable `budget_warning_threshold` parameter (see section 3.1) and includes `threshold` as a payload field for auditability. Additionally, consider adding a one-shot guard: once `BUDGET_WARNING` has been fired for a given `sim_id`, it should not fire again until the budget recovers above the threshold. Otherwise it will fire on every tick once triggered, flooding the log.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  hackathon_of_the_century.tables4eventlog.event_log (
# MAGIC
# MAGIC  event_id STRING NOT NULL COMMENT 'Primary key. UUID uniquely identifying this event record.',
# MAGIC  sim_id STRING NOT NULL COMMENT 'Foreign key to env_sim_config.sim_id. Identifies the simulation run that produced this event.',
# MAGIC  tick INT NOT NULL COMMENT 'Simulation tick at which this event occurred. TICK_STARTED and TICK_ENDED bookend every tick; all other events fall between them. Use this column for simulation-time ordering, not logged_at.',
# MAGIC  event_type STRING NOT NULL COMMENT 'Category of event. Allowed values: SIM_STARTED, SIM_ENDED, TICK_STARTED, TICK_ENDED, DEMAND_DRAWN, SUPPLY_ARRIVED, REORDER_PLACED, REORDER_HELD, DISRUPTION_ACTIVATED, DISRUPTION_DEACTIVATED, STOCKOUT_OCCURRED, BUDGET_WARNING, BUDGET_EXHAUSTED, COST_ACCRUED, TRANSIT_LOSS_APPLIED, LEAD_TIME_EXTENDED.',
# MAGIC  item_id STRING COMMENT 'Foreign key to env_item_types.item_id. The item type involved in this event. NULL for simulation-level events: SIM_STARTED, SIM_ENDED, TICK_STARTED, TICK_ENDED, BUDGET_WARNING, BUDGET_EXHAUSTED.',
# MAGIC  entity_id STRING COMMENT 'ID of the primary non-item entity involved. Interpretation by event_type: order_id for REORDER_PLACED, REORDER_HELD, SUPPLY_ARRIVED, TRANSIT_LOSS_APPLIED, LEAD_TIME_EXTENDED; disruption_id for DISRUPTION_ACTIVATED, DISRUPTION_DEACTIVATED; NULL for all others.',
# MAGIC  payload STRING NOT NULL COMMENT 'JSON object carrying event-specific fields. Stored as STRING; parse as JSON at read time. Key fields by event_type - SIM_STARTED: {config_snapshot}; SIM_ENDED: {total_cost, total_stockout_ticks, total_reorders}; TICK_STARTED|TICK_ENDED: {tick}; DEMAND_DRAWN: {raw_demand, disrupted_demand, fulfilled, unmet}; SUPPLY_ARRIVED: {order_id, ordered_qty, arrived_qty, lost_qty}; REORDER_PLACED: {order_id, order_qty, expected_arrival_tick, order_cost}; REORDER_HELD: {stock_on_hand, stock_in_transit, reasoning}; DISRUPTION_ACTIVATED: {disruption_id, disruption_type, effective_magnitude}; DISRUPTION_DEACTIVATED: {disruption_id}; STOCKOUT_OCCURRED: {unmet_demand, stockout_cost}; BUDGET_WARNING: {remaining_budget, budget_limit, threshold}; BUDGET_EXHAUSTED: {tick, remaining_budget}; COST_ACCRUED: {holding_cost, stockout_cost, order_cost, transit_loss_cost, tick_total}; TRANSIT_LOSS_APPLIED: {order_id, lost_qty, arrived_qty, disruption_id}; LEAD_TIME_EXTENDED: {order_id, original_lead_time, extended_lead_time, disruption_id}.',
# MAGIC  logged_at TIMESTAMP NOT NULL COMMENT 'Wall-clock timestamp at which this event was written to the log by the simulation engine. Not a simulation time - use tick for simulation-time ordering.',
# MAGIC
# MAGIC  CONSTRAINT pk_event_log PRIMARY KEY (event_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Unified, append-only, immutable event stream. Every state-changing action in the simulation writes a row here (FR-08). TICK_STARTED and TICK_ENDED events bookend every tick, making quiet ticks distinguishable from log gaps. Source of truth for simulation replay and audit.'
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.appendOnly' = 'true',
# MAGIC  'simulation.layer' = 'event'
# MAGIC )
# MAGIC PARTITIONED BY (sim_id);