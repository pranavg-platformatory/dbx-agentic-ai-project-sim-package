'''
warehouse_sim/world/setup.py

- Writes a fully constructed SimWorld into the Databricks env tables
- This is the world-initialisation step that runs once before the engine starts
- It is the write-side complement of config/loader.py (which reads the same tables)

---

NOTE: No engine or agent dependency - only Stage 1 models (i.e. warehouse_sim/config) are imported.

---

Usage:

```
from warehouse_sim.world.setup import write_world, teardown_world
write_world(spark, world)
```
'''

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ..config.models import (
    Consumer,
    DisruptionSchedule,
    ItemType,
    Pattern,
    SimConfig,
    SimWorld,
    Supplier,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Catalog / schema constants - must match loader.py and DDL
# ---------------------------------------------------------------------------

CATALOG = "hackathon_of_the_century"
ENV     = f"{CATALOG}.tables4env"


def _t(name: str) -> str:
    return f"{ENV}.{name}"


# ---------------------------------------------------------------------------
# Explicit DDL schemas per table

# NOTE:
# - These prevent PySpark from dealing with:
#   - Inferring long instead of int
#   - Missing nullability on arrays
#   - Ambiguous boolean/double types
# - The same class of issues were seen in the Stage 1 notebook (_testNotebooks/stage1-testAndInspect.py in this repo) when using `createDataFrame` without a schema.
# ---------------------------------------------------------------------------

# REFERENCE: _dataStoreDefinition in this repo
_SCHEMAS = {
    "env_sim_config": '''
        sim_id                     STRING,
        random_seed                BIGINT,
        num_ticks                  INT,
        run_mode                   STRING,
        tick_unit                  STRING,
        budget_limit               DOUBLE,
        budget_warning_threshold   DOUBLE,
        agent_history_window_ticks INT,
        start_timestamp            TIMESTAMP,
        created_at                 TIMESTAMP
    ''',
    "env_item_types": '''
        item_id                          STRING,
        item_name                        STRING,
        unit_value                       DOUBLE,
        initial_stock                    INT,
        reorder_point                    INT,
        min_order_qty                    INT,
        max_order_qty                    INT,
        holding_cost_per_unit_per_tick   DOUBLE,
        stockout_cost_per_unit_per_tick  DOUBLE,
        order_fixed_cost                 DOUBLE,
        order_variable_cost_per_unit     DOUBLE,
        transit_loss_cost_per_unit       DOUBLE
    ''',
    "env_suppliers": '''
        supplier_id            STRING,
        supplier_name          STRING,
        base_lead_time_ticks   INT,
        lead_time_variability  DOUBLE
    ''',
    "env_consumers": '''
        consumer_id    STRING,
        consumer_name  STRING
    ''',
    "env_supplier_item_map": '''
        sim_id       STRING,
        supplier_id  STRING,
        item_id      STRING
    ''',
    "env_consumer_item_map": '''
        sim_id       STRING,
        consumer_id  STRING,
        item_id      STRING
    ''',
    "env_patterns": '''
        pattern_id                    STRING,
        sim_id                        STRING,
        item_id                       STRING,
        role                          STRING,
        pattern_type                  STRING,
        distribution                  STRING,
        dist_params                   STRING,
        custom_schedule               ARRAY<DOUBLE>,
        seasonal_multiplier_schedule  ARRAY<DOUBLE>,
        noise_std                     DOUBLE,
        supplier_id                   STRING
    ''',
    "env_disruption_schedule": '''
        disruption_id        STRING,
        sim_id               STRING,
        item_id              STRING,
        disruption_type      STRING,
        start_tick           INT,
        end_tick             INT,
        magnitude            DOUBLE,
        is_stochastic        BOOLEAN,
        trigger_probability  DOUBLE
    ''',
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _write(spark: "SparkSession", table_key: str, rows: list[dict]) -> None:
    '''
    - Create a DataFrame from a list of dictionaries
    - IMPORTANT: Do the above using the explicit schema for that table
    - Then append to the Delta table specified by `table_key`

    NOTE:
    - Using explicit schemas avoids all PySpark type inference surprises
    - The DataFrame created from the list of dictionaries is:
        - Created within the context of `spark`, the SparkSession instance
        - Immediately appended to the specified table

    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `table_key` (str): Table name (without qualifying catalog or schema) \n
      Table names (without qualifying catalog or schema) are keys in `_SCHEMA`
    - `rows` (list[dict]): List of rows in the specified table, each row represented by a dictionary
    
    RETURNS:
    - None
    '''
    
    schema = _SCHEMAS[table_key].strip()
    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(_t(table_key))


def _delete_sim_rows(spark: "SparkSession", sim_id: str) -> None:
    '''
    Remove all rows scoped to sim_id from tables that carry a sim_id column.

    NOTE: Tables without `sim_id` ("env_item_types", "env_suppliers", "env_consumers") are NOT touched - those entities are shared across runs.
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `table` (str): Fully-qualified table name (refer to the docstring of `_get_fully_qualified_table_name`)
    - `sim_id` (str): Simulation ID

    RETURNS:
    - None
    '''

    for table in [
        "env_sim_config",
        "env_supplier_item_map",
        "env_consumer_item_map",
        "env_patterns",
        "env_disruption_schedule",
    ]:
        spark.sql(f"DELETE FROM {_t(table)} WHERE sim_id = '{sim_id}'")


# ---------------------------------------------------------------------------
# Per-table writers
# ---------------------------------------------------------------------------

# NOTE: Detailed docstrings are not given for the following as they are simply wrappers for `_write`, each function wrapping the `_write` function call needed to add data into the tables referenced/indicated by the function names.

def _write_sim_config(spark: "SparkSession", config: SimConfig) -> None:
    _write(spark, "env_sim_config", [{
        "sim_id":                     config.sim_id,
        "random_seed":                config.random_seed,
        "num_ticks":                  config.num_ticks,
        "run_mode":                   config.run_mode.value,
        "tick_unit":                  config.tick_unit.value,
        "budget_limit":               config.budget_limit,
        "budget_warning_threshold":   config.budget_warning_threshold,
        "agent_history_window_ticks": config.agent_history_window_ticks,
        "start_timestamp":            config.start_timestamp,
        "created_at":                 config.created_at or _now(),
    }])


def _write_items(spark: "SparkSession", items: dict[str, ItemType]) -> None:
    '''
    NOTE:
    - "env_item_types" has no `sim_id`
    - Hence, the upsert pattern is: delete by `item_id` then insert.
    - This is safe because item definitions are expected to be consistent across runs
    '''

    ids_sql = ", ".join(f"'{i}'" for i in items)
    spark.sql(f"DELETE FROM {_t('env_item_types')} WHERE item_id IN ({ids_sql})")

    _write(spark, "env_item_types", [
        {
            "item_id":                         it.item_id,
            "item_name":                       it.item_name,
            "unit_value":                      it.unit_value,
            "initial_stock":                   it.initial_stock,
            "reorder_point":                   it.reorder_point,
            "min_order_qty":                   it.min_order_qty,
            "max_order_qty":                   it.max_order_qty,
            "holding_cost_per_unit_per_tick":  it.holding_cost_per_unit_per_tick,
            "stockout_cost_per_unit_per_tick": it.stockout_cost_per_unit_per_tick,
            "order_fixed_cost":                it.order_fixed_cost,
            "order_variable_cost_per_unit":    it.order_variable_cost_per_unit,
            "transit_loss_cost_per_unit":      it.transit_loss_cost_per_unit,
        }
        for it in items.values()
    ])


def _write_suppliers(spark: "SparkSession", suppliers: dict[str, Supplier]) -> None:
    ids_sql = ", ".join(f"'{s}'" for s in suppliers)
    spark.sql(f"DELETE FROM {_t('env_suppliers')} WHERE supplier_id IN ({ids_sql})")

    _write(spark, "env_suppliers", [
        {
            "supplier_id":           s.supplier_id,
            "supplier_name":         s.supplier_name,
            "base_lead_time_ticks":  s.base_lead_time_ticks,
            "lead_time_variability": s.lead_time_variability,
        }
        for s in suppliers.values()
    ])


def _write_consumers(spark: "SparkSession", consumers: dict[str, Consumer]) -> None:
    ids_sql = ", ".join(f"'{c}'" for c in consumers)
    spark.sql(f"DELETE FROM {_t('env_consumers')} WHERE consumer_id IN ({ids_sql})")

    _write(spark, "env_consumers", [
        {"consumer_id": c.consumer_id, "consumer_name": c.consumer_name}
        for c in consumers.values()
    ])


def _write_supplier_item_map(
    spark: "SparkSession",
    sim_id: str,
    supplier_item_map: dict[str, str],  # item_id -> supplier_id
) -> None:
    _write(spark, "env_supplier_item_map", [
        {"sim_id": sim_id, "supplier_id": supplier_id, "item_id": item_id}
        for item_id, supplier_id in supplier_item_map.items()
    ])


def _write_consumer_item_map(
    spark: "SparkSession",
    sim_id: str,
    consumer_item_map: dict[str, str],  # item_id -> consumer_id
) -> None:
    _write(spark, "env_consumer_item_map", [
        {"sim_id": sim_id, "consumer_id": consumer_id, "item_id": item_id}
        for item_id, consumer_id in consumer_item_map.items()
    ])


def _write_patterns(
    spark: "SparkSession",
    sim_id: str,
    patterns: dict[str, Pattern],
) -> None:
    _write(spark, "env_patterns", [
        {
            "pattern_id":                   p.pattern_id,
            "sim_id":                       sim_id,
            "item_id":                      p.item_id,
            "role":                         p.role.value,
            "pattern_type":                 p.pattern_type.value,
            "distribution":                 p.distribution.value if p.distribution else None,
            "dist_params":                  json.dumps(p.dist_params) if p.dist_params else None,
            "custom_schedule":              p.custom_schedule,
            "seasonal_multiplier_schedule": p.seasonal_multiplier_schedule,
            "noise_std":                    p.noise_std,
            "supplier_id":                  p.supplier_id,
        }
        for p in patterns.values()
    ])


def _write_disruptions(
    spark: "SparkSession",
    disruptions: list[DisruptionSchedule],
) -> None:
    if not disruptions:
        return
    _write(spark, "env_disruption_schedule", [
        {
            "disruption_id":       d.disruption_id,
            "sim_id":              d.sim_id,
            "item_id":             d.item_id,
            "disruption_type":     d.disruption_type.value,
            "start_tick":          d.start_tick,
            "end_tick":            d.end_tick,
            "magnitude":           d.magnitude,
            "is_stochastic":       d.is_stochastic,
            "trigger_probability": d.trigger_probability,
        }
        for d in disruptions
    ])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_world(spark: "SparkSession", world: SimWorld) -> None:
    '''
    Persist a fully constructed SimWorld into the Databricks env tables.

    NOTE: This is safe to call repeatedly for the same `sim_id` - existing rows for this `sim_id` are deleted before re-insertion (=> idempotency).

    Write order respects logical dependencies:
    1. Shared entity tables (for items, suppliers, consumers) - no `sim_id`
    2. Simulation configuration (encapsulated by a SimConfig instance)
    3. Mapping tables (scoped by `sim_id`)
    4. Patterns and disruptions (scoped by `sim_id`)

    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `world` (SimWorld): SimWorld instance encapsulating the configuration for the simulation

    RETURNS:
    - None
    '''
    sim_id = world.config.sim_id

    # Wipe sim-scoped rows first so re-runs are safe
    _delete_sim_rows(spark, sim_id)

    # 1. Shared entity tables
    _write_items(spark, world.items)
    _write_suppliers(spark, world.suppliers)
    _write_consumers(spark, world.consumers)

    # 2. Sim config
    _write_sim_config(spark, world.config)

    # 3. Mapping tables
    _write_supplier_item_map(spark, sim_id, world.supplier_item_map)
    _write_consumer_item_map(spark, sim_id, world.consumer_item_map)

    # 4. Patterns and disruptions
    all_patterns = {**world.demand_patterns, **world.supply_patterns}
    _write_patterns(spark, sim_id, all_patterns)
    _write_disruptions(spark, world.disruptions)

    print(f"[setup] World written for sim_id={sim_id!r}")
    print(f"  items:       {len(world.items)}")
    print(f"  suppliers:   {len(world.suppliers)}")
    print(f"  consumers:   {len(world.consumers)}")
    print(f"  patterns:    {len(all_patterns)}")
    print(f"  disruptions: {len(world.disruptions)}")


def teardown_world(spark: "SparkSession", sim_id: str) -> None:
    '''
    Remove all env table rows for a given `sim_id`.
    
    NOTE: Shared entity rows (items, suppliers, consumers) are NOT removed as they may be referenced by other simulation runs.
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID

    RETURNS:
    - None
    '''
    
    _delete_sim_rows(spark, sim_id)
    print(f"[setup] Teardown complete for sim_id={sim_id!r}")