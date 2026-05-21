'''
warehouse_sim/config/loader.py

- Reads all env tables for a given `sim_id` from Databricks
- Assembles a fully typed SimWorld object

---

NOTE:
- This is the only Databricks-aware file in the infra layer
- Everything else works with plain Python objects

---

Usage (in a Databricks notebook or job):

```
from warehouse_sim.config.loader import load_world
world = load_world(spark, sim_id="sim_001")
```
'''

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from warehouse_sim.config.models import (
    Consumer,
    ConsumerItemMapping,
    DisruptionSchedule,
    ItemType,
    Pattern,
    PatternRole,
    SimConfig,
    SimWorld,
    Supplier,
    SupplierItemMapping,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Catalog / schema constants - adjust if your Databricks paths differ
# ---------------------------------------------------------------------------

CATALOG = "hackathon_of_the_century"
ENV     = f"{CATALOG}.tables4env"

def _get_fully_qualified_table_name(name: str) -> str:
    '''
    Gets the fully-qualified table name for the given table name.

    ---

    NOTE:

    Fully-qualified format: `{catalog}.{schema}.{table name}`

    E.g.: `hackathon_of_the_century.tables4env.env_item_types`

    ---

    PARAMETERS:
    - `name` (str): Table name (unqualified, i.e. without catalog and schema)

    RETURNS:
    - (str): Fully-qualified table name
    '''
    
    return f"{ENV}.{name}"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _rows(spark: "SparkSession", table: str, sim_id: str) -> list[dict]:
    '''
    Return all rows (as a list of dictionaries) in the specified table for the specified simulation ID.

    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `table` (str): Fully-qualified table name (refer to the docstring of `_get_fully_qualified_table_name`)
    - `sim_id` (str): Simulation ID

    RETURNS:
    - (list[dict]): List of rows in the specified table, each row represented by a dictionary
    '''
    
    df = spark.table(table)
    if "sim_id" in df.columns:
        df = df.filter(df.sim_id == sim_id)
    return [row.asDict() for row in df.collect()]


def _single_row(spark: "SparkSession", table: str, sim_id: str) -> dict:
    '''
    - Return the row (as a dictionaries) in the specified table for the specified simulation ID \n
      ... IF AND ONLY IF there is only one row for the specified simulation ID
    - If there are no rows or there are multiple rows for the simulation ID, raise a ValueError
    
    ---

    USE-CASE:

    Useful for validation in cases where 1 simulation ID must have only 1 entry in a table.
    
    E.g. 1 simulation must have only 1 environment configuration attached to it (stored in the table "env_sim_config" (fully qualify this table name using `_get_fully_qualified_table_name`)).

    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `table` (str): Fully-qualified table name (refer to the docstring of `_get_fully_qualified_table_name`)
    - `sim_id` (str): Simulation ID

    RETURNS:
    - (dict): Single row represented by a dictionary
    '''
    
    rows = _rows(spark, table, sim_id)
    if not rows:
        raise ValueError(f"No row found in {table} for sim_id={sim_id!r}")
    if len(rows) > 1:
        raise ValueError(f"Expected 1 row in {table} for sim_id={sim_id!r}, got {len(rows)}")
    return rows[0]

def _parse_dist_params(raw: str | dict | None) -> dict | None:
    '''
    Parses distribution parameters for some pattern (i.e. supply/demand pattern) in the table "env_patterns".
    
    NOTE:
    - The `dist_params` field stores the distribution parameters for a given pattern
    - `dist_params` is stored as a JSON string, hence parsing may be necessary

    ---

    PARAMETERS:
    - `raw` (str | dict, optional): String or dictionary containing distribution parameters.

    RETURNS:
    - (dict, optional)
    '''
    
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_world(spark: "SparkSession", sim_id: str) -> SimWorld:
    '''
    Load and validate the full world configuration for a simulation run.

    What it does:
    - Reads from all env tables in the hackathon_of_the_century catalog
    - Returns a SimWorld instance
    - Raises ValueError on any data inconsistency

    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID

    RETURNS:
    - (SimWorld): SimWorld instance encapsulating the configuration for the simulation
    '''

    # -- SimConfig -----------------------------------------------------------
    config_dict = _single_row(spark, _get_fully_qualified_table_name("env_sim_config"), sim_id)
    config = SimConfig(**config_dict)

    # -- Items ---------------------------------------------------------------
    raw_items = _rows(spark, _get_fully_qualified_table_name("env_item_types"), sim_id=sim_id)
    # env_item_types has no sim_id column; fetch all and index by item_id
    # (items are shared across runs - the mapping tables scope them per sim)
    all_item_rows = [row.asDict() for row in spark.table(_get_fully_qualified_table_name("env_item_types")).collect()]
    items: dict[str, ItemType] = {r["item_id"]: ItemType(**r) for r in all_item_rows}

    # -- Suppliers -----------------------------------------------------------
    all_supplier_rows = [row.asDict() for row in spark.table(_get_fully_qualified_table_name("env_suppliers")).collect()]
    suppliers: dict[str, Supplier] = {r["supplier_id"]: Supplier(**r) for r in all_supplier_rows}

    # -- Consumers -----------------------------------------------------------
    all_consumer_rows = [row.asDict() for row in spark.table(_get_fully_qualified_table_name("env_consumers")).collect()]
    consumers: dict[str, Consumer] = {r["consumer_id"]: Consumer(**r) for r in all_consumer_rows}

    # -- Supplier-item mapping -----------------------------------------------
    sim_supplier_map_rows = _rows(spark, _get_fully_qualified_table_name("env_supplier_item_map"), sim_id)
    supplier_item_map: dict[str, str] = {}
    for r in sim_supplier_map_rows:
        mapping = SupplierItemMapping(**r)
        if mapping.item_id in supplier_item_map:
            raise ValueError(
                f"item_id={mapping.item_id!r} is mapped to more than one supplier "
                f"in sim_id={sim_id!r}. Only one supplier per item is supported."
            )
        supplier_item_map[mapping.item_id] = mapping.supplier_id

    # -- Consumer-item mapping -----------------------------------------------
    sim_consumer_map_rows = _rows(spark, _get_fully_qualified_table_name("env_consumer_item_map"), sim_id)
    consumer_item_map: dict[str, str] = {}
    for r in sim_consumer_map_rows:
        mapping = ConsumerItemMapping(**r)
        if mapping.item_id in consumer_item_map:
            raise ValueError(
                f"item_id={mapping.item_id!r} is mapped to more than one consumer "
                f"in sim_id={sim_id!r}. Only one consumer per item is supported."
            )
        consumer_item_map[mapping.item_id] = mapping.consumer_id

    # -- Patterns ------------------------------------------------------------
    pattern_rows = _rows(spark, _get_fully_qualified_table_name("env_patterns"), sim_id)
    demand_patterns: dict[str, Pattern] = {}
    supply_patterns: dict[str, Pattern] = {}

    for r in pattern_rows:
        r["dist_params"] = _parse_dist_params(r.get("dist_params"))
        pattern = Pattern(**r)
        if pattern.role == PatternRole.DEMAND:
            if pattern.item_id in demand_patterns:
                raise ValueError(
                    f"Duplicate demand pattern for item_id={pattern.item_id!r} "
                    f"in sim_id={sim_id!r}"
                )
            demand_patterns[pattern.item_id] = pattern
        else:
            if pattern.item_id in supply_patterns:
                raise ValueError(
                    f"Duplicate supply pattern for item_id={pattern.item_id!r} "
                    f"in sim_id={sim_id!r}"
                )
            supply_patterns[pattern.item_id] = pattern

    # -- Disruptions ---------------------------------------------------------
    disruption_rows = _rows(spark, _get_fully_qualified_table_name("env_disruption_schedule"), sim_id)
    disruptions: list[DisruptionSchedule] = [DisruptionSchedule(**r) for r in disruption_rows]

    # -- Cross-reference validation ------------------------------------------
    sim_item_ids = set(supplier_item_map.keys())

    missing_demand_patterns = sim_item_ids - set(demand_patterns.keys())
    if missing_demand_patterns:
        raise ValueError(
            f"No demand pattern found for item(s): {sorted(missing_demand_patterns)} "
            f"in sim_id={sim_id!r}"
        )

    unknown_supplier_ids = set(supplier_item_map.values()) - set(suppliers.keys())
    if unknown_supplier_ids:
        raise ValueError(f"Unknown supplier_id(s) in mapping: {unknown_supplier_ids}")

    unknown_consumer_ids = set(consumer_item_map.values()) - set(consumers.keys())
    if unknown_consumer_ids:
        raise ValueError(f"Unknown consumer_id(s) in mapping: {unknown_consumer_ids}")

    # -- Assemble ------------------------------------------------------------
    return SimWorld(
        config=config,
        items={item_id: items[item_id] for item_id in sim_item_ids},
        suppliers={
            sid: suppliers[sid]
            for sid in set(supplier_item_map.values())
        },
        consumers={
            cid: consumers[cid]
            for cid in set(consumer_item_map.values())
        },
        supplier_item_map=supplier_item_map,
        consumer_item_map=consumer_item_map,
        demand_patterns=demand_patterns,
        supply_patterns=supply_patterns,
        disruptions=disruptions,
    )