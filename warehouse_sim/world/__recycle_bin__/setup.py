'''
warehouse_sim/world/setup.py

Writes a fully constructed SimWorld into the Databricks env tables.
This is the world-initialisation step that runs once before the engine starts.

It is the write-side complement of config/loader.py (which reads the same tables).
No engine or agent dependency - only Stage 1 models are imported.

Usage:
    from warehouse_sim.world.setup import write_world, teardown_world
    write_world(spark, world)
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


#################################################
# Catalog / schema constants - must match loader.py and DDL
#################################################

CATALOG = "hackathon_of_the_century"
ENV     = f"{CATALOG}.tables4env"


def _t(name: str) -> str:
    return f"{ENV}.{name}"


#################################################
# Internal helpers
#################################################

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _Row(**kwargs):
    '''Lazy Row constructor so this module is importable without PySpark installed.'''
    from pyspark.sql import Row
    return Row(**kwargs)


def _delete_sim_rows(spark: "SparkSession", sim_id: str) -> None:
    '''
    Remove all rows scoped to sim_id from tables that carry a sim_id column.
    Tables without sim_id (env_item_types, env_suppliers, env_consumers) are
    NOT touched - those entities are shared across runs.
    '''
    for table in [
        "env_sim_config",
        "env_supplier_item_map",
        "env_consumer_item_map",
        "env_patterns",
        "env_disruption_schedule",
    ]:
        spark.sql(f"DELETE FROM {_t(table)} WHERE sim_id = '{sim_id}'")


#################################################
# Per-table writers
#################################################

def _write_sim_config(spark: "SparkSession", config: SimConfig) -> None:
    row = _Row(
        sim_id                     = config.sim_id,
        random_seed                = config.random_seed,
        num_ticks                  = config.num_ticks,
        run_mode                   = config.run_mode.value,
        tick_unit                  = config.tick_unit.value,
        budget_limit               = config.budget_limit,
        budget_warning_threshold   = config.budget_warning_threshold,
        agent_history_window_ticks = config.agent_history_window_ticks,
        start_timestamp            = config.start_timestamp,
        created_at                 = config.created_at or _now(),
    )
    spark.createDataFrame([row]).write.mode("append").saveAsTable(_t("env_sim_config"))


def _write_items(spark: "SparkSession", items: dict[str, ItemType]) -> None:
    '''
    env_item_types has no sim_id - upsert pattern: delete by item_id then insert.
    Safe because item definitions are expected to be consistent across runs.
    '''
    item_ids = list(items.keys())
    ids_sql  = ", ".join(f"'{i}'" for i in item_ids)
    spark.sql(f"DELETE FROM {_t('env_item_types')} WHERE item_id IN ({ids_sql})")

    rows = [
        _Row(
            item_id                         = it.item_id,
            item_name                       = it.item_name,
            unit_value                      = it.unit_value,
            initial_stock                   = it.initial_stock,
            reorder_point                   = it.reorder_point,
            min_order_qty                   = it.min_order_qty,
            max_order_qty                   = it.max_order_qty,
            holding_cost_per_unit_per_tick  = it.holding_cost_per_unit_per_tick,
            stockout_cost_per_unit_per_tick = it.stockout_cost_per_unit_per_tick,
            order_fixed_cost                = it.order_fixed_cost,
            order_variable_cost_per_unit    = it.order_variable_cost_per_unit,
            transit_loss_cost_per_unit      = it.transit_loss_cost_per_unit,
        )
        for it in items.values()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_item_types"))


def _write_suppliers(spark: "SparkSession", suppliers: dict[str, Supplier]) -> None:
    ids_sql = ", ".join(f"'{s}'" for s in suppliers)
    spark.sql(f"DELETE FROM {_t('env_suppliers')} WHERE supplier_id IN ({ids_sql})")

    rows = [
        _Row(
            supplier_id           = s.supplier_id,
            supplier_name         = s.supplier_name,
            base_lead_time_ticks  = s.base_lead_time_ticks,
            lead_time_variability = s.lead_time_variability,
        )
        for s in suppliers.values()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_suppliers"))


def _write_consumers(spark: "SparkSession", consumers: dict[str, Consumer]) -> None:
    ids_sql = ", ".join(f"'{c}'" for c in consumers)
    spark.sql(f"DELETE FROM {_t('env_consumers')} WHERE consumer_id IN ({ids_sql})")

    rows = [
        _Row(consumer_id=c.consumer_id, consumer_name=c.consumer_name)
        for c in consumers.values()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_consumers"))


def _write_supplier_item_map(
    spark: "SparkSession",
    sim_id: str,
    supplier_item_map: dict[str, str],  # item_id -> supplier_id
) -> None:
    rows = [
        _Row(sim_id=sim_id, supplier_id=supplier_id, item_id=item_id)
        for item_id, supplier_id in supplier_item_map.items()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_supplier_item_map"))


def _write_consumer_item_map(
    spark: "SparkSession",
    sim_id: str,
    consumer_item_map: dict[str, str],  # item_id -> consumer_id
) -> None:
    rows = [
        _Row(sim_id=sim_id, consumer_id=consumer_id, item_id=item_id)
        for item_id, consumer_id in consumer_item_map.items()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_consumer_item_map"))


def _write_patterns(
    spark: "SparkSession",
    sim_id: str,
    patterns: dict[str, Pattern],  # item_id -> Pattern
) -> None:
    rows = [
        _Row(
            pattern_id                    = p.pattern_id,
            sim_id                        = sim_id,
            item_id                       = p.item_id,
            role                          = p.role.value,
            pattern_type                  = p.pattern_type.value,
            distribution                  = p.distribution.value if p.distribution else None,
            dist_params                   = json.dumps(p.dist_params) if p.dist_params else None,
            custom_schedule               = p.custom_schedule,
            seasonal_multiplier_schedule  = p.seasonal_multiplier_schedule,
            noise_std                     = p.noise_std,
            supplier_id                   = p.supplier_id,
        )
        for p in patterns.values()
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_patterns"))


def _write_disruptions(
    spark: "SparkSession",
    disruptions: list[DisruptionSchedule],
) -> None:
    if not disruptions:
        return
    rows = [
        _Row(
            disruption_id       = d.disruption_id,
            sim_id              = d.sim_id,
            item_id             = d.item_id,
            disruption_type     = d.disruption_type.value,
            start_tick          = d.start_tick,
            end_tick            = d.end_tick,
            magnitude           = d.magnitude,
            is_stochastic       = d.is_stochastic,
            trigger_probability = d.trigger_probability,
        )
        for d in disruptions
    ]
    spark.createDataFrame(rows).write.mode("append").saveAsTable(_t("env_disruption_schedule"))


#################################################
# Public API
#################################################

def write_world(spark: "SparkSession", world: SimWorld) -> None:
    '''
    Persist a fully constructed SimWorld into the Databricks env tables.

    Safe to call repeatedly for the same sim_id - existing rows for this
    sim_id are deleted before re-insertion (idempotent).

    Write order respects logical dependencies:
      1. Shared entity tables (items, suppliers, consumers) - no sim_id
      2. sim config
      3. Mapping tables (scoped by sim_id)
      4. Patterns and disruptions (scoped by sim_id)
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
    Remove all env table rows for a given sim_id.
    Shared entity rows (items, suppliers, consumers) are NOT removed
    as they may be referenced by other sim runs.
    '''
    _delete_sim_rows(spark, sim_id)
    print(f"[setup] Teardown complete for sim_id={sim_id!r}")
