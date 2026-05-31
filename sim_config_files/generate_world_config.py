#!/usr/bin/env python3
'''
generate_world_config.py

Generates a randomised but plausible sim_config YAML file for the continuous simulation.
All randomisation bounds are derived from the reference world in sim_config--llm_agent.yaml and are defined as named constants below.

Usage:
    python3 generate_world_config.py \
        --sim-id      sim_continuous_rule_based \
        --output      sim_config--generated.yaml \
        --items       5 \
        --suppliers   5 \
        --consumers   5 \
        --disruptions 3

All arguments are optional; defaults are shown above in the example.
Run with --help for the full list.
'''

from __future__ import annotations

import argparse
import random
import string
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


###############################################################################
# Randomisation bounds
# All ranges are derived from the reference world (sim_config--llm_agent.yaml).
# Adjust these constants to widen or narrow the space of generated worlds.
# Comments indicate the reference value and the rationale for the chosen range.
###############################################################################

# ── Items ─────────────────────────────────────────────────────────────────────

# Unit value of an item (£). Reference: item_A=5.0, item_B=12.0.
# Range covers low-value consumables through mid-value components.
ITEM_UNIT_VALUE             = (2.0,   25.0)

# Starting stock level. Reference: item_A=80, item_B=40.
# Low end avoids immediate stockout; high end avoids trivially never reordering.
ITEM_INITIAL_STOCK          = (20,    120)

# Reorder point as a fraction of initial_stock. Reference: ~0.31 (item_A=25/80),
# ~0.38 (item_B=15/40). Kept proportional so reorder point scales with stock.
ITEM_REORDER_POINT_FRACTION = (0.20,  0.45)

# Min order quantity. Reference: item_A=20, item_B=10.
ITEM_MIN_ORDER_QTY          = (5,     30)

# Max order quantity multiplier over min_order_qty. Reference: item_A=150/20=7.5×,
# item_B=80/10=8×. Keeps max sensibly above min.
ITEM_MAX_ORDER_QTY_MULT     = (4.0,   10.0)

# Holding cost per unit per tick (£). Reference: item_A=0.05, item_B=0.10.
# Scales loosely with unit value — expensive items cost more to store.
ITEM_HOLDING_COST           = (0.02,  0.20)

# Stockout cost per unit per tick (£). Reference: item_A=2.0, item_B=5.0.
# Always significantly higher than holding cost to make stockouts costly.
ITEM_STOCKOUT_COST          = (1.0,   10.0)

# Order fixed cost (£). Reference: item_A=50.0, item_B=30.0.
ITEM_ORDER_FIXED_COST       = (20.0,  80.0)

# Order variable cost per unit (£). Reference: item_A=4.5, item_B=10.0.
ITEM_ORDER_VARIABLE_COST    = (2.0,   15.0)

# Transit loss cost per unit (£). Reference: item_A=6.0, item_B=15.0.
# Always above order_variable_cost to make transit loss meaningfully painful.
ITEM_TRANSIT_LOSS_COST      = (4.0,   25.0)

# ── Demand patterns ───────────────────────────────────────────────────────────

# Probability that an item gets a custom (cyclic) pattern vs. Poisson.
# Reference world: 1 of 2 items is custom. ~0.5 feels balanced.
DEMAND_CUSTOM_PROBABILITY   = 0.5

# Cycle length for custom patterns (number of ticks). Reference: 5.
DEMAND_CUSTOM_CYCLE_LEN     = (3,     8)

# Demand values within a custom cycle. Reference: [10,15,20,12,8], range ≈ 8–20.
DEMAND_CUSTOM_VALUE         = (5.0,   25.0)

# Noise std for custom patterns. Reference: 2.0.
DEMAND_CUSTOM_NOISE_STD     = (0.5,   4.0)

# Poisson mu (mean demand per tick). Reference: item_B mu=8.
DEMAND_POISSON_MU           = (4.0,   15.0)

# ── Suppliers ─────────────────────────────────────────────────────────────────

# Base lead time in ticks. Reference: sup_001=3, sup_002=4.
SUPPLIER_LEAD_TIME          = (2,     7)

# Lead time variability (std in ticks). Reference: sup_001=0.5, sup_002=1.0.
# Capped below base_lead_time to avoid nonsensical negative lead times.
SUPPLIER_LEAD_TIME_VAR      = (0.0,   2.0)

# ── Disruptions ───────────────────────────────────────────────────────────────

# Available disruption types (must match DisruptionType enum in warehouse_sim).
DISRUPTION_TYPES            = ["demand_spike", "transit_delay"]

# Tick at which a disruption can start. Reference: 10 and 20.
# Starts after the simulation has had a few ticks to stabilise.
DISRUPTION_START_TICK       = (5,     30)

# Disruption magnitude multiplier. Reference: demand_spike=2.5, transit_delay=1.5.
DISRUPTION_MAGNITUDE        = (1.2,   3.0)

# Probability of activation per tick (stochastic disruptions only).
# Reference: demand_spike=0.40, transit_delay=0.25.
DISRUPTION_TRIGGER_PROB     = (0.10,  0.50)

# Fixed end tick. Reference: 999999 (effectively infinite for open-ended runs).
DISRUPTION_END_TICK         = 999_999

# ── Simulation defaults (not randomised; exposed for easy editing) ─────────────

DEFAULT_CATALOG                   = "hackathon_of_the_century"
DEFAULT_AGENT_TYPE                = "rule_based" # available types: "llm", "rule_based"
DEFAULT_RUN_MODE                  = "infinite"
DEFAULT_TICK_UNIT                 = "hour"
DEFAULT_TICK_DURATION_SECONDS     = 3.0
DEFAULT_PRINT_EVERY_N_TICKS       = 1
DEFAULT_SEED                      = 42
DEFAULT_EXECUTOR_TRIGGER_N        = 2
DEFAULT_LLM_PACKAGE_PATH          = "/Workspace/Shared/reorder-llm-agent"
DEFAULT_BUDGET_LIMIT              = None           # null in YAML = unlimited
DEFAULT_BUDGET_WARNING_THRESHOLD  = 0.10
DEFAULT_AGENT_HISTORY_WINDOW      = 10


###############################################################################
# Name pools
# Used to construct plausible item, supplier, and consumer names.
###############################################################################

_ITEM_ADJECTIVES  = ["Compact", "Heavy-Duty", "Precision", "Standard", "Premium",
                      "Modular", "Ultra", "Bulk", "Micro", "Deluxe"]
_ITEM_NOUNS       = ["Widget", "Gadget", "Component", "Assembly", "Unit",
                      "Module", "Part", "Device", "Fitting", "Pack"]
_ITEM_SUFFIXES    = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon",
                      "Pro", "Plus", "Lite", "Max", "Core"]

_SUPPLIER_NAMES   = ["Acme Corp", "Globex Ltd", "Initech", "Umbrella Supply",
                      "Vandelay Industries", "Soylent Logistics", "Oscorp Materials",
                      "Wayne Enterprises", "Stark Components", "LexCorp Distribution"]

_CONSUMER_NAMES   = ["Retail Division", "Wholesale Arm", "Export Unit",
                      "Northern Region", "Southern Region", "Online Fulfilment",
                      "B2B Channel", "Direct Sales", "Partner Network", "EU Operations"]


###############################################################################
# Helpers
###############################################################################

def _uid(prefix: str, n: int = 3) -> str:
    """Short unique-ish suffix for IDs: e.g. 'item_a3f'."""
    chars = string.ascii_lowercase + string.digits
    return f"{prefix}_{''.join(random.choices(chars, k=n))}"


def _item_name(index: int) -> str:
    adj    = _ITEM_ADJECTIVES[index % len(_ITEM_ADJECTIVES)]
    noun   = _ITEM_NOUNS[(index * 3) % len(_ITEM_NOUNS)]
    suffix = _ITEM_SUFFIXES[(index * 7) % len(_ITEM_SUFFIXES)]
    return f"{adj} {noun} {suffix}"


def _supplier_name(index: int) -> str:
    return _SUPPLIER_NAMES[index % len(_SUPPLIER_NAMES)]


def _consumer_name(index: int) -> str:
    return _CONSUMER_NAMES[index % len(_CONSUMER_NAMES)]


def _rf(lo: float, hi: float, dp: int = 2) -> float:
    """Random float in [lo, hi] rounded to dp decimal places."""
    return round(random.uniform(lo, hi), dp)


def _ri(lo: int, hi: int) -> int:
    """Random int in [lo, hi] inclusive."""
    return random.randint(lo, hi)


###############################################################################
# Generators
###############################################################################

def _gen_item(index: int) -> tuple[str, dict[str, Any]]:
    item_id       = f"item_{string.ascii_lowercase[index]}" if index < 26 else _uid("item")
    initial_stock = _ri(*ITEM_INITIAL_STOCK)
    reorder_point = max(5, int(initial_stock * _rf(*ITEM_REORDER_POINT_FRACTION)))
    min_order_qty = _ri(*ITEM_MIN_ORDER_QTY)
    max_order_qty = max(min_order_qty + 10,
                        int(min_order_qty * _rf(*ITEM_MAX_ORDER_QTY_MULT)))

    return item_id, {
        "name":                           _item_name(index),
        "unit_value":                     _rf(*ITEM_UNIT_VALUE),
        "initial_stock":                  initial_stock,
        "reorder_point":                  reorder_point,
        "min_order_qty":                  min_order_qty,
        "max_order_qty":                  max_order_qty,
        "holding_cost_per_unit_per_tick": _rf(*ITEM_HOLDING_COST),
        "stockout_cost_per_unit_per_tick":_rf(*ITEM_STOCKOUT_COST),
        "order_fixed_cost":               _rf(*ITEM_ORDER_FIXED_COST),
        "order_variable_cost_per_unit":   _rf(*ITEM_ORDER_VARIABLE_COST),
        "transit_loss_cost_per_unit":     _rf(*ITEM_TRANSIT_LOSS_COST),
    }


def _gen_supplier(index: int) -> tuple[str, dict[str, Any]]:
    sup_id       = f"sup_{index + 1:03d}"
    base_lt      = _ri(*SUPPLIER_LEAD_TIME)
    # Variability capped at base_lead_time - 1 to avoid nonsensical zero/negative draws.
    max_var      = max(0.0, float(base_lt - 1))
    variability  = round(min(_rf(*SUPPLIER_LEAD_TIME_VAR), max_var), 2)

    return sup_id, {
        "name":                  _supplier_name(index),
        "base_lead_time_ticks":  base_lt,
        "lead_time_variability": variability,
    }


def _gen_consumer(index: int) -> tuple[str, dict[str, Any]]:
    con_id = f"con_{index + 1:03d}"
    return con_id, {"name": _consumer_name(index)}


def _gen_demand_pattern(item_id: str) -> dict[str, Any]:
    if random.random() < DEMAND_CUSTOM_PROBABILITY:
        cycle_len = _ri(*DEMAND_CUSTOM_CYCLE_LEN)
        return {
            "pattern_type":    "custom",
            "custom_schedule": [_rf(*DEMAND_CUSTOM_VALUE) for _ in range(cycle_len)],
            "noise_std":       _rf(*DEMAND_CUSTOM_NOISE_STD),
        }
    else:
        return {
            "pattern_type": "statistical",
            "distribution": "poisson",
            "dist_params":  {"mu": _rf(*DEMAND_POISSON_MU)},
            "noise_std":    0.0,
        }


def _gen_disruption(
    sim_id:   str,
    index:    int,
    item_ids: list[str],
) -> dict[str, Any]:
    item_id = random.choice(item_ids)
    d_type  = random.choice(DISRUPTION_TYPES)
    # Stagger start ticks so disruptions don't all fire at once.
    start   = _ri(*DISRUPTION_START_TICK) + index * 3

    return {
        "disruption_id":       f"dis_{d_type.split('_')[0]}_{index + 1:02d}",
        "item_id":             item_id,
        "disruption_type":     d_type,
        "start_tick":          start,
        "end_tick":            DISRUPTION_END_TICK,
        "magnitude":           _rf(*DISRUPTION_MAGNITUDE),
        "is_stochastic":       True,
        "trigger_probability": _rf(*DISRUPTION_TRIGGER_PROB),
    }


###############################################################################
# World builder
###############################################################################

def generate_world(
    sim_id:         str,
    n_items:        int,
    n_suppliers:    int,
    n_consumers:    int,
    n_disruptions:  int,
    seed:           int,
) -> dict[str, Any]:

    random.seed(seed)

    if n_suppliers < n_items:
        print(
            f"Warning: n_suppliers ({n_suppliers}) < n_items ({n_items}). "
            f"Some suppliers will serve multiple items (round-robin assignment). "
            f"The 1:1 constraint is maintained per-item; surplus suppliers are included.",
            file=sys.stderr,
        )

    # ── Items ─────────────────────────────────────────────────────────────────
    items: dict[str, Any] = {}
    item_ids: list[str]   = []
    for i in range(n_items):
        item_id, item_def = _gen_item(i)
        items[item_id]    = item_def
        item_ids.append(item_id)

    # ── Suppliers ─────────────────────────────────────────────────────────────
    suppliers: dict[str, Any] = {}
    supplier_ids: list[str]   = []
    for i in range(n_suppliers):
        sup_id, sup_def  = _gen_supplier(i)
        suppliers[sup_id] = sup_def
        supplier_ids.append(sup_id)

    # ── Consumers ─────────────────────────────────────────────────────────────
    consumers: dict[str, Any] = {}
    consumer_ids: list[str]   = []
    for i in range(n_consumers):
        con_id, con_def   = _gen_consumer(i)
        consumers[con_id]  = con_def
        consumer_ids.append(con_id)

    # ── Maps (1:1 item→supplier, round-robin item→consumer) ──────────────────
    supplier_item_map: dict[str, str] = {
        item_id: supplier_ids[i % len(supplier_ids)]
        for i, item_id in enumerate(item_ids)
    }
    consumer_item_map: dict[str, str] = {
        item_id: consumer_ids[i % len(consumer_ids)]
        for i, item_id in enumerate(item_ids)
    }

    # ── Demand patterns ───────────────────────────────────────────────────────
    demand_patterns: dict[str, Any] = {
        item_id: _gen_demand_pattern(item_id)
        for item_id in item_ids
    }

    # ── Disruptions ───────────────────────────────────────────────────────────
    disruptions: list[dict[str, Any]] = [
        _gen_disruption(sim_id, i, item_ids)
        for i in range(n_disruptions)
    ]

    return {
        "items":              items,
        "suppliers":          suppliers,
        "consumers":          consumers,
        "supplier_item_map":  supplier_item_map,
        "consumer_item_map":  consumer_item_map,
        "demand_patterns":    demand_patterns,
        "disruptions":        disruptions,
        "sim_config": {
            "budget_limit":               DEFAULT_BUDGET_LIMIT,
            "budget_warning_threshold":   DEFAULT_BUDGET_WARNING_THRESHOLD,
            "agent_history_window_ticks": DEFAULT_AGENT_HISTORY_WINDOW,
        },
    }


###############################################################################
# Full config builder
###############################################################################

def generate_config(
    sim_id:        str,
    n_items:       int,
    n_suppliers:   int,
    n_consumers:   int,
    n_disruptions: int,
    seed:          int,
) -> dict[str, Any]:

    return {
        # ── Shared ────────────────────────────────────────────────────────────
        "sim_id":  sim_id,
        "catalog": DEFAULT_CATALOG,

        # ── Agent ─────────────────────────────────────────────────────────────
        "agent": {
            "type": DEFAULT_AGENT_TYPE,
        },

        # ── Simulation ────────────────────────────────────────────────────────
        "simulation": {
            "run_mode":              DEFAULT_RUN_MODE,
            "tick_unit":             DEFAULT_TICK_UNIT,
            "tick_duration_seconds": DEFAULT_TICK_DURATION_SECONDS,
            "print_every_n_ticks":   DEFAULT_PRINT_EVERY_N_TICKS,
            "seed":                  seed,
        },

        # ── LLM agent ─────────────────────────────────────────────────────────
        "llm_agent": {
            "executor_trigger_n": DEFAULT_EXECUTOR_TRIGGER_N,
            "package_path":       DEFAULT_LLM_PACKAGE_PATH,
            "config_override":    {},
        },

        # ── World ─────────────────────────────────────────────────────────────
        "world": generate_world(
            sim_id        = sim_id,
            n_items       = n_items,
            n_suppliers   = n_suppliers,
            n_consumers   = n_consumers,
            n_disruptions = n_disruptions,
            seed          = seed,
        ),
    }


###############################################################################
# YAML serialisation
# PyYAML dumps None as "null" by default but omits the value for None-valued
# keys unless we force it. A custom representer keeps budget_limit: null
# explicit in the output, consistent with the reference YAML.
###############################################################################

def _none_representer(dumper: yaml.Dumper, _: None) -> yaml.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:null", "null")


yaml.add_representer(type(None), _none_representer)


def _to_yaml(config: dict[str, Any]) -> str:
    header = (
        f"# sim_config.yaml  (generated by generate_world_config.py)\n"
        f"# sim_id  : {config['sim_id']}\n"
        f"# generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"#\n"
        f"# Edit the bounds at the top of generate_world_config.py to change\n"
        f"# the randomisation ranges, then re-run to produce a new config.\n\n"
    )
    return header + yaml.dump(
        config,
        default_flow_style = False,
        allow_unicode      = True,
        sort_keys          = False,
        indent             = 2,
    )


###############################################################################
# CLI
###############################################################################

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description = "Generate a randomised sim_config YAML for the continuous simulation.",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sim-id",
        required = True,
        help     = "Unique simulation ID written into the YAML (e.g. sim_continuous_002).",
    )
    parser.add_argument(
        "--output",
        required = True,
        help     = "Output YAML file path (e.g. sim_config--generated.yaml).",
    )
    parser.add_argument(
        "--items",
        type    = int,
        default = 2,
        help    = "Number of items to generate.",
    )
    parser.add_argument(
        "--suppliers",
        type    = int,
        default = 2,
        help    = "Number of suppliers to generate. Must be >= --items for strict 1:1 mapping; "
                  "if fewer, assignment wraps round-robin.",
    )
    parser.add_argument(
        "--consumers",
        type    = int,
        default = 1,
        help    = "Number of consumers to generate.",
    )
    parser.add_argument(
        "--disruptions",
        type    = int,
        default = 2,
        help    = "Number of disruptions to generate.",
    )
    parser.add_argument(
        "--seed",
        type    = int,
        default = DEFAULT_SEED,
        help    = "Random seed. Same seed + same counts = same output.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Validate
    if args.items < 1:
        sys.exit("Error: --items must be >= 1.")
    if args.suppliers < 1:
        sys.exit("Error: --suppliers must be >= 1.")
    if args.consumers < 1:
        sys.exit("Error: --consumers must be >= 1.")
    if args.disruptions < 0:
        sys.exit("Error: --disruptions must be >= 0.")

    output_path = Path(args.output)
    if output_path.exists():
        print(f"Warning: {output_path} already exists and will be overwritten.", file=sys.stderr)

    config = generate_config(
        sim_id        = args.sim_id,
        n_items       = args.items,
        n_suppliers   = args.suppliers,
        n_consumers   = args.consumers,
        n_disruptions = args.disruptions,
        seed          = args.seed,
    )

    output_path.write_text(_to_yaml(config), encoding="utf-8")

    print(f"Generated: {output_path}")
    print(f"  sim_id      : {args.sim_id}")
    print(f"  items       : {args.items}")
    print(f"  suppliers   : {args.suppliers}")
    print(f"  consumers   : {args.consumers}")
    print(f"  disruptions : {args.disruptions}")
    print(f"  seed        : {args.seed}")


if __name__ == "__main__":
    main()