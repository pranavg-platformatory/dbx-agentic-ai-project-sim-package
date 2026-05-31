'''
uc_tools.py
test_llm_reorder_agent/uc_tools.py

Wraps UC functions as LangChain tools for use by the LangGraph agent.

Each tool corresponds to one UC function in
hackathon_of_the_century.agent_tools.*

The two tools that are still broken in Task 2
(get_demand_history, get_active_disruptions) are included here
but marked so the agent can be told to skip them until fixed.

Design:
- Tools are pure read wrappers except log_agent_decision and
  escalate_item which do INSERT ... SELECT via spark.sql
- No SparkSession.builder.getOrCreate() - uses the existing
  active session only, which is safe on the driver
- Each tool returns a clean string the LLM can read directly
'''

from __future__ import annotations

import json
from typing import Optional

from langchain_core.tools import tool


#################################################
# Helper - get active Spark session safely
#################################################

def _get_spark():
    '''
    Returns the active SparkSession without creating a new one.
    Raises RuntimeError if no session is active, which is the correct
    behaviour - we never want to create a SparkContext on a worker.
    '''
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            'No active SparkSession found. '
            'Tools must be called from the driver inside a Databricks notebook.'
        )
    return spark


def _df_to_json_str(df) -> str:
    '''Convert a Spark DataFrame to a compact JSON string for the LLM.'''
    rows = df.collect()
    if not rows:
        return '[]'
    return json.dumps([row.asDict() for row in rows], default=str)


#################################################
# Read tools
#################################################

@tool
def get_inventory_state(sim_id: str, item_id: str) -> str:
    '''
    Returns the latest stock snapshot for one item in one simulation run.
    Use this to verify current stock_on_hand, stock_in_transit, and
    expected_arrivals_next_tick before making a reorder decision.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier

    Returns:
        JSON string with fields: item_id, tick, stock_on_hand,
        stock_in_transit, expected_arrivals_next_tick
    '''
    spark = _get_spark()
    df = spark.sql(f'''
        SELECT * FROM hackathon_of_the_century.agent_tools.get_inventory_state(
            '{sim_id}', '{item_id}'
        )
    ''')
    return _df_to_json_str(df)


@tool
def get_demand_history(sim_id: str, item_id: str, n_ticks: int = 10) -> str:
    '''
    Returns the last N ticks of demand history for one item including
    rolling 7-tick average. Use this to understand demand velocity
    and trend before deciding order quantity.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier
        n_ticks: Number of most recent ticks to return (default 10)

    Returns:
        JSON string with fields: tick, raw_demand, disrupted_demand,
        fulfilled_demand, unmet_demand, rolling_avg_7
    '''
    # NOTE: get_demand_history UC function is pending fix from Task 2.
    # Returns a helpful message until fixed so the LLM can continue
    # reasoning using the demand history already in the prompt.
    return (
        f'[get_demand_history] UC function pending fix. '
        f'Use demand_history already provided in the prompt for '
        f'sim_id={sim_id} item_id={item_id}.'
    )


@tool
def get_pending_orders(sim_id: str, item_id: str) -> str:
    '''
    Returns all in-flight pending orders for one item.
    Always call this before placing a reorder to avoid duplicate orders.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier

    Returns:
        JSON string with fields: order_id, supplier_id, order_tick,
        expected_arrival_tick, order_qty, status
    '''
    spark = _get_spark()
    df = spark.sql(f'''
        SELECT * FROM hackathon_of_the_century.agent_tools.get_pending_orders(
            '{sim_id}', '{item_id}'
        )
    ''')
    return _df_to_json_str(df)


@tool
def get_supplier_info(sim_id: str, item_id: str) -> str:
    '''
    Returns supplier lead time and variability for one item.
    Use this to calculate how far in advance to place an order
    and to assess whether a reorder will arrive before stockout.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier

    Returns:
        JSON string with fields: supplier_id, supplier_name,
        base_lead_time_ticks, lead_time_variability
    '''
    spark = _get_spark()
    df = spark.sql(f'''
        SELECT * FROM hackathon_of_the_century.agent_tools.get_supplier_info(
            '{sim_id}', '{item_id}'
        )
    ''')
    return _df_to_json_str(df)


@tool
def get_cost_snapshot(sim_id: str, item_id: str) -> str:
    '''
    Returns cumulative costs and remaining budget for one item.
    Use this to check whether a reorder fits within the remaining
    budget before placing it.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier

    Returns:
        JSON string with fields: tick, cumulative_holding_cost,
        cumulative_stockout_cost, cumulative_order_cost,
        cumulative_transit_loss_cost, cumulative_total_cost,
        remaining_budget
    '''
    spark = _get_spark()
    df = spark.sql(f'''
        SELECT * FROM hackathon_of_the_century.agent_tools.get_cost_snapshot(
            '{sim_id}', '{item_id}'
        )
    ''')
    return _df_to_json_str(df)


@tool
def get_active_disruptions(sim_id: str, item_id: str) -> str:
    '''
    Returns all active disruptions for one item this tick including
    start and end ticks. Use this to understand how long a disruption
    will last and adjust order quantity accordingly.

    Args:
        sim_id: The simulation run identifier
        item_id: The item type identifier

    Returns:
        JSON string with fields: disruption_id, disruption_type,
        effective_magnitude, is_active_this_tick, start_tick, end_tick
    '''
    # NOTE: get_active_disruptions UC function is pending fix from Task 2.
    # Returns a helpful message until fixed so the LLM can continue
    # reasoning using the disruption data already in the prompt.
    return (
        f'[get_active_disruptions] UC function pending fix. '
        f'Use active_disruptions already provided in the prompt for '
        f'sim_id={sim_id} item_id={item_id}.'
    )

# uc_tools.py - add this tool
@tool
def get_full_context(sim_id: str, item_id: str, history_ticks: int = 10) -> str:
    '''
    Single-call context assembly for one item. Returns inventory, supplier,
    costs, demand summary, pending orders, and disruption summary in one row.
    Call this first for each item before making a reorder decision. Only call
    individual tools if you need more detail than this provides.

    Args:
        sim_id:        The simulation run identifier
        item_id:       The item type identifier
        history_ticks: Number of demand history ticks to summarise (default 10)

    Returns:
        JSON string with full context for the item
    '''
    spark = _get_spark()
    df = spark.sql(f'''
        SELECT * FROM hackathon_of_the_century.agent_tools.get_full_context(
            '{sim_id}', '{item_id}', {history_ticks}
        )
    ''')
    return _df_to_json_str(df)

#################################################
# Write tools
#################################################

@tool
def log_agent_decision(
    sim_id:     str,
    item_id:    str,
    tick:       int,
    decision:   str,
    order_qty:  int,
    rationale:  str,
    confidence: float,
    agent_ver:  str,
) -> str:
    '''
    Logs the agent decision and reasoning to hist_reorder_decisions.
    Always call this for every item after deciding, whether reorder or hold.

    Args:
        sim_id:     The simulation run identifier
        item_id:    The item type identifier
        tick:       Current simulation tick
        decision:   Either "reorder" or "hold"
        order_qty:  Units to order (0 for hold)
        rationale:  Full reasoning text explaining the decision
        confidence: Self-reported confidence score between 0.0 and 1.0
        agent_ver:  Agent version string

    Returns:
        Confirmation string
    '''
    
    spark = _get_spark()
    safe_rationale = rationale.replace("'", "''")
    spark.sql(f'''
        INSERT INTO hackathon_of_the_century.tables4hist.hist_reorder_decisions
        SELECT * FROM hackathon_of_the_century.agent_tools.log_agent_decision(
            '{sim_id}', {tick}, '{item_id}', '{decision}',
            {order_qty}, '{safe_rationale}', {confidence}, '{agent_ver}'
        )
    ''')
    return f'logged: {decision} qty={order_qty} for {item_id} tick={tick}'


@tool
def escalate_item(
    sim_id:       str,
    item_id:      str,
    tick:         int,
    reason:       str,
    context_json: str,
) -> str:
    '''
    Escalates an item to the human review queue.
    Call this when a reorder is needed but cannot be placed due to
    budget breach, no supplier available, or imminent stockout with
    no viable order. Also make a HOLD decision for the same item.

    Args:
        sim_id:       The simulation run identifier
        item_id:      The item type identifier
        tick:         Current simulation tick
        reason:       One of: BUDGET_BREACH, NO_SUPPLIER,
                      STOCKOUT_IMMINENT, OTHER
        context_json: JSON string with relevant context fields

    Returns:
        Confirmation string
    '''
    spark = _get_spark()
    safe_ctx = context_json.replace("'", "''")
    spark.sql(f'''
        INSERT INTO hackathon_of_the_century.tables4ops.ops_escalation_queue
        SELECT * FROM hackathon_of_the_century.agent_tools.escalate_item(
            '{sim_id}', {tick}, '{item_id}', '{reason}', '{safe_ctx}'
        )
    ''')
    return f'escalated: {item_id} reason={reason} tick={tick}'


#################################################
# Tool registry - exported for use in llm_agent.py
#################################################

ALL_TOOLS = [
    get_inventory_state,
    get_demand_history,
    get_pending_orders,
    get_supplier_info,
    get_cost_snapshot,
    get_active_disruptions,
    log_agent_decision,
    escalate_item,
    get_full_context,
]