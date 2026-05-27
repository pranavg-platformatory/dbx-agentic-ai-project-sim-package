-- ============================================================
-- TEST QUERIES: LLMAgentWrapper Integration
-- Set this once and all queries below use it.
-- ============================================================

-- SET your sim_id here
DECLARE OR REPLACE VARIABLE sim_id STRING DEFAULT 'your_sim_id';


-- ============================================================
-- Q1. Did the agent produce decisions on every tick?
--
-- Expect: one row per (tick, item_id) for every tick in the run.
-- A gap in tick sequence means the runner's resilience wrap fired
-- (AGENT_ERROR in the event log) and hold-all was substituted.
-- agent_version confirms LLMAgentWrapper ran (not a stub or
-- inline test agent).
-- ============================================================
SELECT
    tick,
    item_id,
    decision,
    order_qty,
    agent_version,
    agent_reasoning,
    stock_on_hand_at_decision,
    stock_in_transit_at_decision
FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
WHERE sim_id = session.sim_id
ORDER BY tick, item_id;


-- ============================================================
-- Q2. Were any ticks missing decisions? (gap check)
--
-- Expect: zero rows. Any row here is a tick where the runner's
-- last-resort hold-all fired instead of the agent, meaning no
-- row was written to hist_reorder_decisions for that tick.
-- Cross-reference with Q5 (AGENT_ERROR events).
-- ============================================================
WITH expected_ticks AS (
    -- all ticks that had at least one decision
    SELECT DISTINCT tick FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
    WHERE sim_id = session.sim_id
),
full_range AS (
    SELECT EXPLODE(SEQUENCE(MIN(tick), MAX(tick))) AS tick FROM expected_ticks
)
SELECT f.tick AS missing_tick
FROM full_range f
LEFT JOIN expected_ticks e ON f.tick = e.tick
WHERE e.tick IS NULL
ORDER BY missing_tick;


-- ============================================================
-- Q3. Did the executor fire? At what ticks?
--
-- hist_eval_metrics is written every tick by the monitoring loop.
-- NOTE: metric values are currently 0.0 stubs - this query is
-- useful now for confirming the monitoring loop ran and the
-- trigger cadence is correct. Once metrics are implemented it
-- becomes a substantive observability query.
-- ============================================================
SELECT
    tick,
    item_id,
    metric_name,
    metric_value,
    logged_at
FROM hackathon_of_the_century.tables4hist.hist_eval_metrics
WHERE sim_id = session.sim_id
ORDER BY tick, item_id, metric_name;


-- ============================================================
-- Q4. Did any orders get placed? What is their status?
--
-- Expect: rows only for ticks where decision = 'reorder' in Q1.
-- status should be 'arrived' (or 'partially_lost'/'fully_lost')
-- for orders whose expected_arrival_tick has passed.
-- Rows still showing 'pending' beyond expected_arrival_tick
-- indicate a supply processing bug.
-- ============================================================
SELECT
    order_tick,
    item_id,
    supplier_id,
    order_qty,
    expected_arrival_tick,
    status,
    order_id,
    disruptions_active_at_order
FROM hackathon_of_the_century.tables4ops.ops_pending_orders
WHERE sim_id = session.sim_id
ORDER BY order_tick, item_id;


-- ============================================================
-- Q5. Did the LLM wrapper fall back or fail at any point?
--
-- Expect: zero rows for a clean run.
-- FALLBACK_STRUCTURAL: LLM response was unparseable
-- FALLBACK_LOGICAL:    LLM response had invalid quantities/item_ids
-- EXECUTOR_ALL_STALE:  every queued context was too old to use
-- AGENT_ERROR:         unhandled exception caught by runner's
--                      resilience wrap (last resort)
--
-- payload is JSON - key fields:
--   FALLBACK_STRUCTURAL: {raw_response, error}
--   FALLBACK_LOGICAL:    {violations}
--   EXECUTOR_ALL_STALE:  {queue_size, oldest_tick, newest_tick, current_tick}
--   AGENT_ERROR:         {exception_type, message}
-- ============================================================
SELECT
    tick,
    item_id,
    event_type,
    payload,
    logged_at
FROM hackathon_of_the_century.tables4eventlog.event_log
WHERE sim_id = session.sim_id
  AND event_type IN (
      'FALLBACK_STRUCTURAL',
      'FALLBACK_LOGICAL',
      'EXECUTOR_ALL_STALE',
      'AGENT_ERROR'
  )
ORDER BY tick;


-- ============================================================
-- Q6. Cost outcome: per-item totals at end of run.
--
-- Reads the final tick's row from ops_cost_accumulator
-- (MAX(tick) per item = cumulative totals at run end).
-- Use this for performance comparison between agents:
-- run once with RuleBasedAgent, once with LLMAgentWrapper
-- using the same seed, compare total_stockout_cost and
-- total_holding_cost across the two sim_ids.
-- ============================================================
SELECT
    item_id,
    cumulative_holding_cost,
    cumulative_stockout_cost,
    cumulative_order_cost,
    cumulative_transit_loss_cost,
    cumulative_total_cost,
    remaining_budget
FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator
WHERE sim_id = session.sim_id
  AND tick = (
      SELECT MAX(tick)
      FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator
      WHERE sim_id = session.sim_id
  )
ORDER BY item_id;


-- ============================================================
-- Q7. Cost comparison: LLMAgentWrapper vs RuleBasedAgent.
--
-- Run after executing both sim_rulebased_001 and sim_llm_001.
-- Lower stockout_cost = better demand coverage.
-- Lower holding_cost  = less over-ordering.
-- ============================================================
SELECT
    sim_id,
    SUM(cumulative_stockout_cost)   AS total_stockout_cost,
    SUM(cumulative_holding_cost)    AS total_holding_cost,
    SUM(cumulative_order_cost)      AS total_order_cost,
    SUM(cumulative_transit_loss_cost) AS total_transit_loss_cost,
    SUM(cumulative_total_cost)      AS total_cost
FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator
WHERE sim_id IN ('sim_rulebased_001', 'sim_llm_001')
  AND tick = (
      SELECT MAX(tick)
      FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator
      WHERE sim_id = ops_cost_accumulator.sim_id
  )
GROUP BY sim_id
ORDER BY sim_id;


-- ============================================================
-- Q8. Did the agent escalate anything?
--
-- Only populated by LLMReorderAgent via the escalate_item UC
-- function. The simulation engine never writes here.
-- OPEN status = awaiting human review.
-- context_json holds the AgentContext snapshot at escalation time.
-- ============================================================
SELECT
    tick,
    item_id,
    reason,
    status,
    raised_at,
    context_json
FROM hackathon_of_the_century.tables4ops.ops_escalation_queue
WHERE sim_id = session.sim_id
ORDER BY tick, item_id;


-- ============================================================
-- Q9. Stockout events: when and how bad?
--
-- Shows every tick where unmet_demand > 0, with the cost
-- incurred. Cross-reference with Q4 (pending orders) to check
-- whether a reorder was placed in the preceding ticks that
-- should have prevented the stockout.
-- ============================================================
SELECT
    tick,
    item_id,
    get_json_object(payload, '$.unmet_demand')  AS unmet_demand,
    get_json_object(payload, '$.stockout_cost') AS stockout_cost
FROM hackathon_of_the_century.tables4eventlog.event_log
WHERE sim_id = session.sim_id
  AND event_type = 'STOCKOUT_OCCURRED'
ORDER BY tick, item_id;