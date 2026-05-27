# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hackathon_of_the_century;
# MAGIC CREATE SCHEMA IF NOT EXISTS agent_tools
# MAGIC   COMMENT 'UC functions used by the LLM reorder agent as tools';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hackathon_of_the_century.tables4ops.ops_escalation_queue (
# MAGIC   sim_id        STRING    NOT NULL COMMENT 'Simulation run identifier',
# MAGIC   tick          INT       NOT NULL COMMENT 'Tick at which escalation was raised',
# MAGIC   item_id       STRING    NOT NULL COMMENT 'Item that triggered the escalation',
# MAGIC   reason        STRING    NOT NULL COMMENT 'BUDGET_BREACH | NO_SUPPLIER | STOCKOUT_IMMINENT | OTHER',
# MAGIC   context_json  STRING             COMMENT 'JSON snapshot of relevant AgentContext fields',
# MAGIC   status        STRING    NOT NULL COMMENT 'OPEN or REVIEWED',
# MAGIC   raised_at     TIMESTAMP NOT NULL COMMENT 'Wall-clock time the escalation was written'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Escalation queue written by the LLM agent when a decision requires human review'
# MAGIC TBLPROPERTIES ('delta.appendOnly' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_inventory_state(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   item_id                     STRING,
# MAGIC   tick                        INT,
# MAGIC   stock_on_hand               INT,
# MAGIC   stock_in_transit            INT,
# MAGIC   expected_arrivals_next_tick INT
# MAGIC )
# MAGIC COMMENT 'Returns the latest warehouse state row for one item in one sim run.'
# MAGIC RETURN
# MAGIC   SELECT ws.item_id, ws.tick, ws.stock_on_hand,
# MAGIC          ws.stock_in_transit, ws.expected_arrivals_next_tick
# MAGIC   FROM hackathon_of_the_century.tables4ops.ops_warehouse_state ws
# MAGIC   INNER JOIN (
# MAGIC     SELECT item_id AS _item_id, MAX(tick) AS max_tick
# MAGIC     FROM hackathon_of_the_century.tables4ops.ops_warehouse_state
# MAGIC     WHERE sim_id = p_sim_id AND item_id = p_item_id
# MAGIC     GROUP BY item_id
# MAGIC   ) latest ON ws.item_id = latest._item_id AND ws.tick = latest.max_tick
# MAGIC   WHERE ws.sim_id = p_sim_id;

# COMMAND ----------

# %sql
# CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_demand_history(
#   p_sim_id  STRING COMMENT 'Simulation run ID',
#   p_item_id STRING COMMENT 'Item type ID',
#   p_n_ticks INT    COMMENT 'Number of most recent ticks to return'
# )
# RETURNS TABLE (
#   tick             INT,
#   raw_demand       DOUBLE,
#   disrupted_demand DOUBLE,
#   fulfilled_demand INT,
#   unmet_demand     INT,
#   rolling_avg_7    DOUBLE
# )
# COMMENT 'Returns the last N ticks of demand history for one item, plus a rolling 7-tick average.'
# RETURN
#   WITH max_tick AS (
#     SELECT MAX(tick) AS mt
#     FROM hackathon_of_the_century.tables4hist.hist_demand_actuals
#     WHERE sim_id = p_sim_id AND item_id = p_item_id
#   ),
#   base AS (
#     SELECT d.tick, d.raw_demand, d.disrupted_demand,
#            d.fulfilled_demand, d.unmet_demand
#     FROM hackathon_of_the_century.tables4hist.hist_demand_actuals d
#     CROSS JOIN max_tick
#     WHERE d.sim_id   = p_sim_id
#       AND d.item_id  = p_item_id
#       AND d.tick     > (max_tick.mt - p_n_ticks)
#   )
#   SELECT
#     tick,
#     raw_demand,
#     disrupted_demand,
#     fulfilled_demand,
#     unmet_demand,
#     AVG(fulfilled_demand) OVER (
#       ORDER BY tick ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
#     ) AS rolling_avg_7
#   FROM base
#   ORDER BY tick ASC;

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS hackathon_of_the_century.agent_tools.get_demand_history;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_demand_history(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID',
# MAGIC   p_n_ticks INT    COMMENT 'Number of most recent ticks to return'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   tick             INT,
# MAGIC   raw_demand       DOUBLE,
# MAGIC   disrupted_demand DOUBLE,
# MAGIC   fulfilled_demand INT,
# MAGIC   unmet_demand     INT,
# MAGIC   rolling_avg_7    DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns the last N ticks of demand history for one item, plus a rolling 7-tick average.'
# MAGIC RETURN
# MAGIC   WITH base AS (
# MAGIC     SELECT tick, raw_demand, disrupted_demand, fulfilled_demand, unmet_demand
# MAGIC     FROM (
# MAGIC       SELECT tick, raw_demand, disrupted_demand, fulfilled_demand, unmet_demand,
# MAGIC              ROW_NUMBER() OVER (ORDER BY tick DESC) AS rn
# MAGIC       FROM hackathon_of_the_century.tables4hist.hist_demand_actuals
# MAGIC       WHERE sim_id = p_sim_id AND item_id = p_item_id
# MAGIC     ) ranked
# MAGIC     WHERE rn <= p_n_ticks
# MAGIC   )
# MAGIC   SELECT tick, raw_demand, disrupted_demand, fulfilled_demand, unmet_demand,
# MAGIC     AVG(fulfilled_demand) OVER (
# MAGIC       ORDER BY tick ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_avg_7
# MAGIC   FROM base
# MAGIC   ORDER BY tick ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_pending_orders(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   order_id              STRING,
# MAGIC   supplier_id           STRING,
# MAGIC   order_tick            INT,
# MAGIC   expected_arrival_tick INT,
# MAGIC   order_qty             INT,
# MAGIC   status                STRING
# MAGIC )
# MAGIC COMMENT 'Returns all pending orders for one item.'
# MAGIC RETURN
# MAGIC   SELECT order_id, supplier_id, order_tick,
# MAGIC          expected_arrival_tick, order_qty, status
# MAGIC   FROM hackathon_of_the_century.tables4ops.ops_pending_orders
# MAGIC   WHERE sim_id = p_sim_id AND item_id = p_item_id AND status = 'pending';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_supplier_info(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   supplier_id           STRING,
# MAGIC   supplier_name         STRING,
# MAGIC   base_lead_time_ticks  INT,
# MAGIC   lead_time_variability DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns supplier details for one item in one sim run.'
# MAGIC RETURN
# MAGIC   SELECT s.supplier_id, s.supplier_name,
# MAGIC          s.base_lead_time_ticks, s.lead_time_variability
# MAGIC   FROM hackathon_of_the_century.tables4env.env_suppliers s
# MAGIC   INNER JOIN hackathon_of_the_century.tables4env.env_supplier_item_map m
# MAGIC     ON s.supplier_id = m.supplier_id
# MAGIC   WHERE m.sim_id = p_sim_id AND m.item_id = p_item_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_cost_snapshot(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   tick                         INT,
# MAGIC   cumulative_holding_cost      DOUBLE,
# MAGIC   cumulative_stockout_cost     DOUBLE,
# MAGIC   cumulative_order_cost        DOUBLE,
# MAGIC   cumulative_transit_loss_cost DOUBLE,
# MAGIC   cumulative_total_cost        DOUBLE,
# MAGIC   remaining_budget             DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns the latest cost accumulator row for one item.'
# MAGIC RETURN
# MAGIC   SELECT ca.tick, ca.cumulative_holding_cost, ca.cumulative_stockout_cost,
# MAGIC          ca.cumulative_order_cost, ca.cumulative_transit_loss_cost,
# MAGIC          ca.cumulative_total_cost, ca.remaining_budget
# MAGIC   FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator ca
# MAGIC   INNER JOIN (
# MAGIC     SELECT MAX(tick) AS max_tick
# MAGIC     FROM hackathon_of_the_century.tables4ops.ops_cost_accumulator
# MAGIC     WHERE sim_id = p_sim_id AND item_id = p_item_id
# MAGIC   ) latest ON ca.tick = latest.max_tick
# MAGIC   WHERE ca.sim_id = p_sim_id AND ca.item_id = p_item_id;

# COMMAND ----------

# %sql
# CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_active_disruptions(
#   p_sim_id  STRING COMMENT 'Simulation run ID',
#   p_item_id STRING COMMENT 'Item type ID'
# )
# RETURNS TABLE (
#   disruption_id       STRING,
#   disruption_type     STRING,
#   effective_magnitude DOUBLE,
#   is_active_this_tick BOOLEAN
# )
# COMMENT 'Returns all active disruptions for one item in one sim run.'
# RETURN
#   SELECT disruption_id, disruption_type, effective_magnitude,
#          is_active_this_tick
#   FROM hackathon_of_the_century.tables4ops.ops_active_disruptions
#   WHERE sim_id     = p_sim_id
#     AND item_id    = p_item_id
#     AND is_active_this_tick = true;

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS hackathon_of_the_century.agent_tools.get_active_disruptions;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_active_disruptions(
# MAGIC   p_sim_id  STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id STRING COMMENT 'Item type ID'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   disruption_id       STRING,
# MAGIC   disruption_type     STRING,
# MAGIC   effective_magnitude DOUBLE,
# MAGIC   is_active_this_tick BOOLEAN,
# MAGIC   start_tick          INT,
# MAGIC   end_tick            INT
# MAGIC )
# MAGIC COMMENT 'Returns all active disruptions for one item in one sim run.'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC     ad.disruption_id,
# MAGIC     ad.disruption_type,
# MAGIC     ad.effective_magnitude,
# MAGIC     ad.is_active_this_tick,
# MAGIC     ds.start_tick,
# MAGIC     ds.end_tick
# MAGIC   FROM hackathon_of_the_century.tables4ops.ops_active_disruptions ad
# MAGIC   INNER JOIN hackathon_of_the_century.tables4env.env_disruption_schedule ds
# MAGIC     ON  ad.disruption_id = ds.disruption_id
# MAGIC     AND ad.sim_id        = ds.sim_id
# MAGIC   WHERE ad.sim_id            = p_sim_id
# MAGIC     AND ad.item_id           = p_item_id
# MAGIC     AND ad.is_active_this_tick = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS hackathon_of_the_century.agent_tools.log_agent_decision;

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.log_agent_decision(
# MAGIC   p_sim_id     STRING COMMENT 'Simulation run ID',
# MAGIC   p_tick       INT    COMMENT 'Current simulation tick',
# MAGIC   p_item_id    STRING COMMENT 'Item the decision covers',
# MAGIC   p_decision   STRING COMMENT 'reorder or hold',
# MAGIC   p_order_qty  INT    COMMENT 'Units ordered; 0 for hold',
# MAGIC   p_rationale  STRING COMMENT 'Full LLM reasoning text',
# MAGIC   p_confidence DOUBLE COMMENT 'Agent self-reported confidence 0.0-1.0',
# MAGIC   p_agent_ver  STRING COMMENT 'Agent version string'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   sim_id                       STRING,
# MAGIC   tick                         INT,
# MAGIC   item_id                      STRING,
# MAGIC   supplier_id                  STRING,
# MAGIC   stock_on_hand_at_decision    INT,
# MAGIC   stock_in_transit_at_decision INT,
# MAGIC   decision                     STRING,
# MAGIC   order_qty                    INT,
# MAGIC   order_id                     STRING,
# MAGIC   agent_reasoning              STRING,
# MAGIC   agent_version                STRING
# MAGIC )
# MAGIC COMMENT 'Builds a validated decision-log row for insertion into hist_reorder_decisions. Caller performs INSERT INTO ... SELECT from this function.'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC     p_sim_id AS sim_id,
# MAGIC     p_tick AS tick,
# MAGIC     inv.item_id AS item_id,
# MAGIC     'LLM_AGENT' AS supplier_id,
# MAGIC     inv.stock_on_hand AS stock_on_hand_at_decision,
# MAGIC     inv.stock_in_transit AS stock_in_transit_at_decision,
# MAGIC     CASE
# MAGIC       WHEN lower(p_decision) IN ('reorder', 'hold') THEN lower(p_decision)
# MAGIC       ELSE raise_error('p_decision must be reorder or hold')
# MAGIC     END AS decision,
# MAGIC     CASE
# MAGIC       WHEN lower(p_decision) = 'hold' THEN 0
# MAGIC       WHEN p_order_qty IS NULL OR p_order_qty < 0 THEN raise_error('p_order_qty must be >= 0')
# MAGIC       ELSE p_order_qty
# MAGIC     END AS order_qty,
# MAGIC     CAST(NULL AS STRING) AS order_id,
# MAGIC     CONCAT(
# MAGIC       COALESCE(p_rationale, ''),
# MAGIC       '\n\n[agent_meta] confidence=',
# MAGIC       COALESCE(CAST(p_confidence AS STRING), 'null')
# MAGIC     ) AS agent_reasoning,
# MAGIC     p_agent_ver AS agent_version
# MAGIC   FROM hackathon_of_the_century.agent_tools.get_inventory_state(p_sim_id, p_item_id) inv;

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS hackathon_of_the_century.agent_tools.escalate_item;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.escalate_item(
# MAGIC   p_sim_id       STRING COMMENT 'Simulation run ID',
# MAGIC   p_tick         INT    COMMENT 'Current simulation tick',
# MAGIC   p_item_id      STRING COMMENT 'Item requiring escalation',
# MAGIC   p_reason       STRING COMMENT 'BUDGET_BREACH | NO_SUPPLIER | STOCKOUT_IMMINENT | OTHER',
# MAGIC   p_context_json STRING COMMENT 'JSON snapshot of relevant context fields'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   sim_id       STRING,
# MAGIC   tick         INT,
# MAGIC   item_id      STRING,
# MAGIC   reason       STRING,
# MAGIC   context_json STRING,
# MAGIC   status       STRING,
# MAGIC   raised_at    TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Builds a validated escalation row for insertion into ops_escalation_queue. Caller performs INSERT INTO ... SELECT from this function.'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC     p_sim_id AS sim_id,
# MAGIC     p_tick AS tick,
# MAGIC     p_item_id AS item_id,
# MAGIC     CASE
# MAGIC       WHEN upper(p_reason) IN ('BUDGET_BREACH', 'NO_SUPPLIER', 'STOCKOUT_IMMINENT', 'OTHER') THEN upper(p_reason)
# MAGIC       ELSE raise_error('p_reason must be one of BUDGET_BREACH, NO_SUPPLIER, STOCKOUT_IMMINENT, OTHER')
# MAGIC     END AS reason,
# MAGIC     p_context_json AS context_json,
# MAGIC     'OPEN' AS status,
# MAGIC     current_timestamp() AS raised_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hackathon_of_the_century.agent_tools.get_full_context(
# MAGIC   p_sim_id      STRING COMMENT 'Simulation run ID',
# MAGIC   p_item_id     STRING COMMENT 'Item type ID',
# MAGIC   p_history_ticks INT  COMMENT 'Number of demand history ticks to summarise'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   sim_id                    STRING,
# MAGIC   item_id                   STRING,
# MAGIC   latest_observed_tick      INT,
# MAGIC   stock_on_hand             INT,
# MAGIC   stock_in_transit          INT,
# MAGIC   expected_arrivals_next_tick INT,
# MAGIC   supplier_id               STRING,
# MAGIC   supplier_name             STRING,
# MAGIC   base_lead_time_ticks      INT,
# MAGIC   lead_time_variability     DOUBLE,
# MAGIC   cumulative_total_cost     DOUBLE,
# MAGIC   remaining_budget          DOUBLE,
# MAGIC   demand_points             LONG,
# MAGIC   avg_raw_demand            DOUBLE,
# MAGIC   avg_disrupted_demand      DOUBLE,
# MAGIC   fulfilled_units           LONG,
# MAGIC   unmet_units               LONG,
# MAGIC   latest_rolling_avg_7      DOUBLE,
# MAGIC   pending_order_count       LONG,
# MAGIC   pending_units             LONG,
# MAGIC   next_expected_arrival_tick INT,
# MAGIC   active_disruption_count   LONG,
# MAGIC   max_effective_magnitude   DOUBLE,
# MAGIC   disruption_types          STRING
# MAGIC )
# MAGIC COMMENT 'Single-call context assembly joining all UC read functions for one item. Use this first before calling individual tools.'
# MAGIC RETURN
# MAGIC   WITH inventory AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_inventory_state(p_sim_id, p_item_id)
# MAGIC   ),
# MAGIC   demand AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_demand_history(p_sim_id, p_item_id, p_history_ticks)
# MAGIC   ),
# MAGIC   pending AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_pending_orders(p_sim_id, p_item_id)
# MAGIC   ),
# MAGIC   supplier AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_supplier_info(p_sim_id, p_item_id)
# MAGIC   ),
# MAGIC   costs AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_cost_snapshot(p_sim_id, p_item_id)
# MAGIC   ),
# MAGIC   disruptions AS (
# MAGIC     SELECT * FROM hackathon_of_the_century.agent_tools.get_active_disruptions(p_sim_id, p_item_id)
# MAGIC   ),
# MAGIC   demand_summary AS (
# MAGIC     SELECT
# MAGIC       COUNT(*)                  AS demand_points,
# MAGIC       AVG(raw_demand)           AS avg_raw_demand,
# MAGIC       AVG(disrupted_demand)     AS avg_disrupted_demand,
# MAGIC       SUM(fulfilled_demand)     AS fulfilled_units,
# MAGIC       SUM(unmet_demand)         AS unmet_units,
# MAGIC       MAX(rolling_avg_7)        AS latest_rolling_avg_7
# MAGIC     FROM demand
# MAGIC   ),
# MAGIC   pending_summary AS (
# MAGIC     SELECT
# MAGIC       COUNT(*)                        AS pending_order_count,
# MAGIC       COALESCE(SUM(order_qty), 0)     AS pending_units,
# MAGIC       MIN(expected_arrival_tick)      AS next_expected_arrival_tick
# MAGIC     FROM pending
# MAGIC   ),
# MAGIC   disruption_summary AS (
# MAGIC     SELECT
# MAGIC       COUNT(*)                                              AS active_disruption_count,
# MAGIC       MAX(effective_magnitude)                              AS max_effective_magnitude,
# MAGIC       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(disruption_type))) AS disruption_types
# MAGIC     FROM disruptions
# MAGIC   )
# MAGIC   SELECT
# MAGIC     p_sim_id                          AS sim_id,
# MAGIC     p_item_id                         AS item_id,
# MAGIC     inv.tick                          AS latest_observed_tick,
# MAGIC     inv.stock_on_hand,
# MAGIC     inv.stock_in_transit,
# MAGIC     inv.expected_arrivals_next_tick,
# MAGIC     sup.supplier_id,
# MAGIC     sup.supplier_name,
# MAGIC     sup.base_lead_time_ticks,
# MAGIC     sup.lead_time_variability,
# MAGIC     cost.cumulative_total_cost,
# MAGIC     cost.remaining_budget,
# MAGIC     ds.demand_points,
# MAGIC     ds.avg_raw_demand,
# MAGIC     ds.avg_disrupted_demand,
# MAGIC     ds.fulfilled_units,
# MAGIC     ds.unmet_units,
# MAGIC     ds.latest_rolling_avg_7,
# MAGIC     ps.pending_order_count,
# MAGIC     ps.pending_units,
# MAGIC     ps.next_expected_arrival_tick,
# MAGIC     dis.active_disruption_count,
# MAGIC     dis.max_effective_magnitude,
# MAGIC     dis.disruption_types
# MAGIC   FROM inventory inv
# MAGIC   CROSS JOIN supplier sup
# MAGIC   CROSS JOIN costs cost
# MAGIC   CROSS JOIN demand_summary ds
# MAGIC   CROSS JOIN pending_summary ps
# MAGIC   CROSS JOIN disruption_summary dis;

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %sql
# MAGIC -- Read functions
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.get_inventory_state('sim_stage4_001', 'item_A');
# MAGIC
# MAGIC -- SELECT * FROM hackathon_of_the_century.agent_tools.get_pending_orders('sim_stage4_001', 'item_A');
# MAGIC -- SELECT * FROM hackathon_of_the_century.agent_tools.get_supplier_info('sim_stage4_001', 'item_A');
# MAGIC -- SELECT * FROM hackathon_of_the_century.agent_tools.get_cost_snapshot('sim_stage4_001', 'item_A');
# MAGIC -- SELECT * FROM hackathon_of_the_century.agent_tools.get_active_disruptions('sim_stage4_001', 'item_A');
# MAGIC
# MAGIC -- Write pattern for UC-supported decision logging
# MAGIC -- INSERT INTO hackathon_of_the_century.tables4hist.hist_reorder_decisions
# MAGIC -- SELECT *
# MAGIC -- FROM hackathon_of_the_century.agent_tools.log_agent_decision(
# MAGIC --   'sim_stage4_001', 9999, 'item_A', 'hold', 0,
# MAGIC --   'Smoke test rationale', 0.9, 'smoke_test_v1'
# MAGIC -- );
# MAGIC
# MAGIC -- Verify writes landed
# MAGIC -- SELECT *
# MAGIC -- FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
# MAGIC -- WHERE sim_id = 'sim_stage4_001' AND tick = 9999 AND agent_version = 'smoke_test_v1'
# MAGIC -- ORDER BY tick DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.get_demand_history('sim_stage4_001', 'item_A', 5);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.get_supplier_info('sim_stage4_001', 'item_A');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.get_cost_snapshot('sim_stage4_001', 'item_A');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.get_active_disruptions('sim_stage4_001', 'item_A');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO hackathon_of_the_century.tables4hist.hist_reorder_decisions
# MAGIC SELECT * FROM hackathon_of_the_century.agent_tools.log_agent_decision(
# MAGIC   'sim_stage4_001', 9999, 'item_A', 'hold', 0,
# MAGIC   'Smoke test rationale', 0.9, 'smoke_test_v2'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
# MAGIC WHERE sim_id = 'sim_stage4_001' AND tick = 9999 AND agent_version = 'smoke_test_v2';

# COMMAND ----------

# DBTITLE 1,Cell 19
# %sql
# INSERT INTO hackathon_of_the_century.tables4hist.hist_reorder_decisions
# SELECT *
# FROM hackathon_of_the_century.agent_tools.log_agent_decision(
#   'sim_stage4_001', 9999, 'item_A', 'hold', 0,
#   'Smoke test rationale', 0.9, 'smoke_test_v1'
# );

# COMMAND ----------

# DBTITLE 1,Cell 20
# %sql
# SELECT *
# FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
# WHERE sim_id = 'sim_stage4_001' AND tick = 9999 AND agent_version = 'smoke_test_v1'
# ORDER BY tick DESC;

# COMMAND ----------

# DBTITLE 1,Escalation smoke insert
# MAGIC %sql
# MAGIC INSERT INTO hackathon_of_the_century.tables4ops.ops_escalation_queue
# MAGIC SELECT *
# MAGIC FROM hackathon_of_the_century.agent_tools.escalate_item(
# MAGIC   'sim_stage4_001',
# MAGIC   9999,
# MAGIC   'item_A',
# MAGIC   'STOCKOUT_IMMINENT',
# MAGIC   '{"source":"smoke_test","confidence":0.92,"note":"Escalation smoke test"}'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Escalation smoke verify
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hackathon_of_the_century.tables4ops.ops_escalation_queue
# MAGIC WHERE sim_id = 'sim_stage4_001'
# MAGIC   AND tick = 9999
# MAGIC   AND item_id = 'item_A'
# MAGIC   AND reason = 'STOCKOUT_IMMINENT'
# MAGIC ORDER BY raised_at DESC;