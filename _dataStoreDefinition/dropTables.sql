-- Environment tables
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_sim_config;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_item_types;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_suppliers;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_supplier_item_map;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_consumers;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_consumer_item_map;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_patterns;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4env.env_disruption_schedule;

-- Operational tables
DROP TABLE IF EXISTS hackathon_of_the_century.tables4ops.ops_warehouse_state;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4ops.ops_pending_orders;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4ops.ops_cost_accumulator;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4ops.ops_active_disruptions;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4ops.ops_escalation_queue;

-- Historical tables
DROP TABLE IF EXISTS hackathon_of_the_century.tables4hist.hist_demand_actuals;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4hist.hist_supply_arrivals;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4hist.hist_reorder_decisions;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4hist.hist_cost_by_tick;
DROP TABLE IF EXISTS hackathon_of_the_century.tables4hist.hist_eval_metrics;

-- Event log
DROP TABLE IF EXISTS hackathon_of_the_century.tables4eventlog.event_log;