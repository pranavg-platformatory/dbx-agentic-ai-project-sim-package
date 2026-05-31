'''
agent_model.py
/Workspace/Shared/reorder-llm-agent/agent_model.py

MLflow code-based model definition for the LLM reorder agent.

This file is referenced by mlflow.pyfunc.log_model(python_model=...).
MLflow loads this file at serve time and calls predict().

Per MLflow 3.x code-based logging requirements:
- The model class must be defined in this file
- mlflow.models.set_model() must be called at the bottom
- No instantiated objects are passed to log_model()
'''

import sys
import os
import json
import pandas as pd
import mlflow

# Path setup - works both in notebook and serving container
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)


class ReorderAgentModel(mlflow.pyfunc.PythonModel):
    '''
    MLflow PyFunc model wrapping LLMReorderAgent.

    predict() accepts a dict payload representing an AgentContext and returns a list of ReorderDecision dicts.

    This is the serving interface - the underlying agent code is unchanged.
    '''
    
    def load_context(self, context):
        '''Called once when the model is loaded for serving.'''
        import sys

        # In serving container, artifacts are at context.artifacts['agent_dir']
        # This is the reliable path regardless of environment
        agent_dir = context.artifacts.get('agent_dir', '')
        if agent_dir and agent_dir not in sys.path:
            sys.path.insert(0, agent_dir)

        # Fallback to __file__ directory for notebook execution
        this_dir = os.path.dirname(os.path.abspath(__file__))
        if this_dir not in sys.path:
            sys.path.insert(0, this_dir)

        from llm_agent import LLMReorderAgent
        self.agent = LLMReorderAgent()

    def predict(self, context, model_input, params=None):
        '''
        model_input : dict or single-row DataFrame with AgentContext fields
        Returns     : list of dicts with item_id, decision, order_qty, reasoning
        '''
        from base import (
            AgentContext, ItemState, PendingOrder,
            DemandRecord, ActiveDisruption, CostSnapshot,
        )

        # Accept dict or single-row DataFrame
        if isinstance(model_input, pd.DataFrame):
            payload = model_input.iloc[0].to_dict()
        elif isinstance(model_input, list) and len(model_input) > 0:
            payload = model_input[0]
        else:
            payload = model_input

        # Parse fields - all complex fields arrive as JSON strings
        # when called via REST endpoint
        def _parse(val):
            if isinstance(val, str):
                return json.loads(val)
            return val

        sim_id           = payload.get('sim_id', 'unknown')
        tick             = int(payload.get('tick', 0))
        remaining_budget = payload.get('remaining_budget', None)
        if remaining_budget is not None:
            remaining_budget = float(remaining_budget)

        # Item states
        item_states_raw = _parse(payload.get('item_states', '{}'))
        item_states = {
            k: ItemState(**v) for k, v in item_states_raw.items()
        }

        # Demand history
        demand_history_raw = _parse(payload.get('demand_history', '{}'))
        demand_history = {
            k: [DemandRecord(**r) for r in v]
            for k, v in demand_history_raw.items()
        }

        # Pending orders
        pending_orders = [
            PendingOrder(**o)
            for o in _parse(payload.get('pending_orders', '[]'))
        ]

        # Active disruptions
        active_disruptions = [
            ActiveDisruption(**d)
            for d in _parse(payload.get('active_disruptions', '[]'))
        ]

        # Cost snapshots
        cost_snapshots = {
            k: CostSnapshot(**v)
            for k, v in _parse(payload.get('cost_snapshots', '{}')).items()
        }

        agent_context = AgentContext(
            sim_id             = sim_id,
            tick               = tick,
            item_states        = item_states,
            pending_orders     = pending_orders,
            demand_history     = demand_history,
            active_disruptions = active_disruptions,
            cost_snapshots     = cost_snapshots,
            remaining_budget   = remaining_budget,
        )

        decisions = self.agent.decide(agent_context)

        return [
            {
                'item_id'   : d.item_id,
                'decision'  : 'reorder' if d.is_reorder else 'hold',
                'order_qty' : d.order_qty,
                'reasoning' : d.reasoning or '',
            }
            for d in decisions
        ]


# Required by MLflow 3.x code-based logging
# This line tells MLflow which class is the model entry point
mlflow.models.set_model(ReorderAgentModel())