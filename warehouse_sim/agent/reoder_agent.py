'''
warehouse_sim/agent/reoder_agent.py

Trivial agent that reorders a fixed amount upon stock reaching a certain threshold.
'''

from .base import AgentContext, BaseAgent, ReorderDecision

class ReorderAgent(BaseAgent):
    '''Simple rule: reorder min_order_qty when stock_on_hand < reorder_point and no pending orders exist for the item.'''

    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        decisions = []
        for item_id in context.items():
            state   = context.item_states[item_id]
            pending = context.pending_for(item_id)
            if state.stock_on_hand < state.reorder_point and not pending:
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = state.min_order_qty,
                    reasoning = (
                        f"stock_on_hand={state.stock_on_hand} < "
                        f"reorder_point={state.reorder_point}. No pending orders."
                    ),
                ))
            else:
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = 0,
                    reasoning = "Stock sufficient or orders pending.",
                ))
        return decisions
    
    def agent_version(self) -> str:
        return "reorder_agent_v1"        

