'''
warehouse_sim/agent/hold_agent.py

Trivial agent that does nothing.
'''

from .base import AgentContext, BaseAgent, ReorderDecision

class HoldAgent(BaseAgent):
    '''Always holds - useful for verifying engine runs without orders.'''
    
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        return [ReorderDecision(item_id=i, order_qty=0, reasoning="Always hold.")
                for i in context.items()]
    def agent_version(self) -> str:
        return "hold_agent_v1"
