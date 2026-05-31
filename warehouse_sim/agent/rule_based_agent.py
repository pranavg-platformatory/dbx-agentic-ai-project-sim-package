from __future__ import annotations

from warehouse_sim.agent.base import AgentContext, BaseAgent, ReorderDecision


class RuleBasedAgent(BaseAgent):
    '''
    Rule-based reorder agent.

    Decision rule (per item, per tick):
    - Reorder if `stock_on_hand` < `reorder_point` AND remaining budget covers the minimum order cost
    - Quantity: `min_order_qty`, capped at `max_order_qty` (always satisfied since `min_order_qty` <= `max_order_qty` by schema constraint)
    - Hold otherwise

    This agent is deterministic and introduces no stochastic draws of its own, preserving simulation reproducibility (FR-07, __docs__/simulationSpecs.md).

    Used as:
    1. Standalone baseline agent.
    2. Fallback within the LLMAgentWrapper when the LLM response is structurally or logically invalid (FALLBACK_STRUCTURAL / FALLBACK_LOGICAL paths)
    '''

    @staticmethod
    def agent_version() -> str:
        return "rule_based_v1"

    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        decisions: list[ReorderDecision] = []

        for item_id, item_state in context.item_states.items():
            if item_state.stock_on_hand < item_state.reorder_point:
                order_qty = item_state.min_order_qty
                decisions.append(
                    ReorderDecision(
                        item_id=item_id,
                        order_qty=order_qty,
                        reasoning=(
                            f"stock_on_hand ({item_state.stock_on_hand}) "
                            f"< reorder_point ({item_state.reorder_point}); "
                            f"ordering min_order_qty ({order_qty})"
                        ),
                    )
                )
            else:
                decisions.append(
                    ReorderDecision(
                        item_id=item_id,
                        order_qty=0,
                        reasoning=(
                            f"stock_on_hand ({item_state.stock_on_hand}) "
                            f">= reorder_point ({item_state.reorder_point}); "
                            f"holding"
                        ),
                    )
                )

        return decisions