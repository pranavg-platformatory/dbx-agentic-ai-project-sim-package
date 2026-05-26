'''
warehouse_sim/agent/llm_agent_wrapper_types.py

Internal dataclasses shared between the following:
- The LLMAgentWrapper monitoring loop (llm_agent_wrapper.py, sync side)
- The executor thread (llm_agent_wrapper.py, async side)

---

NOTE:
- These dataclasses are kept in a separate module so that both sides can import from one place without either importing from the other, avoiding circular dependencies
- These types are LLMAgentWrapper internals - they are not part of the BaseAgent contract and should not be imported outside of the warehouse_sim/agent/llm_agent_wrapper* files
'''

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .base import AgentContext, ReorderDecision


@dataclass
class QueueMessage:
    '''
    A single message placed on the monitoring queue each tick.

    Produced by the sync monitoring loop; consumed by the executor thread.

    ---

    # Fields:
    
    - `trigger_tick` (int): The simulation tick at which this context was assembled
    - `trigger_condition_met` (bool): Whether the executor trigger condition was met this tick
        - Included in the message (rather than evaluated by the executor)
        - This is so that the queue accurately reflects the monitoring loop's view at assembly time, independent of when the executor drains it
    - `assembly_timestamp` (datetime): Wall-clock time at which this message was assembled
        - Used by the executor to compute elapsed wall-clock time since assembly, if needed for diagnostics
        - Not a simulation time - use trigger_tick for simulation-time ordering
    - `obsolescence_threshold` (int): The value of K at the time this message was assembled, copied from LLMAgentWrapperConfig (warehouse_sim/config/llm_agent_wrapper_config.py) (or its resolved default) \n
      NOTE:
        - This threshold is carried in the message so the executor can evaluate staleness without needing access to LLMAgentWrapperConfig directly.
        - The context assembled at trigger_tick T is stale at current tick C if: C - T > `obsolescence_threshold`
    - `context` (AgentContext): The full AgentContext snapshot assembled at `trigger_tick` \n
      NOTE: The agent must not mutate it (same constraint as in the runner (warehouse_sim/engine/runner.py))
    - `sim_id` (str): Copied from `context.sim_id` for convenience \n
      NOTE: Her Majesty Reshma the Boss's LangFuse instrumentation attaches this to traces on the executor side; including it at the envelope level means she does not need to unpack the context to get it
    '''

    trigger_tick:           int
    trigger_condition_met:  bool
    assembly_timestamp:     datetime
    obsolescence_threshold: int
    context:                AgentContext
    sim_id:                 str
@dataclass
class ExecutorResult:
    '''
    The result written by the executor thread to the shared result slot (`_result_slot`) on LLMAgentWrapper.

    Read by the sync side of decide() at the top of the next tick after the executor completes.

    ---

    # Fields

    - `decisions` (list[ReorderDecision]): The list of ReorderDecision objects to be returned to the runner
        - Always one decision per item, either from the LLM response or from the RuleBasedAgent fallback
        - Pre-flight validated before being written here - the runner's `_validate_decisions` will not see an invalid result
    - `produced_at_tick` (int): The `trigger_tick` of the QueueMessage the executor acted on \n
      NOTE: Used to log which tick's context produced this result, independent of which tick the sync side consumes it on
    - `fallback_used`: True if the RuleBasedAgent was substituted for the LLM response
    - `fallback_type` (str): The specific fallback reason, matching the event_log event_type that was fired:
        - "FALLBACK_STRUCTURAL": LLM response failed to parse
        - "FALLBACK_LOGICAL": LLM response was parsed but logically invalid
        - None: No fallback; LLM response was used as-is
    '''

    decisions:        list[ReorderDecision]
    produced_at_tick: int
    fallback_used:    bool
    fallback_type:    str | None # "FALLBACK_STRUCTURAL" | "FALLBACK_LOGICAL" | None