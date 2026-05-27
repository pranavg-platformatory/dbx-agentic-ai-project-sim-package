'''
llm_agent.py
/Workspace/Shared/reorder-llm-agent/llm_agent.py

LLM-powered reorder agent — Task 3: real LangGraph tool-calling loop.

Changes from Task 1:
- _llm_decide() replaces _parse_stub_decisions()
- LangGraph StateGraph with agent + tools nodes
- UCFunctionToolkit replaced by local uc_tools.py wrappers
  (more portable — no dependency on databricks-langchain toolkit)
- Structured JSON output parsed back to list[ReorderDecision]
- MLflow autolog added (Task 4 will extend this further)

Plugin contract is unchanged:
- Extends BaseAgent
- decide(context) -> list[ReorderDecision]
- Never writes to Delta directly
- Never mutates AgentContext
'''

# from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional, Any, Annotated, TypedDict
from langgraph.graph.message import add_messages

import yaml
import mlflow
import mlflow.langchain
# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_AGENT_DIR = Path(__file__).resolve().parent
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))

from base import (
    AgentContext,
    BaseAgent,
    ReorderDecision,
    ItemState,
    PendingOrder,
    DemandRecord,
    ActiveDisruption,
    CostSnapshot,
)
from uc_tools import ALL_TOOLS


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).resolve().parent / 'config.yml'
    if not config_path.exists():
        raise FileNotFoundError(
            f'Agent config not found at: {config_path}\n'
            f'Expected config.yml alongside llm_agent.py in '
            f'/Workspace/Shared/reorder-llm-agent/'
        )
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Prompt serialiser (unchanged from Task 1)
# ---------------------------------------------------------------------------

def serialise_context(
    context:        AgentContext,
    history_window: int = 10,
) -> str:
    lines: list[str] = []

    lines.append(f'SIM_ID: {context.sim_id}')
    lines.append(f'TICK:   {context.tick}')

    if context.remaining_budget is not None:
        lines.append(f'REMAINING_BUDGET: {context.remaining_budget:.2f}')
    else:
        lines.append('REMAINING_BUDGET: unlimited')

    lines.append('')

    if context.active_disruptions:
        lines.append('ACTIVE_DISRUPTIONS:')
        for d in context.active_disruptions:
            lines.append(
                f'  - item={d.item_id} '
                f'type={d.disruption_type} '
                f'magnitude={d.effective_magnitude:.3f}'
            )
    else:
        lines.append('ACTIVE_DISRUPTIONS: none')

    lines.append('')
    lines.append('ITEMS:')

    for item_id in context.items():
        state   = context.item_states[item_id]
        pending = context.pending_for(item_id)
        history = context.history_for(item_id)
        disrupt = context.disruptions_for(item_id)
        costs   = context.cost_snapshots.get(item_id)

        lines.append(f'  [{item_id}]')
        lines.append(f'    stock_on_hand:               {state.stock_on_hand}')
        lines.append(f'    stock_in_transit:            {state.stock_in_transit}')
        lines.append(f'    expected_arrivals_next_tick: {state.expected_arrivals_next_tick}')
        lines.append(f'    reorder_point:               {state.reorder_point}')
        lines.append(f'    min_order_qty:               {state.min_order_qty}')
        lines.append(f'    max_order_qty:               {state.max_order_qty}')

        recent = history[-history_window:] if history else []
        if recent:
            fulfilled_vals = [r.fulfilled for r in recent]
            unmet_vals     = [r.unmet     for r in recent]
            avg_fulfilled  = sum(fulfilled_vals) / len(fulfilled_vals)
            total_unmet    = sum(unmet_vals)
            lines.append(
                f'    demand_history_last_{len(recent)}_ticks: '
                f'fulfilled={fulfilled_vals} unmet={unmet_vals}'
            )
            lines.append(f'    rolling_avg_fulfilled:       {avg_fulfilled:.2f}')
            lines.append(f'    total_unmet_in_window:       {total_unmet}')
            if avg_fulfilled > 0:
                dts = state.stock_on_hand / avg_fulfilled
                lines.append(f'    est_ticks_to_stockout:       {dts:.1f}')
            else:
                lines.append('    est_ticks_to_stockout:       N/A (zero avg demand)')
        else:
            lines.append('    demand_history:              no history available yet')

        if pending:
            lines.append(f'    pending_orders ({len(pending)}):')
            for o in pending:
                lines.append(
                    f'      order_id={o.order_id} qty={o.order_qty} '
                    f'arrives_tick={o.expected_arrival_tick} '
                    f'supplier={o.supplier_id}'
                )
        else:
            lines.append('    pending_orders:              none')

        if disrupt:
            lines.append(f'    item_disruptions ({len(disrupt)}):')
            for d in disrupt:
                lines.append(
                    f'      type={d.disruption_type} '
                    f'magnitude={d.effective_magnitude:.3f}'
                )
        else:
            lines.append('    item_disruptions:            none')

        if costs:
            lines.append(
                f'    cumulative_costs: '
                f'holding={costs.cumulative_holding_cost:.2f} '
                f'stockout={costs.cumulative_stockout_cost:.2f} '
                f'order={costs.cumulative_order_cost:.2f} '
                f'total={costs.cumulative_total_cost:.2f}'
            )

        lines.append('')

    lines.append(
        'Analyse each item using the steps in your system prompt. '
        'Call tools as needed. '
        'Return the JSON decision array when done.'
    )

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# JSON output parser
# ---------------------------------------------------------------------------

def _parse_llm_decisions(
    raw_output: str,
    context:    AgentContext,
    agent_ver:  str,
) -> list[ReorderDecision]:
    '''
    Parse the LLM's JSON array output into ReorderDecision objects.

    Handles:
    - Clean JSON arrays
    - JSON wrapped in markdown code fences
    - Partial output with recoverable items
    - Fallback to HOLD for any item missing from the output

    PARAMETERS:
    - raw_output : the full string output from the LLM
    - context    : AgentContext (used to fill in missing items as HOLD)
    - agent_ver  : agent version string for logging

    RETURNS:
    - list[ReorderDecision] — exactly one per item in context.items()
    '''
    # Strip markdown fences if present
    cleaned = re.sub(r'```(?:json)?', '', raw_output).strip()

    # Extract JSON array — find first [ ... ] block
    match = re.search(r'\[.*\]', cleaned, re.DOTALL)
    if not match:
        print(
            f'[LLMReorderAgent] WARNING: could not find JSON array in output. '
            f'Falling back to HOLD for all items.\n'
            f'Raw output: {raw_output[:500]}'
        )
        return _fallback_hold(context, reason='no JSON array in LLM output')

    try:
        parsed = json.loads(match.group())
    except json.JSONDecodeError as e:
        print(
            f'[LLMReorderAgent] WARNING: JSON parse error: {e}. '
            f'Falling back to HOLD for all items.'
        )
        return _fallback_hold(context, reason=f'JSON parse error: {e}')

    # Build a lookup by item_id
    decision_map: dict[str, dict] = {}
    for item in parsed:
        if isinstance(item, dict) and 'item_id' in item:
            decision_map[item['item_id']] = item

    decisions: list[ReorderDecision] = []
    for item_id in context.items():
        state = context.item_states[item_id]

        if item_id not in decision_map:
            print(
                f'[LLMReorderAgent] WARNING: no decision for {item_id} '
                f'in LLM output. Defaulting to HOLD.'
            )
            decisions.append(ReorderDecision(
                item_id   = item_id,
                order_qty = 0,
                reasoning = '[FALLBACK] item missing from LLM output — holding',
            ))
            continue

        raw = decision_map[item_id]
        decision  = str(raw.get('decision', 'hold')).lower()
        order_qty = int(raw.get('order_qty', 0))
        reasoning = str(raw.get('reasoning', ''))
        confidence = float(raw.get('confidence', 0.0))

        # Enforce hard bounds from context
        if decision == 'reorder':
            if order_qty < state.min_order_qty:
                print(
                    f'[LLMReorderAgent] WARNING: {item_id} order_qty '
                    f'{order_qty} below min {state.min_order_qty}. '
                    f'Clamping up.'
                )
                order_qty = state.min_order_qty
            if order_qty > state.max_order_qty:
                print(
                    f'[LLMReorderAgent] WARNING: {item_id} order_qty '
                    f'{order_qty} above max {state.max_order_qty}. '
                    f'Clamping down.'
                )
                order_qty = state.max_order_qty
        else:
            order_qty = 0

        decisions.append(ReorderDecision(
            item_id   = item_id,
            order_qty = order_qty,
            reasoning = reasoning,
        ))

    return decisions


def _fallback_hold(
    context: AgentContext,
    reason:  str = 'LLM error',
) -> list[ReorderDecision]:
    return [
        ReorderDecision(
            item_id   = item_id,
            order_qty = 0,
            reasoning = f'[FALLBACK] {reason}',
        )
        for item_id in context.items()
    ]


# ---------------------------------------------------------------------------
# LangGraph agentic loop
# ---------------------------------------------------------------------------

def _build_graph(llm_with_tools: Any):
    '''
    Builds a LangGraph StateGraph with two nodes:
    - agent : calls the LLM
    - tools : executes tool calls returned by the LLM

    The loop continues until the LLM returns a message with no
    tool calls (i.e. it has finished reasoning and returned the
    JSON decision array).

    Mirrors the Investment Assistant pattern from the reference
    codebase (agent.py create_tool_calling_agent).
    '''
    from langgraph.graph import StateGraph, END
    from langchain_core.messages import ToolMessage

    class AgentState(TypedDict):
        messages: Annotated[list, add_messages]

    # Tool executor map
    tool_map = {t.name: t for t in ALL_TOOLS}

    def agent_node(state: AgentState) -> AgentState:
        response = llm_with_tools.invoke(state['messages'])
        return {'messages': [response]}

    def tools_node(state: AgentState) -> AgentState:
        last = state['messages'][-1]
        tool_messages = []
        for tc in last.tool_calls:
            tool_fn = tool_map.get(tc['name'])
            if tool_fn is None:
                result = f'[ERROR] unknown tool: {tc["name"]}'
            else:
                try:
                    result = tool_fn.invoke(tc['args'])
                except Exception as e:
                    result = f'[ERROR] tool {tc["name"]} failed: {e}'
            tool_messages.append(
                ToolMessage(content=str(result), tool_call_id=tc['id'])
            )
        return {'messages': tool_messages}

    def should_continue(state: AgentState) -> str:
        last = state['messages'][-1]
        if hasattr(last, 'tool_calls') and last.tool_calls:
            return 'tools'
        return END

    graph = StateGraph(AgentState)
    graph.add_node('agent', agent_node)
    graph.add_node('tools', tools_node)
    graph.set_entry_point('agent')
    graph.add_conditional_edges('agent', should_continue)
    graph.add_edge('tools', 'agent')

    return graph.compile()


# ---------------------------------------------------------------------------
# LLMReorderAgent
# ---------------------------------------------------------------------------

class LLMReorderAgent(BaseAgent):
    '''
    LLM-powered reorder agent — Task 3: real LangGraph loop.

    Usage:
        import sys
        sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')
        from llm_agent import LLMReorderAgent

        agent     = LLMReorderAgent()
        decisions = agent.decide(context)
    '''

    def __init__(
        self,
        config_override: Optional[dict] = None,
    ) -> None:
        self._config = _load_config()
        if config_override:
            self._config.update(config_override)

        self._agent_version  = self._config.get('agent_version', 'llm_reorder_agent_v1')
        self._history_window = self._config.get('prompt_history_window', 10)
        self._system_prompt  = self._config.get('agent_prompt', '')
        self._llm_endpoint   = self._config.get('llm_endpoint', '')
        self._warehouse_id   = self._config.get('warehouse_id', '')

        # MLflow autolog — traces every LLM call and tool invocation
        # automatically. No manual span instrumentation needed.
        if self._config.get('mlflow_autolog', True):
            mlflow.langchain.autolog()
            mlflow.tracing.enable()

        # Set MLflow experiment so traces go to the right place
        experiment_name = self._config.get(
            'mlflow_experiment_name',
            '/Shared/reorder-llm-agent/experiments/llm_reorder_agent'
        )
        try:
            mlflow.set_experiment(experiment_name)
            print(f'  mlflow_experiment : {experiment_name}')
        except Exception as e:
            print(f'  mlflow_experiment : could not set ({e})')

        # Build LLM with tools bound
        self._graph = self._build_agent_graph()

        print(
            f'[LLMReorderAgent] initialised\n'
            f'  version        : {self._agent_version}\n'
            f'  llm_endpoint   : {self._llm_endpoint}\n'
            f'  mode           : LIVE (LangGraph loop active)\n'
        )

    def _build_agent_graph(self):
        '''
        Initialises the ChatDatabricks LLM, binds tools, and compiles
        the LangGraph StateGraph. Called once at init time.
        '''
        try:
            from databricks_langchain import ChatDatabricks
        except ImportError:
            try:
                from langchain_databricks import ChatDatabricks
            except ImportError:
                raise ImportError(
                    'databricks-langchain not installed. '
                    'Run: %pip install databricks-langchain langgraph'
                )
        
        # try:
        #     from langchain_databricks import ChatDatabricks
        # except ImportError:
        #     raise ImportError(
        #         'langchain-databricks not installed. '
        #         'Run: %pip install langchain-databricks langgraph'
        #     )

        llm = ChatDatabricks(
            endpoint  = self._llm_endpoint,
            max_tokens = 4096,
        )
        llm_with_tools = llm.bind_tools(ALL_TOOLS)
        return _build_graph(llm_with_tools)

    # ------------------------------------------------------------------
    # LLM decision path
    # ------------------------------------------------------------------

    def _llm_decide(
        self,
        prompt:  str,
        context: AgentContext,
    ) -> list[ReorderDecision]:
        '''
        Runs the LangGraph agentic loop for one tick, wrapped in an
        MLflow run so every tool call and LLM step is traced.

        Each tick gets its own child run tagged with sim_id and tick
        so you can filter traces in the MLflow UI per simulation run.
        '''
        from langchain_core.messages import HumanMessage, SystemMessage

        messages = [
            SystemMessage(content=self._system_prompt),
            HumanMessage(content=prompt),
        ]

        # Each tick is a child run — tags make it filterable in UI
        run_tags = {
            'sim_id'       : context.sim_id,
            'tick'         : str(context.tick),
            'agent_version': self._agent_version,
            'n_items'      : str(len(context.items())),
        }

        try:
            with mlflow.start_run(
                run_name = f'tick_{context.tick}_{context.sim_id}',
                tags     = run_tags,
                nested   = True,   # nested=True allows runs inside runs
            ):
                # Log the prompt as a parameter for later inspection
                mlflow.log_param('tick',   context.tick)
                mlflow.log_param('sim_id', context.sim_id)
                mlflow.log_param('items',  ','.join(context.items()))

                # Inside _llm_decide(), replace the graph.invoke line with:
                with mlflow.start_span(name=f'langgraph_tick_{context.tick}') as span:
                    span.set_inputs({
                        'sim_id'  : context.sim_id,
                        'tick'    : context.tick,
                        'n_items' : len(context.items()),
                    })
                    result = self._graph.invoke({'messages': messages})
                    final_message = result['messages'][-1]
                    raw_output    = final_message.content
                    span.set_outputs({'response_chars': len(raw_output)})

                # Final message — no tool calls means LLM is done
                final_message = result['messages'][-1]
                raw_output    = final_message.content

                print(
                    f'[LLMReorderAgent] tick={context.tick} '
                    f'LLM response ({len(raw_output)} chars) received'
                )

                decisions = _parse_llm_decisions(
                    raw_output, context, self._agent_version
                )

                # Log decision summary as metrics
                n_reorders = sum(1 for d in decisions if d.is_reorder)
                n_holds    = sum(1 for d in decisions if d.is_hold)
                total_qty  = sum(d.order_qty for d in decisions)

                mlflow.log_metric('n_reorders',  n_reorders)
                mlflow.log_metric('n_holds',     n_holds)
                mlflow.log_metric('total_order_qty', total_qty)

                return decisions

        except Exception as e:
            print(f'[LLMReorderAgent] ERROR in LangGraph invoke: {e}')
            return _fallback_hold(context, reason=f'LangGraph error: {e}')
    # ------------------------------------------------------------------
    # BaseAgent contract
    # ------------------------------------------------------------------

    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        '''
        Entry point — called by SimRunner once per tick,
        or directly in standalone tests.

        Serialises context → runs LangGraph loop → parses decisions.
        '''
        # Serialise context to prompt
        prompt = serialise_context(context, self._history_window)

        # Print prompt preview for tick 0 only
        if context.tick == 0:
            print(f'\n[LLMReorderAgent] tick=0 prompt preview:')
            print('─' * 60)
            print(prompt[:2000])
            if len(prompt) > 2000:
                print(f'  ... ({len(prompt) - 2000} more chars truncated)')
            print('─' * 60)

        # Run LLM loop
        decisions = self._llm_decide(prompt, context)

        # Log summary
        reorders = [d for d in decisions if d.is_reorder]
        holds    = [d for d in decisions if d.is_hold]
        print(
            f'[LLMReorderAgent] tick={context.tick} '
            f'decisions: {len(reorders)} reorder, {len(holds)} hold'
        )
        for d in decisions:
            qty_str = f'qty={d.order_qty}' if d.is_reorder else 'hold'
            print(f'  {d.item_id}: {qty_str}')

        return decisions

    def agent_version(self) -> str:
        return self._agent_version