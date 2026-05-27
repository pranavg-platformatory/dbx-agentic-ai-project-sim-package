'''
warehouse_sim/config/llm_agent_wrapper_config.py

Defines LLMAgentWrapperConfig, which contains the configuration for the LLM Agent Wrapper (LLAgentWrapper, warehouse_sim/agent/llm_agent_wrapper.py).

NOTE: Kept separate from SimConfig (warehouse_sim/config/models.py):
- These configuration parameters govern the wrapper's behaviour, not the simulation's
- Hence, they must be independently configurable and logged to MLflow independently
'''

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class LLMAgentWrapperConfig(BaseModel):
    '''
    Configuration for the LLM Agent Wrapper (LLAgentWrapper).

    Kept separate from SimConfig (warehouse_sim/config/models.py):
    - These configuration parameters govern the wrapper's behaviour, not the simulation's
    - Hence, they must be independently configurable and logged to MLflow independently

    ---

    # Fields:

    - `executor_trigger_every_n_ticks` (int): How often (in ticks) the executor is dispatched. \n
      NOTE:  No default: must be set explicitly to prevent silent non-reproducibility
    - `context_obsolescence_threshold_k` (int): A context assembled at tick T is considered stale if current_tick - T > K
        - Defaults to None, in which case the LLAgentWrapper resolves K to the minimum lead time from SimConfig at initialisation
        - The resolved value is what is logged to MLflow - not None
    - `queue_size` (int): Maximum number of QueueMessage objects the monitoring loop retains
        - Drain logic is always implemented in full regardless of this value
        - Defaults to 1
    - `stub_mode` (str): Controls the StubLLMAgent behaviour used in place of a real LLM call
        - "valid": returns correctly structured, logically valid decisions
        - "structural_fail": returns a malformed / unparseable response
        - "logical_fail": returns a parsed but logically invalid response
        - None: no stub; a real LLM call is made (via LLMReorderAgent)
    - `llm_agent_config_override` (dict | None): Optional config dict forwarded to LLMReorderAgent as its `config_override` argument
        - Used to override any field in LLMReorderAgent's config.yml (e.g. `llm_endpoint`, `prompt_history_window`) without editing the file
        - None means LLMReorderAgent loads config.yml with no overrides
        - Ignored when stub_mode is not None

    NOTE: `stub_mode` is always present and always logged to MLflow so it is unambiguous whether a run used the stub or not.
    '''

    executor_trigger_every_n_ticks: int = Field(
        ...,
        description=(
            "Executor dispatch frequency in ticks. "
            "No default - must be set explicitly."
        ),
        ge=1,
    )

    context_obsolescence_threshold_k: int | None = Field(
        default=None,
        description=(
            "Staleness cutoff in ticks. None resolves to min_lead_time "
            "from SimConfig at LLAgentWrapper initialisation."
        ),
        # ge=1 applies only when a value is explicitly set. Pydantic skips
        # field-level validators for None on an int | None field, so there
        # is no conflict with the None default.
        ge=1,
    )

    queue_size: int = Field(
        default=1,
        description="Maximum QueueMessage retention. Drain logic runs in full for any value.",
        ge=1,
    )

    stub_mode: Literal["valid", "structural_fail", "logical_fail"] | None = Field(
        default=None,
        description=(
            "StubLLMAgent mode. None means a real LLM call is made (via LLMReorderAgent). "
            "Always logged to MLflow."
        ),
    )

    llm_agent_config_override: dict | None = Field(
        default=None,
        description=(
            "Optional config dict forwarded to LLMReorderAgent as its config_override argument. "
            "Overrides any field in LLMReorderAgent's config.yml without editing the file. "
            "None means LLMReorderAgent loads config.yml with no overrides. "
            "Ignored when stub_mode is not None."
        ),
    )

    suppress_write_tools: bool = Field(
        default=True,
        description=(
            "When True (default), the write tools log_agent_decision and escalate_item are "
            "removed from LLMReorderAgent's tool list before the LangGraph graph is built. "
            "This prevents duplicate writes to hist_reorder_decisions: the runner's "
            "_write_decision_row() is the single authoritative writer inside the simulation, "
            "and escalate_item writes to ops_escalation_queue which the runner does not read. "
            "Set to False only when running LLMReorderAgent standalone outside the simulation, "
            "where the runner's write does not occur. Ignored when stub_mode is not None."
        ),
    )

    @model_validator(mode="after")
    def warn_if_k_not_set(self) -> LLMAgentWrapperConfig:
        '''
        Remind implementers that K=None will be resolved at LLAgentWrapper init time.

        NOTE:
        - None is intentionally valid here: LLMAgentWrapperConfig may be constructed before SimConfig is available, and the correct default (`min_lead_time`) can only be read from SimConfig
        - Raising at construction time would prevent this legitimate workflow
        - A warning (not an error) makes the deferred resolution visible without blocking it
        - The LLAgentWrapper is responsible for resolving None to `min_lead_time` at initialisation and logging the resolved value to MLflow - never None
        
        ---

        PARAMETERS:
        - None

        RETURNS:
        - (LLMAgentWrapperConfig): LLMAgentWrapperConfig instance encapsulating LLMAgentWrapper configuration
        '''

        if self.context_obsolescence_threshold_k is None:
            import warnings
            warnings.warn(
                "context_obsolescence_threshold_k is None. "
                "The LLAgentWrapper will resolve this to min_lead_time from SimConfig at "
                "initialisation. Ensure SimConfig is available before the LLAgentWrapper is "
                "constructed, and log the resolved value to MLflow - not None.",
                UserWarning,
                stacklevel=2,
            )
        return self