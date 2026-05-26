'''
warehouse_sim/engine/continuous.py

ContinuousRunner — a thin subclass of SimRunner that adds three capabilities
on top of the base engine, without touching any simulation logic:

  1. Wall-clock pacing    : sleeps between ticks so each tick represents
                            a real duration (configurable in seconds)
  2. Live progress output : prints a one-line status to stdout after each tick
  3. Graceful interruption: catches KeyboardInterrupt and writes SIM_ENDED
                            before exiting, so the event log is always complete

All state management, table writes, and event logging are inherited from
SimRunner unchanged. Only `run` and `_run_tick` are overridden.

---

Usage:

```python
from warehouse_sim.engine.continuous import ContinuousRunner, ProgressConfig

runner = ContinuousRunner(
    spark, world, agent, logger, sampler,
    progress=ProgressConfig(
        tick_real_duration_seconds=2.0,  # 2 real seconds per simulated tick
        print_every_n_ticks=1,
        show_stockouts=True,
        show_orders=True,
    ),
)
runner.run()
```
'''

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING

from .runner import SimRunner
from ..config.models import SimWorld
from ..world.patterns import PatternSampler
from ..event_log.event_log import EventLogger
from ..agent.base import BaseAgent

# NOTE: PatternSampler is kept in the constructor signature for consistency
# with SimRunner — it is not used internally by ContinuousRunner itself.

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# ProgressConfig
# ---------------------------------------------------------------------------

@dataclass
class ProgressConfig:
    '''
    Controls pacing and display behaviour of ContinuousRunner.

    ---

    # Fields:

    - `tick_real_duration_seconds` (float, optional): Wall-clock seconds to sleep after each tick. None = run as fast as possible, equivalent to SimRunner behaviour
    - `print_every_n_ticks` (int): Print one progress line every N ticks. Default 1 = every tick
    - `show_stockouts` (bool): Append a stockout warning to the progress line when unmet demand > 0 this tick
    - `show_orders` (bool): Append total units currently in transit to the progress line
    - `show_costs` (bool): Append cumulative total cost (all items) to the progress line
    - `show_disruptions` (bool): Append the count of deterministic disruptions active this tick
    '''

    tick_real_duration_seconds: Optional[float] = None
    print_every_n_ticks:        int             = 1
    show_stockouts:             bool            = True
    show_orders:                bool            = True
    show_costs:                 bool            = True
    show_disruptions:           bool            = True


# ---------------------------------------------------------------------------
# ContinuousRunner
# ---------------------------------------------------------------------------

class ContinuousRunner(SimRunner):
    '''
    SimRunner with wall-clock pacing and live progress output.

    NOTE:
    - Inherits all simulation logic from SimRunner
    - Only `run` and `_run_tick` are overridden

    ---

    # Fields:

    - `spark` (SparkSession): Active SparkSession
    - `world` (SimWorld): Fully loaded SimWorld (from `load_world`)
    - `agent` (BaseAgent): Any BaseAgent subclass - injected, never imported directly
    - `logger` (EventLogger): EventLogger instance for this sim run
    - `sampler` (PatternSampler): PatternSampler seeded with `world.config.random_seed`
    - `progress` (ProgressConfig, optional): Pacing and display settings. Defaults to `ProgressConfig()` (no sleep, print every tick, all fields shown)
    '''

    def __init__(
        self,
        spark:    "SparkSession",
        world:    SimWorld,
        agent:    BaseAgent,
        logger:   EventLogger,
        sampler:  PatternSampler,
        progress: Optional[ProgressConfig] = None,
    ) -> None:
        super().__init__(spark, world, agent, logger, sampler)
        self._progress  = progress or ProgressConfig()
        self._run_start: Optional[datetime] = None

        # Per-tick display state — populated in _run_tick after each parent call,
        # then consumed by _maybe_print_progress in the same tick.
        self._last_tick_stockouts:  dict[str, int] = {}
        self._last_tick_active_dis: int            = 0
        self._last_tick_pending:    int            = 0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        '''
        Run the simulation with live progress output and graceful interruption.

        NOTE:
        - Overrides SimRunner.run() to wrap the tick loop in a try/except/finally block
        - On KeyboardInterrupt (notebook Interrupt kernel), _teardown is called before exiting so SIM_ENDED is always written and the event log is always complete

        ---

        RETURNS:
        - None

        NOTE: In infinite or cyclic mode this method runs until interrupted. Press Interrupt in the notebook toolbar to stop cleanly.
        '''

        self._run_start = datetime.now(timezone.utc)
        num_ticks = self._config.num_ticks
        mode      = self._config.run_mode.value

        print(f"\n{'─'*60}")
        print(f"  Simulation starting")
        print(f"  sim_id    : {self._sim_id}")
        print(f"  run_mode  : {mode}")
        print(f"  num_ticks : {num_ticks if num_ticks else '∞'}")
        print(f"  tick_unit : {self._config.tick_unit.value}")
        pace = self._progress.tick_real_duration_seconds
        print(f"  pace      : {f'{pace}s / tick' if pace else 'as fast as possible'}")
        print(f"{'─'*60}\n")

        self._initialise()

        tick        = 0
        interrupted = False

        try:
            while self._should_continue(tick):
                self._run_tick(tick)
                self._maybe_print_progress(tick)
                self._maybe_sleep()
                tick += 1

        except KeyboardInterrupt:
            interrupted = True
            print(f"\n\n{'─'*60}")
            print(f"  ⚠  Interrupted at tick {tick}")
            print(f"{'─'*60}")

        finally:
            # NOTE: _teardown is in the finally block, not the except block,
            # so it runs whether the loop exits normally (finite mode) or via
            # interruption. This guarantees SIM_ENDED is always written.
            final_tick = max(tick - 1, 0)
            self._teardown(final_tick)
            self._print_final_summary(final_tick, interrupted)

    # ------------------------------------------------------------------
    # Progress display
    # ------------------------------------------------------------------

    def _maybe_print_progress(self, tick: int) -> None:
        '''
        Print one progress line to stdout if this tick falls on the configured print interval.

        ---

        PARAMETERS:
        - `tick` (int): Current simulation tick

        RETURNS:
        - None

        ---

        NOTE: Progress line format:
        
        `[tick    N/∞]  Xs elapsed  ETA Ys  │  item_A:  NN  item_B:  NN  cost=£NNN  orders= N pending  ⚠ stockout: item_A(N)  🔴 disruptions=N`
        
        ETA is shown only in finite mode; in infinite/cyclic mode it displays `—`.
        '''

        cfg = self._progress
        if tick % cfg.print_every_n_ticks != 0:
            return

        num_ticks   = self._config.num_ticks
        tick_label  = f"{tick:>4}"
        total_label = f"{num_ticks}" if num_ticks else "∞"

        # Elapsed wall time since run() was called
        elapsed     = datetime.now(timezone.utc) - self._run_start
        elapsed_str = _fmt_duration(elapsed)

        # ETA — only meaningful in finite mode
        if num_ticks and tick > 0:
            ticks_left    = num_ticks - tick - 1
            secs_per_tick = elapsed.total_seconds() / (tick + 1)
            eta_str       = _fmt_duration(timedelta(seconds=ticks_left * secs_per_tick))
        else:
            eta_str = "—"

        # Stock on hand per item (sorted for stable output)
        stock_str = "  ".join(
            f"{item_id}: {state.stock_on_hand:>4}"
            for item_id, state in sorted(self._stock_states.items())
        )

        # Optional suffix fields — only appended when their flag is set
        cost_str = (
            f"  cost=£{sum(cs.cumulative_total for cs in self._cost_states.values()):,.0f}"
            if cfg.show_costs else ""
        )
        orders_str = (
            f"  orders={self._last_tick_pending:>2} pending"
            if cfg.show_orders else ""
        )
        stockout_str = ""
        if cfg.show_stockouts and self._last_tick_stockouts:
            items_out    = ", ".join(
                f"{i}({u})" for i, u in self._last_tick_stockouts.items() if u > 0
            )
            stockout_str = f"  ⚠ stockout: {items_out}"

        dis_str = (
            f"  🔴 disruptions={self._last_tick_active_dis}"
            if cfg.show_disruptions and self._last_tick_active_dis > 0 else ""
        )

        print(
            f"[tick {tick_label}/{total_label}]"
            f"  {elapsed_str} elapsed  ETA {eta_str}"
            f"  │  {stock_str}"
            f"{cost_str}{orders_str}{stockout_str}{dis_str}"
        )

    def _print_final_summary(self, final_tick: int, interrupted: bool) -> None:
        '''
        Print a concise end-of-run summary to stdout.

        Called from the finally block of run() regardless of whether the simulation completed normally or was interrupted.

        ---

        PARAMETERS:
        - `final_tick` (int): The last tick that completed successfully
        - `interrupted` (bool): True if the run was stopped via KeyboardInterrupt

        RETURNS:
        - None
        '''

        elapsed = datetime.now(timezone.utc) - self._run_start
        status  = "INTERRUPTED" if interrupted else "COMPLETE"

        print(f"\n{'─'*60}")
        print(f"  Simulation {status}")
        print(f"  Final tick       : {final_tick}")
        print(f"  Wall time        : {_fmt_duration(elapsed)}")
        print(f"  Total reorders   : {self._total_reorders}")
        print(f"  Stockout ticks   : {self._total_stockout_ticks}")
        print(f"  Total cost       : £{self._total_cost:,.2f}")
        if self._remaining_budget is not None:
            print(f"  Remaining budget : £{self._remaining_budget:,.2f}")
        print(f"{'─'*60}\n")

    # ------------------------------------------------------------------
    # Pacing
    # ------------------------------------------------------------------

    def _maybe_sleep(self) -> None:
        '''
        - Sleep for `tick_real_duration_seconds` after each tick if configured
        - No-op when `tick_real_duration_seconds` is None

        ---

        RETURNS:
        - None
        '''

        duration = self._progress.tick_real_duration_seconds
        if duration:
            time.sleep(duration)

    # ------------------------------------------------------------------
    # _run_tick override
    # ------------------------------------------------------------------

    def _run_tick(self, tick: int) -> None:
        '''
        Delegate the full tick to SimRunner._run_tick, then capture the
        per-tick state needed to render the progress line.

        Overrides SimRunner._run_tick. All simulation logic — disruption
        evaluation, demand draw, agent decision, cost accumulation, table
        writes — remains in the parent method, unchanged.

        ---

        PARAMETERS:
        - `tick` (int): Current simulation tick

        RETURNS:
        - None

        ---

        NOTE 1: Stockouts are inferred from the change in cumulative_stockout_cost before and after the parent call, rather than by re-drawing from the RNG. This avoids any extra RNG consumption that would break reproducibility.

        NOTE 2: Disruption count covers only deterministic disruptions whose window includes this tick. Stochastic disruption activation is not re-evaluated here for the same reproducibility reason.
        '''

        # Snapshot cumulative stockout costs before the tick runs
        # so we can diff afterward to find which items stocked out this tick.
        pre_stockout = {
            item_id: cs.cumulative_stockout_cost
            for item_id, cs in self._cost_states.items()
        }

        super()._run_tick(tick)

        # Stockouts this tick: items whose stockout cost increased.
        # Back-calculate units from the cost delta and the per-unit rate.
        self._last_tick_stockouts = {
            item_id: int(
                (cs.cumulative_stockout_cost - pre_stockout.get(item_id, 0.0))
                / max(self._world.items[item_id].stockout_cost_per_unit_per_tick, 1e-9)
            )
            for item_id, cs in self._cost_states.items()
            if cs.cumulative_stockout_cost > pre_stockout.get(item_id, 0.0)
        }

        # Active disruption count — deterministic only, no RNG draw needed.
        self._last_tick_active_dis = sum(
            1 for d in self._world.disruptions
            if d.start_tick <= tick <= d.end_tick
            and not d.is_stochastic
        )

        # Units in transit: sum of stock_in_transit across all items.
        self._last_tick_pending = sum(
            s.stock_in_transit for s in self._stock_states.values()
        )


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_duration(delta: timedelta) -> str:
    '''
    Format a timedelta as a compact human-readable string.

    ---

    PARAMETERS:
    - `delta` (timedelta): Duration to format

    RETURNS:
    - (str): Formatted string, e.g. `2h04m30s`, `3m15s`, or `8s`
    '''

    total_secs      = int(delta.total_seconds())
    hours, rem      = divmod(total_secs, 3600)
    mins, secs      = divmod(rem, 60)
    if hours:
        return f"{hours}h{mins:02d}m{secs:02d}s"
    if mins:
        return f"{mins}m{secs:02d}s"
    return f"{secs}s"