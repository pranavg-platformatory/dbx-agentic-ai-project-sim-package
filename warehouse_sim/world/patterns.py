'''
warehouse_sim/world/patterns.py

- Demand and supply pattern sampling
- Given a `Pattern` model and a tick number, returns a sampled integer quantity

---

NOTE: No Databricks dependency - pure Python / numpy.

---

Supports:
- Statistical distributions: poisson, normal, uniform, negative_binomial, zero_inflated_poisson
- Custom schedules (cycled if shorter than num_ticks)
- Seasonal multiplier overlay
- Gaussian noise
- Deterministic output via a seeded numpy RNG

---

The spec sections referenced in this source file are present in the following path (in this repository):

__docs__/simulationSpecs.md

---

Usage:

```
from warehouse_sim.world.patterns import PatternSampler
sampler = PatternSampler(seed=42)
qty = sampler.sample(pattern, tick=7)
```
'''

from __future__ import annotations

import math

import numpy as np

from ..config.models import Distribution, Pattern, PatternType


class PatternSampler:
    '''
    Stateful sampler that wraps a seeded numpy RNG.

    - A single shared instance should be:
        - Created at simulation startup
        - Passed through to every component that needs to draw samples
    - This guarantees that the global draw order - and therefore reproducibility - is controlled by the caller (engine/runner.py)
    '''

    def __init__(self, seed: int) -> None:
        self._rng = np.random.default_rng(seed)

    #====================================
    # Public API
    #====================================

    def sample(self, pattern: Pattern, tick: int) -> int:
        '''
        Draw one sample for the given pattern at the given tick.

        Pipeline (spec section 3.5, __docs__/simulationSpecs.md in this repo):
        1. Base value from distribution or custom schedule
        2. Apply seasonal multiplier (if present)
        3. Add Gaussian noise (if `noise_std` > 0)
        4. Floor to int, clamp to >= 0

        Returns an integer quantity >= 0.
        '''

        base = self._base_value(pattern, tick)
        value = self._apply_seasonal(base, pattern, tick)
        value = self._apply_noise(value, pattern)
        return max(0, math.floor(value))

    def sample_lead_time(self, base_ticks: int, variability: float) -> int:
        '''
        - Sample an actual lead time (spec section 3.6, __docs__/simulationSpecs.md in this repo)
        - Returns `max(1, round(Normal(base_ticks, variability)))`
        - Called by the engine at reorder placement; placed here so the same RNG is used for all stochastic draws
        '''

        if variability == 0.0:
            return base_ticks
        raw = self._rng.normal(loc=base_ticks, scale=variability)
        return max(1, round(raw))

    def draw_uniform(self) -> float:
        '''
        - Draw a single `uniform [0, 1)` float
        - Used by engine/disruptions.py for stochastic trigger checks
        '''

        return float(self._rng.uniform())

    #====================================
    # Internal helpers
    #====================================

    def _base_value(self, pattern: Pattern, tick: int) -> float:
        if pattern.pattern_type == PatternType.CUSTOM:
            return self._sample_custom(pattern, tick)
        return self._sample_statistical(pattern)

    def _sample_custom(self, pattern: Pattern, tick: int) -> float:
        schedule = pattern.custom_schedule  # guaranteed non-empty by model validator
        idx = tick % len(schedule)          # cycle if schedule shorter than num_ticks
        return float(schedule[idx])

    def _sample_statistical(self, pattern: Pattern) -> float:
        dist   = pattern.distribution
        params = pattern.dist_params or {}

        if dist == Distribution.POISSON:
            mu = float(params.get("mu", params.get("lam", 1.0)))
            return float(self._rng.poisson(lam=mu))

        if dist == Distribution.NORMAL:
            mu    = float(params.get("mu", params.get("mean", 0.0)))
            sigma = float(params.get("sigma", params.get("std", 1.0)))
            return float(self._rng.normal(loc=mu, scale=sigma))

        if dist == Distribution.UNIFORM:
            low  = float(params.get("low",  params.get("min", 0.0)))
            high = float(params.get("high", params.get("max", 1.0)))
            return float(self._rng.uniform(low=low, high=high))

        if dist == Distribution.NEGATIVE_BINOMIAL:
            # Parameterised as (n, p) where mean = n(1-p)/p
            n = float(params.get("n", 1))
            p = float(params.get("p", 0.5))
            return float(self._rng.negative_binomial(n=n, p=p))

        if dist == Distribution.ZERO_INFLATED_POISSON:
            mu   = float(params.get("mu", 1.0))
            pi   = float(params.get("pi", 0.2))   # zero-inflation probability
            if self._rng.uniform() < pi:
                return 0.0
            return float(self._rng.poisson(lam=mu))

        raise ValueError(f"Unsupported distribution: {dist!r}")

    def _apply_seasonal(self, value: float, pattern: Pattern, tick: int) -> float:
        schedule = pattern.seasonal_multiplier_schedule
        if not schedule:
            return value
        multiplier = schedule[tick % len(schedule)]
        return value * multiplier

    def _apply_noise(self, value: float, pattern: Pattern) -> float:
        if pattern.noise_std <= 0:
            return value
        noise = self._rng.normal(loc=0.0, scale=pattern.noise_std)
        return value + noise
