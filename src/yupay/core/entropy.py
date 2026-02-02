
import polars as pl
from typing import Dict, Any, List


class EntropyInjector:
    """Base class for all chaos injectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def apply(self, lf: pl.LazyFrame, col_name: str = None, seed: int = None) -> pl.LazyFrame:
        raise NotImplementedError


class NullInjector(EntropyInjector):
    """
    Randomly sets values to Null based on a probability.
    Simulates missing data entry or system errors.
    """

    def apply(self, lf: pl.LazyFrame, col_name: str, probability: float = None, seed: int = 123) -> pl.LazyFrame:
        prob = probability if probability is not None else self.config.get(
            "null_probability", 0.01)

        if prob <= 0:
            return lf

        return lf.with_columns(
            pl.when(pl.col(col_name).hash(seed=seed).mod(1000) < (prob * 1000))
            .then(None)
            .otherwise(pl.col(col_name))
            .alias(col_name)
        )


class OrphanInjector(EntropyInjector):
    """
    Simulates Broken Foreign Keys.
    Modifies a FK column to point to non-existent IDs.
    """

    def apply(self, lf: pl.LazyFrame, col_name: str, max_valid_id: int, probability: float = None, seed: int = 456) -> pl.LazyFrame:
        prob = probability if probability is not None else self.config.get(
            "orphan_probability", 0.0)

        if prob <= 0:
            return lf

        return lf.with_columns(
            pl.when(pl.col(col_name).hash(seed=seed).mod(1000) < (prob * 1000))
            .then(pl.lit(max_valid_id + 9999))  # Point to nowhere
            .otherwise(pl.col(col_name))
            .alias(col_name)
        )


class StringNoiseInjector(EntropyInjector):
    """
    Injects string-level noise:
    1. Casing inconsistencies (UPPER, lower, Original).
    2. Whispers (Leading/Trailing spaces).
    """

    def apply(self, lf: pl.LazyFrame, col_name: str, casing_prob: float = None, spaces_prob: float = None, seed: int = 777) -> pl.LazyFrame:
        # Get config
        c_prob = casing_prob if casing_prob is not None else self.config.get(
            "string_noise", {}).get("casing_probability", 0.0)
        s_prob = spaces_prob if spaces_prob is not None else self.config.get(
            "string_noise", {}).get("spaces_probability", 0.0)

        # 1. Casing Noise
        if c_prob > 0:
            # Derived seed 1
            s1 = seed
            seed_expr = pl.col(col_name).hash(seed=s1)
            is_hit = seed_expr.mod(1000) < (c_prob * 1000)
            is_upper = seed_expr.mod(100) < 50

            lf = lf.with_columns(
                pl.when(is_hit & is_upper)
                .then(pl.col(col_name).str.to_uppercase())
                .when(is_hit & ~is_upper)
                .then(pl.col(col_name).str.to_lowercase())
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

        # 2. Spaces (Trim issues)
        if s_prob > 0:
            # Derived seed 2
            s2 = seed + 111
            seed_expr = pl.col(col_name).hash(seed=s2)
            is_hit = seed_expr.mod(1000) < (s_prob * 1000)
            is_prefix = seed_expr.mod(100) < 50

            lf = lf.with_columns(
                pl.when(is_hit & is_prefix)
                .then(pl.lit(" ") + pl.col(col_name))
                .when(is_hit & ~is_prefix)
                .then(pl.col(col_name) + pl.lit(" "))
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

        return lf


class EntropyManager:
    """
    Central hub for injecting chaos.
    Loads the correct profile (low, medium, high) and dispatches injectors.
    """

    def __init__(self, full_config: Dict[str, Any]):
        self.full_config = full_config
        self.chaos_config = full_config.get("chaos", {})

        # Load Global Seed (Default 42)
        # Prioritize main.yaml > defaults.yaml.
        # defaults.yaml should have it now.
        # But we look in full_config which is merged.
        self.base_seed = self.chaos_config.get("global_seed", 42)

        # Determine active factory profile
        level = full_config.get(
            "chaos_level", self.chaos_config.get("default_level", "low"))
        self.profile = self.chaos_config.get("levels", {}).get(level, {})

        # Instantiate injectors
        self.null_injector = NullInjector(self.profile)
        self.orphan_injector = OrphanInjector(self.profile)
        self.string_injector = StringNoiseInjector(self.profile)

    def inject_nulls(self, lf: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        """Apply null injection to a list of columns."""
        res = lf
        for i, col in enumerate(columns):
            # Salt the seed with column index to differ per column
            s = self.base_seed + 1000 + i
            res = self.null_injector.apply(res, col, seed=s)
        return res

    def inject_orphans(self, lf: pl.LazyFrame, fk_col: str, max_id: int) -> pl.LazyFrame:
        """Apply orphan injection to a FK column."""
        # Derived seed
        s = self.base_seed + 2000
        return self.orphan_injector.apply(lf, fk_col, max_id, seed=s)

    def inject_string_noise(self, lf: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        """Apply casing/spacing noise to text columns."""
        res = lf
        for i, col in enumerate(columns):
            # Derived seed
            s = self.base_seed + 3000 + i
            res = self.string_injector.apply(res, col, seed=s)
        return res
