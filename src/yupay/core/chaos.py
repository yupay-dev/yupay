import polars as pl
import random
from typing import List, Any

class ChaosEngine:
    def __init__(self, config: dict):
        self.config = config.get("chaos", {})
        self.enabled = self.config.get("enabled", False)
        self.seed = config.get("seed", 42)
        self.rng = random.Random(self.seed)

    def apply(self, df: pl.DataFrame, table_name: str) -> pl.DataFrame:
        """Applies configured chaos rules to a DataFrame."""
        if not self.enabled:
            return df
            
        rules = self.config.get("rules", {}).get(table_name, {})
        if not rules:
            return df
            
        print(f"   [Chaos] Injecting anomalies into {table_name}...")
        
        # 1. Row Duplication
        dup_rate = rules.get("duplication_rate", 0.0)
        if dup_rate > 0:
            df = self._inject_duplicates(df, dup_rate)

        # 2. Column-specific anomalies
        for col_name, anomalies in rules.get("columns", {}).items():
            if col_name not in df.columns:
                continue
                
            for anomaly_type, params in anomalies.items():
                if anomaly_type == "nulls":
                    df = self._inject_nulls(df, col_name, params)
                elif anomaly_type == "outliers":
                    df = self._inject_outliers(df, col_name, params)
                elif anomaly_type == "text_corruption":
                    df = self._corrupt_text(df, col_name, params)
                elif anomaly_type == "negatives":
                    df = self._inject_negatives(df, col_name, params)
                    
        return df

    def _inject_duplicates(self, df: pl.DataFrame, rate: float) -> pl.DataFrame:
        n_rows = len(df)
        n_dupes = int(n_rows * rate)
        if n_dupes == 0:
            return df
            
        # Sample rows to duplicate
        dupe_indices = self.rng.sample(range(n_rows), n_dupes)
        dupes = df.select(pl.all().gather(dupe_indices))
        print(f"      -> Duplicated {n_dupes} rows")
        return pl.concat([df, dupes])

    def _inject_nulls(self, df: pl.DataFrame, col: str, rate: float) -> pl.DataFrame:
        """Randomly sets values to null."""
        return df.with_columns(
            pl.when(pl.Series([self.rng.random() > rate for _ in range(len(df))]))
            .then(pl.col(col))
            .otherwise(None)
            .alias(col)
        )

    def _inject_outliers(self, df: pl.DataFrame, col: str, params: dict) -> pl.DataFrame:
        """Multiplies values by a distinct factor."""
        rate = params.get("rate", 0.01)
        factor = params.get("factor", 100.0)
        
        return df.with_columns(
            pl.when(pl.Series([self.rng.random() > rate for _ in range(len(df))]))
            .then(pl.col(col))
            .otherwise(pl.col(col) * factor)
            .alias(col)
        )

    def _inject_negatives(self, df: pl.DataFrame, col: str, rate: float) -> pl.DataFrame:
        """Multiplies values by -1."""
        return df.with_columns(
            pl.when(pl.Series([self.rng.random() > rate for _ in range(len(df))]))
            .then(pl.col(col))
            .otherwise(pl.col(col) * -1)
            .alias(col)
        )
        
    def _corrupt_text(self, df: pl.DataFrame, col: str, rate: float) -> pl.DataFrame:
        """Adds whitespace padding or alters case."""
        # This is expensive in Polars without a custom expression, 
        # so we'll use a simpler mapping approach or map_elements if needed.
        # For performance, we'll just add padding to random rows.
        
        def corrupt(val):
            if val is None: return None
            if self.rng.random() > rate: return val
            
            # Types of corruption
            choice = self.rng.choice(["pad", "upper", "lower", "empty"])
            if choice == "pad": return f"  {val} "
            if choice == "upper": return val.upper()
            if choice == "lower": return val.lower()
            if choice == "empty": return ""
            return val

        return df.with_columns(
            pl.col(col).map_elements(corrupt, return_dtype=pl.String).alias(col)
        )
