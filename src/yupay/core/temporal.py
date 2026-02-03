
import polars as pl
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import math
import random


class TimeProfile:
    """
    Defines the temporal behavior of a business domain (Seasonality, Trend, Weekly Patterns).
    """

    def __init__(self, name: str, seasonality_weights: List[float] = None, trend_slope: float = 0.0):
        self.name = name
        # Monthly weights (Jan-Dec). Default: Flat (1.0)
        self.seasonality_weights = seasonality_weights or [1.0] * 12
        # Simple linear trend (growth per month). 0.01 = 1% growth/month
        self.trend_slope = trend_slope

    def get_factor(self, date: datetime, start_date: datetime) -> float:
        """
        Calculates the volume factor for a specific date based on seasonality and trend.
        """
        month_idx = date.month - 1
        seasonal_factor = self.seasonality_weights[month_idx]

        # Trend calculation: linear growth based on months elapsed
        months_elapsed = (date.year - start_date.year) * \
            12 + (date.month - start_date.month)
        trend_factor = 1.0 + (self.trend_slope * months_elapsed)

        # Weekly pattern (simplified): Weekends might be lower (0.8) for B2B, or Higher (1.2) for Retail
        # TODO: Make weekday weights configurable. Now hardcoding a generic erratic pattern.
        # Mon(0), Tue(1)... Sun(6)
        weekday = date.weekday()
        weekday_factor = 1.0
        if weekday >= 5:  # Sat, Sun
            weekday_factor = 0.9  # Slight dip on weekends generic

        return max(0.1, seasonal_factor * trend_factor * weekday_factor)


class TimeEngine:
    """
    Generates the temporal skeleton of the simulation.
    Replaces the old 'rows=N' logic with 'start_date' -> 'end_date' generation.
    """

    def __init__(self, start_date: str, end_date: str, daily_avg: int, profile: TimeProfile = None):
        self.start_date = datetime.strptime(str(start_date), "%Y-%m-%d")
        self.end_date = datetime.strptime(str(end_date), "%Y-%m-%d")
        self.daily_avg = daily_avg
        self.profile = profile or TimeProfile("default")

    def generate_timeline(self) -> pl.DataFrame:
        """
        Generates a DataFrame with one row per day and the target transaction count for that day.
        """
        days = (self.end_date - self.start_date).days + 1
        date_range = [self.start_date + timedelta(days=i) for i in range(days)]

        counts = []
        dates = []

        # Poisson helper
        def poisson_sample(lambda_):
            if lambda_ < 30:
                L = math.exp(-lambda_)
                k = 0
                p = 1.0
                while p > L:
                    k += 1
                    p *= random.random()
                return k - 1
            else:
                return max(0, int(random.normalvariate(lambda_, math.sqrt(lambda_))))

        for d in date_range:
            # 1. Base Seasonality & Trend
            base_factor = self.profile.get_factor(d, self.start_date)

            # 2. Macro Volatility (Yearly Economic Conditions) - 0.85 to 1.15
            year_seed = d.year * 13
            # Deterministic but pseudo-random per year
            macro_rnd = ((year_seed * 997) % 31) / 31.0
            macro_factor = 0.85 + (macro_rnd * 0.30)

            # 3. Micro Volatility (Payday + Weekends)
            day = d.day
            weekday = d.weekday()  # Mon=0, Sun=6
            micro_factor = 1.0

            # Payday Effect (Quincena & Fin de Mes)
            if 14 <= day <= 16 or day >= 28:
                micro_factor *= 1.25

            # Weekend Boost
            if weekday >= 5:  # Sat, Sun
                micro_factor *= 1.15

            # 4. Total Factor
            total_factor = base_factor * macro_factor * micro_factor

            # 5. Pure Randomness/Chaos (Daily Jitter) 0.8-1.2
            jitter = 0.8 + (random.random() * 0.4)

            # Final Expected Volume
            expected_vol = self.daily_avg * total_factor * jitter

            daily_count = poisson_sample(expected_vol)

            # Ensure at least 0
            counts.append(max(0, daily_count))
            dates.append(d)

        return pl.DataFrame({
            "date": dates,
            "target_rows": counts,
            "day_of_week": [d.weekday() for d in dates]
        })

    def expand_events(self, timeline_df: pl.DataFrame) -> pl.LazyFrame:
        """
        Explodes the daily summary into individual interaction events (rows).
        Critically: This replaces simple "row generation".
        Each row here is a potential "order" slot.
        """
        return (
            timeline_df.lazy()
            .select(
                pl.col("date").repeat_by(
                    "target_rows").explode().alias("event_date")
            )
            # Safety cleanup for 0 rows days
            .filter(pl.col("event_date").is_not_null())
        )
