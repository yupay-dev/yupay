
import polars as pl
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Optional, Any
import math
import random


class TimeProfile:
    """
    Defines the temporal behavior of a business domain (Seasonality, Trend, Weekly Patterns, Events).
    """

    def __init__(self, name: str,
                 seasonality_weights: List[float] = None,
                 trend_slope: float = 0.0,
                 weekly_weights: List[float] = None,
                 holidays: Dict[Tuple[int, int], float] = None,
                 special_dates: Dict[date, float] = None,
                 enable_payday: bool = True):
        self.name = name
        # Monthly weights (Jan-Dec). Default: Flat (1.0)
        self.seasonality_weights = seasonality_weights or [1.0] * 12
        # Simple linear trend (growth per month). 0.01 = 1% growth/month
        self.trend_slope = trend_slope
        # Validations for weekly weights
        if weekly_weights and len(weekly_weights) != 7:
            raise ValueError("weekly_weights must have exactly 7 elements (Mon-Sun)")
        # Default: Flat week
        self.weekly_weights = weekly_weights or [1.0] * 7
        
        # Holidays: (Month, Day) -> Multiplier
        self.holidays = holidays or {}
        # Special Shocks: Date -> Multiplier
        self.special_dates = special_dates or {}
        
        self.enable_payday = enable_payday

    def get_factor(self, current_date: datetime, start_date: datetime) -> float:
        """
        Calculates the volume factor for a specific date based on all configured components.
        """
        # 1. Seasonality (Monthly)
        month_idx = current_date.month - 1
        seasonal_factor = self.seasonality_weights[month_idx]

        # 2. Trend (Linear daily growth)
        # Calculate percent of year elapsed or total days elapsed
        days_elapsed = (current_date - start_date).days
        # Approx months for compatible slope definition
        months_elapsed = days_elapsed / 30.44
        trend_factor = 1.0 + (self.trend_slope * months_elapsed)

        # 3. Weekly Pattern
        weekday = current_date.weekday() # 0=Mon, 6=Sun
        weekly_factor = self.weekly_weights[weekday]
        
        # 4. Holidays (Recurring)
        holiday_factor = self.holidays.get((current_date.month, current_date.day), 1.0)
        
        # 5. Special Dates (One-off)
        special_factor = self.special_dates.get(current_date.date(), 1.0)
        
        # 6. Payday Effect (if enabled)
        payday_factor = 1.0
        if self.enable_payday:
            day = current_date.day
            # Quincena (14-16) or Fin de Mes (28+)
            if 14 <= day <= 16: 
                payday_factor = 1.25
            elif day >= 28:
                payday_factor = 1.35

        return max(0.1, seasonal_factor * trend_factor * weekly_factor * holiday_factor * special_factor * payday_factor)


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
            # 1. Base Composite Factor (Seasonality + Trend + Weekly + Holiday + Payday)
            composite_factor = self.profile.get_factor(d, self.start_date)

            # 2. Macro Volatility (Yearly Economic Conditions) - 0.85 to 1.15
            year_seed = d.year * 13
            # Deterministic but pseudo-random per year
            macro_rnd = ((year_seed * 997) % 31) / 31.0
            macro_factor = 0.85 + (macro_rnd * 0.30)
            
            # 3. Pure Randomness/Chaos (Daily Jitter) 0.8-1.2
            jitter = 0.8 + (random.random() * 0.4)

            # Final Expected Volume
            expected_vol = self.daily_avg * composite_factor * macro_factor * jitter

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
