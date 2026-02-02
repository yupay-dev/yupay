
import shutil
from datetime import datetime
from typing import Dict, Any


class SizeEstimator:
    """
    Estimates the dataset size and validates resource constraints before generation.
    Enforces 'Guardrails' to protect the user's machine.
    """

    # Uncertainty factor (30% buffer for overhead, metadata, and var size)
    UNCERTAINTY_FACTOR = 1.3

    def __init__(self, config: Dict[str, Any], defaults: Dict[str, Any]):
        self.config = config
        self.defaults = defaults

    def validate_and_estimate(self, domain: str = "sales") -> Dict[str, Any]:
        """
        Runs validation checks and returns estimation details.
        Raises ValueError if hard limits are exceeded.
        Raises EnvironmentError if disk space is insufficient.
        """
        # 1. Parse Dates and Duration
        start_date = datetime.strptime(
            str(self.config["start_date"]), "%Y-%m-%d")
        end_date = datetime.strptime(str(self.config["end_date"]), "%Y-%m-%d")

        if end_date < start_date:
            raise ValueError(
                f"End date ({end_date}) cannot be before start date ({start_date})")

        days = (end_date - start_date).days + 1
        max_days = self.defaults["system"]["max_days_hard_limit"]

        if days > max_days:
            raise ValueError(
                f"Requested duration {days} days exceeds the hard limit of {max_days} days. "
                "Please reduce the date range."
            )

        # 2. Validate Volume
        # Inventory volume is derived, so we might need a multiplier or specific config
        # For now, we respect the global 'daily_avg_transactions' which usually drives the main volume
        daily_volume = self.config["daily_avg_transactions"]
        hard_cap = self.defaults["system"]["max_daily_volume_cap"]
        user_cap = self.config.get("max_daily_volume_limit", hard_cap)

        effective_cap = min(hard_cap, user_cap)

        if daily_volume > effective_cap:
            raise ValueError(
                f"Requested daily volume {daily_volume} exceeds the limit of {effective_cap}. "
                "Check 'max_daily_volume_limit' in main.yaml or reduce 'daily_avg_transactions'."
            )

        # 3. Estimate Size
        # Formula: Days * DailyVol * AvgRowBytes * Factor
        # Get column weights from defaults based on DOMAIN
        weights = self.defaults["domains"].get(
            domain, {}).get("estimation_weights", {})
        if not weights:
            # Fallback to sales if domain not found or generic
            weights = self.defaults["domains"]["sales"]["estimation_weights"]

        # Calculate approximate bytes per transaction
        if domain == "sales":
            # Unified Sales Domain (ERP Scope)
            # Weights cover: orders, payments, customers (0.1), products (0.1), suppliers (0.1), stock_movements (0.1)
            # Note: Stock movements is roughly 10% of sales volume, hence 0.1 factor or we sum full weights if volume is separate?
            # Estimator uses 'daily_volume' which is SALES volume.
            # So items depending on it should be weighted.

            # 1. Transactional (1:1 approx or 1:N)
            w_orders = weights.get("orders", 20)
            w_payments = weights.get("payments", 30)

            # 2. Dimensions (Overhead per transaction is small, we amortize total size)
            # Amortization factor: assume 1MB dimension size distributed over 1M rows -> 1 byte.
            # But let's keep the existing safe heuristic: 10% of row size per transaction
            w_cust = weights.get("customers", 50) * 0.1
            w_prod = weights.get("products", 40) * 0.1
            w_supp = weights.get("suppliers", 50) * 0.05

            # 3. Related Facts
            # Stock Movements: ~10% of sales volume.
            w_movements = weights.get("stock_movements", 25) * 0.1

            bytes_per_trx = w_orders + w_payments + w_cust + w_prod + w_supp + w_movements
        else:
            # Generic fallback or other future domains
            bytes_per_trx = 100

        total_rows = days * daily_volume
        estimated_bytes = total_rows * bytes_per_trx * self.UNCERTAINTY_FACTOR
        estimated_gb = estimated_bytes / (1024**3)

        # 4. Check Disk Space
        output_path = self.config.get("output_path", "data")
        total, used, free = shutil.disk_usage(
            output_path if "." in output_path else ".")
        free_gb = free / (1024**3)

        needed_free_gb = estimated_gb + \
            self.defaults["system"]["safety_buffer_gb"]

        # Check against user config max usage
        user_max_gb = self.config.get("max_disk_usage_gb", 10)

        if estimated_gb > user_max_gb:
            raise ValueError(
                f"Estimated size {estimated_gb:.2f} GB exceeds your configured limit of {user_max_gb} GB. "
                "Increase 'max_disk_usage_gb' or reduce volume/dates."
            )

        if free_gb < needed_free_gb:
            raise EnvironmentError(
                f"Insufficient disk space. Need {needed_free_gb:.2f} GB free (including buffer), "
                f"but only have {free_gb:.2f} GB available."
            )

        return {
            "days": days,
            "total_rows_estimated": int(total_rows),
            "estimated_size_gb": estimated_gb,
            "status": "OK"
        }
