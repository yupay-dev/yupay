
import polars as pl
from typing import Dict, Any

from yupay.domains.sales.inventory.movements import InventoryDataset
from yupay.domains.sales.orders import SalesDataset
from yupay.domains.sales.customers import CustomerGenerator
from yupay.domains.sales.products import ProductGenerator
from yupay.core.dataset import BaseDataset
from yupay.core.entropy import EntropyManager


class ERPDataset(BaseDataset):
    def build(self, config: Dict[str, Any]) -> Dict[str, pl.LazyFrame]:
        """
        Backward compatible monolithic build.
        """
        dims = self.build_dimensions(config)
        trans = self.build_batch(config)
        return {**dims, **trans}

    def build_dimensions(self, config: Dict[str, Any]) -> Dict[str, pl.LazyFrame]:
        """
        Generates catalogs/dimensions only.
        """
        print("   -> Generando Dimensiones (Catalogs)...")
        # Reuse SalesDataset and InventoryDataset but extract only catalogs
        # This is a bit duplicative but keeps causality.
        sales_ds = SalesDataset()
        inv_ds = InventoryDataset()

        # We use a dummy date range for catalogs if needed,
        # but build() returns everything. We'll filter.
        sales_data = sales_ds.build(config)
        inv_data = inv_ds.build(config)

        return {
            "customers": sales_data["customers"],
            "products": sales_data["products"],
            "suppliers": inv_data["suppliers"]
        }

    def build_batch(self, config: Dict[str, Any], start_date: str = None, end_date: str = None) -> Dict[str, pl.LazyFrame]:
        """
        Generates transactional data for a specific time window.
        """
        inv_ds = InventoryDataset()
        sales_ds = SalesDataset()

        inv_batch = inv_ds.build(
            config, start_date_override=start_date, end_date_override=end_date)
        sales_batch = sales_ds.build(
            config, start_date_override=start_date, end_date_override=end_date)

        return {
            "stock_movements": inv_batch["stock_movements"],
            "orders": sales_batch["orders"],
            "payments": sales_batch["payments"]
        }
