
import polars as pl
from yupay.core.dataset import BaseDataset
from yupay.domains.inventory.suppliers import SupplierGenerator
from yupay.domains.sales.products import ProductGenerator
from yupay.core.temporal import TimeEngine
from yupay.core.random import Randomizer
from yupay.core.entropy import EntropyManager


class InventoryDataset(BaseDataset):
    """
    Orchestrates generation of Inventory data: Suppliers and Stock Movements.
    """

    def build(self, config: dict, rows_map: dict[str, int] = None, start_date_override: str = None, end_date_override: str = None) -> dict[str, pl.LazyFrame]:
        # 0. Config
        start_date = start_date_override or config.get(
            "start_date", "2024-01-01")
        end_date = end_date_override or config.get("end_date", "2024-12-31")
        # Stock movements volume usually lower than sales (bulk orders)
        # but for simplicity we assume a ratio
        daily_avg_sales = config.get("daily_avg_transactions", 1000)
        daily_avg_movements = max(10, daily_avg_sales // 10)

        n_suppliers = config.get("domains", {}).get(
            "inventory", {}).get("suppliers_base", 100)
        n_products = config.get("domains", {}).get(
            "sales", {}).get("products_catalog_size", 500)

        # 1. Generators
        supp_gen = SupplierGenerator(config.get(
            "entities", {}).get("suppliers", {}))
        # We need products just to ensure we know the count for FKs, we don't necessarily need to output them again if Sales does it.
        # But for standalone Inventory generation, we might want them.
        # For now, we assume we generate the dataframe to get consistent IDs if deterministic

        suppliers_lazy = supp_gen.generate(
            n_suppliers).with_row_index("supp_idx")

        # 2. Stock Movements (Time Engine)
        rnd = Randomizer(seed=config.get("seed", 42) + 10)
        time_eng = TimeEngine(start_date, end_date, daily_avg_movements)
        chaos_eng = EntropyManager(config)

        # Generate timeline
        movements_lf = time_eng.expand_events(time_eng.generate_timeline())
        movements_lf = movements_lf.with_row_index("mov_idx")

        movements_lf = movements_lf.with_columns([
            pl.col("mov_idx").cast(pl.UInt32).alias("movement_id"),
            pl.col("event_date").cast(pl.Date).alias("movement_date"),

            # FKs
            rnd.random_index_expr(n_suppliers, "mov_idx").alias("supp_idx"),
            rnd.random_index_expr(n_products, "mov_idx").alias(
                "product_id"),  # Direct ID assumption (0 to N-1)

            # Attributes
            # Type: 90% Ingress (Purchase), 10% Adjustment
            (pl.col("mov_idx").hash(11).mod(100) < 90).alias("is_ingress"),

            # Qty: Bulk quantities for ingress
            (pl.col("mov_idx").hash(13).mod(100) *
             5 + 10).cast(pl.UInt16).alias("quantity")
        ])

        movements_lf = movements_lf.with_columns(
            pl.when(pl.col("is_ingress")).then(pl.lit("INGRESS"))
            .otherwise(pl.lit("ADJUSTMENT")).alias("movement_type")
        ).drop("is_ingress")

        # 3. Entropy
        # Orphans in suppliers
        movements_lf = chaos_eng.inject_orphans(
            movements_lf, "supp_idx", n_suppliers)
        movements_lf = chaos_eng.inject_nulls(movements_lf, ["movement_date"])

        # 4. Join to resolve Supplier - Denormalized & Dirty
        # Join on supp_idx (internal) to get real attributes
        movements_enriched = movements_lf.join(
            suppliers_lazy.select(["supp_idx", "supplier_id", "name"]),
            on="supp_idx",
            how="left"
        ).rename({"name": "supplier_name"})

        # Inject String Noise (Casing, Spaces) into Supplier Name
        movements_enriched = chaos_eng.inject_string_noise(
            movements_enriched, ["supplier_name"])

        # Final Select: DROP supplier_id (User requirement: "no usar los ids")
        movements_enriched = movements_enriched.select([
            "movement_id", "movement_date",
            "supplier_name",  # Replaces supplier_id
            "product_id", "movement_type", "quantity"
        ])

        return {
            "suppliers": suppliers_lazy.drop("supp_idx"),
            "stock_movements": movements_enriched
        }
