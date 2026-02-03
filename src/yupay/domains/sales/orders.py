import polars as pl
from yupay.core.dataset import BaseDataset
from yupay.domains.sales.customers import CustomerGenerator
from yupay.domains.sales.products import ProductGenerator
from yupay.core.temporal import TimeEngine
from yupay.core.random import Randomizer
from yupay.core.entropy import EntropyManager


class SalesDataset(BaseDataset):
    """
    Orquestador para el dominio de Ventas.
    Genera Clientes, Productos y Órdenes relacionadas.
    """

    def build(self, config: dict, rows_map: dict[str, int] = None, start_date_override: str = None, end_date_override: str = None) -> dict[str, pl.LazyFrame]:
        # 0. Configuración Temporal y Volumetría
        start_date = start_date_override or config.get(
            "start_date", "2024-01-01")
        end_date = end_date_override or config.get("end_date", "2024-12-31")
        daily_avg = config.get("daily_avg_transactions", 1000)

        # 0.1 Configuración de Estacionalidad
        seasonality_cfg = config.get("seasonality", {})
        season_map = seasonality_cfg.get("seasons", {
            "summer": [1, 2, 3, 12],
            "winter": [6, 7, 8, 9]
        })
        # Default fallback map if config is missing (South Hemisphere)
        summer_months = set(season_map.get("summer", [1, 2, 3, 12]))
        winter_months = set(season_map.get("winter", [6, 7, 8, 9]))

        weights_cfg = seasonality_cfg.get("weights", {
            "summer_months": {"summer": 0.6, "winter": 0.05, "all_year": 0.35},
            "winter_months": {"summer": 0.1, "winter": 0.5, "all_year": 0.4},
            "transition_months": {"summer": 0.25, "winter": 0.25, "all_year": 0.5},
        })

        # Entities
        n_customers = config.get("domains", {}).get(
            "sales", {}).get("customers_base", 10000)
        n_products = config.get("domains", {}).get(
            "sales", {}).get("products_catalog_size", 500)

        # 1. Generar Catálogos (Materializamos Productos para facilitar sampling)
        cust_gen = CustomerGenerator(config.get(
            "entities", {}).get("customers", {}))

        # Inject catalog into product config
        prod_config = config.get("entities", {}).get("products", {})
        prod_config["catalog"] = config.get("catalogs", {}).get("products", {})
        prod_gen = ProductGenerator(prod_config)

        customers_lazy = cust_gen.generate(
            n_customers).with_row_index("cust_idx")
        products_lazy = prod_gen.generate(
            n_products).with_row_index("prod_idx")

        # collect products to buckets
        products_pdf = products_lazy.collect()

        # Get IDs per tag
        ids_summer = products_pdf.filter(pl.col("seasonal_tag") == "summer")[
            "product_id"].to_list()
        ids_winter = products_pdf.filter(pl.col("seasonal_tag") == "winter")[
            "product_id"].to_list()
        ids_allyear = products_pdf.filter(pl.col("seasonal_tag") == "all_year")[
            "product_id"].to_list()

        # Fallback if empty lists (avoid crash)
        if not ids_summer:
            ids_summer = [0]
        if not ids_winter:
            ids_winter = [0]
        if not ids_allyear:
            ids_allyear = range(n_products)  # Use all if tag missing

        # 2. Generar Órdenes (Temporal Engine Strategy)
        rnd = Randomizer(seed=config.get("seed", 42) + 2)
        time_eng = TimeEngine(start_date, end_date, daily_avg)
        chaos_eng = EntropyManager(config)

        orders_lf = time_eng.expand_events(time_eng.generate_timeline())
        orders_lf = orders_lf.with_row_index("order_idx")

        # 3. Lógica Estacional: Asignar Tag Target -> Asignar Product ID
        # Paso 3.1: Determinar el "Target Tag" (Summer/Winter/AllYear) - Lógica Orgánica (Ondas)
        # Constants
        VAL_TWO_PI = 6.28318530718
        VAL_PEAK_SUMMER = 45

        # Generar Factores de Estacionalidad Continua
        orders_lf = orders_lf.with_columns([
            pl.col("event_date").dt.ordinal_day().cast(pl.Int32).alias("doy"),
            pl.col("event_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("order_idx").hash(100).mod(1000).cast(pl.Float32).truediv(
                1000.0).alias("probs_rnd"),  # Roll de dado 0-1

            # Base logic columns (Restored)
            pl.col("order_idx").cast(pl.UInt32).alias("order_id"),
            pl.col("event_date").cast(pl.Date).alias("order_date"),
        ])

        # Calcular Onda Estacional con "Chaos" (Shift Anual + Ruido)
        orders_lf = orders_lf.with_columns([
            # Yearly Shift: +/- 15 dias de desfase cada año
            (pl.col("year").hash(2024).mod(31).cast(
                pl.Int32) - 15).alias("yearly_shift"),

            # Daily Noise: (-0.1 a +0.1)
            (pl.col("order_idx").hash(999).mod(200).cast(
                pl.Float32).truediv(1000.0) - 0.1).alias("daily_noise")
        ])

        orders_lf = orders_lf.with_columns([
            # Normalized Day: (doy + shift - peak)
            (pl.col("doy") + pl.col("yearly_shift") -
             pl.lit(VAL_PEAK_SUMMER)).alias("shifted_doy")
        ])

        # Calcular Seasonal Intensity
        orders_lf = orders_lf.with_columns([
            (
                (
                    (pl.col("shifted_doy").cast(pl.Float32) /
                     365.0 * pl.lit(VAL_TWO_PI)).cos()
                    + 1.0
                ) / 2.0
                + pl.col("daily_noise")
            ).clip(0.0, 1.0).alias("seasonal_intensity")
        ])

        # Definir Probabilidades Dinámicas
        orders_lf = orders_lf.with_columns([
            (0.05 + 0.65 * pl.col("seasonal_intensity")).alias("p_summer"),
            (0.05 + 0.55 * (1.0 - pl.col("seasonal_intensity"))).alias("p_winter")
        ])

        # Weekend Bias for Summer Products (BBQ, Beer, etc.)
        # Weekday: Mon=1, Sun=7. Polars doy -> need weekday.
        # We calculated 'doy'. We need weekday from event_date.
        # day_of_week is 1-7 in Polars or 0-6? dt.weekday() is 1-7 (Mon-Sun).
        orders_lf = orders_lf.with_columns(
            pl.col("event_date").dt.weekday().alias("weekday")
        )

        # Adjust p_summer: If Sat(6) or Sun(7), boost by 20%
        orders_lf = orders_lf.with_columns([
            pl.when(pl.col("weekday") >= 6)
            .then(pl.col("p_summer") * 1.2)
            .otherwise(pl.col("p_summer"))
            .clip(0.0, 1.0)  # Ensure valid prob
            .alias("p_summer")
        ])

        # Assign Target Tag based on thresholds
        # Threshold 1: p_summer
        # Threshold 2: p_summer + p_winter
        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("probs_rnd") < pl.col(
                "p_summer")).then(pl.lit("summer"))
            .when(pl.col("probs_rnd") < (pl.col("p_summer") + pl.col("p_winter"))).then(pl.lit("winter"))
            .otherwise(pl.lit("all_year"))
            .alias("target_tag")
        )

        # Paso 3.2: Asignar IDs reales - OPTIMIZED (Join Strategy)
        # Avoid map_elements (slow python UDF). Use Modulo + Join.

        # Helper to create lookup tables
        def make_lookup(ids_list, name_suffix):
            return pl.DataFrame({
                f"idx_{name_suffix}": pl.int_range(0, len(ids_list), dtype=pl.UInt32, eager=True),
                f"cand_{name_suffix}": pl.Series(ids_list, dtype=pl.UInt32)
            }).lazy()

        lookup_summer = make_lookup(ids_summer, "summer")
        lookup_winter = make_lookup(ids_winter, "winter")
        lookup_allyear = make_lookup(ids_allyear, "allyear")

        # Calculate Indices in Orders
        len_summer = len(ids_summer)
        len_winter = len(ids_winter)
        len_allyear = len(ids_allyear)

        orders_lf = orders_lf.with_columns([
            (pl.col("order_idx") % len_summer).cast(
                pl.UInt32).alias("idx_summer"),
            ((pl.col("order_idx") + 1) %
             len_winter).cast(pl.UInt32).alias("idx_winter"),
            ((pl.col("order_idx") + 2) %
             len_allyear).cast(pl.UInt32).alias("idx_allyear"),
        ])

        # Join to get Candidates
        # Valid joins because indices are mathematically guaranteed to be in range
        orders_lf = orders_lf.join(lookup_summer, on="idx_summer", how="left")
        orders_lf = orders_lf.join(lookup_winter, on="idx_winter", how="left")
        orders_lf = orders_lf.join(
            lookup_allyear, on="idx_allyear", how="left")

        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("target_tag") == "summer").then(
                pl.col("cand_summer"))
            .when(pl.col("target_tag") == "winter").then(pl.col("cand_winter"))
            .otherwise(pl.col("cand_allyear"))
            .alias("product_id")
        )

        # Drop temp columns to keep plan clean
        orders_lf = orders_lf.drop(["idx_summer", "idx_winter", "idx_allyear",
                                    "cand_summer", "cand_winter", "cand_allyear"])

        # 3.3 Customer Assignment (Pareto 80/20 Distribution)
        # Logic: 80% of orders assigned to first 20% of IDs (VIPs)
        #        20% of orders assigned to remaining 80% IDs (Casuals)

        # Calculate thresholds
        cutoff_vip_id = int(n_customers * 0.2)

        # 1. Random float for Pareto decision (Is this a VIP order?)
        # 2. Random Index within range
        orders_lf = orders_lf.with_columns([
            # pareto_rnd: 0-1
            pl.col("order_idx").hash(777).mod(1000).cast(
                pl.Float32).truediv(1000.0).alias("pareto_rnd"),

            # random index for VIP (0 to cutoff)
            # rnd.random_index_expr(cutoff_vip_id, "order_idx") replacement:
            (pl.col("order_idx").hash(101).mod(
                cutoff_vip_id).cast(pl.UInt32)).alias("idx_vip"),

            # random index for Casual (cutoff to N)
            # Explicit hash using order_idx (integer) to avoid float issues
            (pl.lit(cutoff_vip_id) + pl.col("order_idx").hash(202).mod(n_customers -
             cutoff_vip_id).cast(pl.UInt32)).alias("idx_casual")
        ])

        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("pareto_rnd") < 0.80)  # 80% prob -> VIP
            .then(pl.col("idx_vip"))
            .otherwise(pl.col("idx_casual"))
            .alias("cust_idx")
        )

        # Quantity
        orders_lf = orders_lf.with_columns(
            (pl.col("order_idx").hash(21).mod(100).map_elements(
                lambda x: 1 if x < 50 else (2 if x < 80 else (
                    3 if x < 90 else (4 if x < 95 else 5))),
                return_dtype=pl.UInt16
            )).alias("quantity")
        )

        # 4. RAW EXPORT DENORMALIZATION (Vuelco de ERP Sucio)

        # Join Products (Full Info)
        # Note: We join on product_id.
        orders_flat = orders_lf.join(
            products_lazy,
            on="product_id",
            how="left"
        )

        # Join Customers (Full Info)
        # Note: Customer LazyFrame needs a customer_id column?
        # Actually customer_lazy created in build() has row index "cust_idx".
        # We can join on cust_idx.

        # Debug Schema before final join
        # print("DEBUG: orders_lf Schema before final joins:")
        # print(orders_lf.schema)

        orders_flat = orders_flat.join(
            customers_lazy,
            on="cust_idx",
            how="left"
        )

        # Calculate Amounts (Total)
        # Dynamic Price: +/- 5% variance
        orders_flat = orders_flat.with_columns([
            # Price Factor 0.95 to 1.05
            ((pl.col("order_idx").hash(555).mod(100).cast(
                pl.Float32) / 1000.0) + 0.95).alias("price_factor")
        ])

        orders_flat = orders_flat.with_columns([
            (pl.col("quantity") * pl.col("base_price") * pl.col("price_factor")
             ).round(2).alias("total_amount")
        ])

        # Add String Noise (Dirty Data)
        orders_flat = chaos_eng.inject_string_noise(
            orders_flat, ["product_name", "first_name"])
        # orders_flat = chaos_eng.inject_nulls(
        #     orders_flat, ["seasonal_tag"])  # Sometimes tag is missing

        # Create final Customer mix name
        orders_flat = orders_flat.with_columns(
            (pl.col("first_name") + " " + pl.col("last_name")).alias("customer_name")
        )

        # Sort by Date (Chronological Log)
        orders_flat = orders_flat.sort("order_date")

        # SELECT FINAL COLUMNS (Massive Flat Table)
        final_table = orders_flat.select([
            "order_id",
            "order_date",

            # Customer Info (Redundant)
            "customer_id",
            "customer_name",
            "email",
            "phone_number",
            "city",   # From customer gen

            # Product Info (Redundant)
            "product_id",
            "category",
            "subtype",
            "brand",
            "product_name",
            "seasonal_tag",
            "base_price",  # Unit Price

            # Transaction Info
            "quantity",
            "total_amount"
        ])

        # 5. Pagos (Payments) - Restored Logic
        methods = ["Credit Card", "Debit Card",
                   "PayPal", "Bank Transfer", "Cash"]

        payments_lf = orders_lf.select([
            pl.col("order_id"),
            pl.col("order_idx"),
            pl.col("order_date")
        ])

        payments_lf = payments_lf.with_columns([
            pl.col("order_id").alias("payment_id"),
            (pl.col("order_date") + pl.duration(days=pl.col("order_idx").hash(101).mod(4))
             ).alias("payment_date"),
        ])

        payments_enriched = payments_lf.join(
            final_table.select(["order_id", "total_amount"]),
            on="order_id",
            how="inner"
        ).with_columns([
            (pl.col("order_idx").hash(102).mod(100).map_elements(
                lambda x: methods[0] if x < 40 else (
                    methods[1] if x < 70 else (
                        methods[2] if x < 85 else (
                            methods[3] if x < 95 else
                            methods[4]))), return_dtype=pl.String).alias("payment_method")),

            (pl.col("order_idx").hash(103).mod(100) < 95).alias("is_success")
        ]).with_columns(
            pl.when(pl.col("is_success")).then(pl.lit("COMPLETED"))
            .otherwise(pl.lit("FAILED")).alias("status")
        ).select([
            "payment_id", "order_id", "payment_date", "payment_method", "total_amount", "status"
        ])

        return {
            "customers": customers_lazy.drop("cust_idx"),
            "products": products_lazy.drop("prod_idx"),
            "orders": final_table,
            "payments": payments_enriched
        }
