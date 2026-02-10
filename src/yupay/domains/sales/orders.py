import polars as pl
from datetime import datetime, date, timedelta

from yupay.core.dataset import BaseDataset
from yupay.domains.sales.customers import CustomerGenerator
from yupay.domains.sales.products import ProductGenerator
from yupay.core.temporal import TimeEngine, TimeProfile
from yupay.core.random import Randomizer
from yupay.core.entropy import EntropyManager


class SalesDataset(BaseDataset):
    """
    Orquestador para el dominio de Ventas.
    Genera Clientes, Productos y Órdenes relacionadas.
    """

    def build(self, config: dict, rows_map: dict[str, int] = None, 
              start_date_override: str = None, end_date_override: str = None,
              stores_df: pl.DataFrame = None) -> dict[str, pl.LazyFrame]:
        
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
        
        # Entities
        n_customers = config.get("domains", {}).get(
            "sales", {}).get("customers_base", 10000)
        n_products = config.get("domains", {}).get(
            "sales", {}).get("products_catalog_size", 500)

        # 1. Generar Catálogos
        cust_gen = CustomerGenerator(config.get(
            "entities", {}).get("customers", {}))

        # Inject catalog into product config
        prod_config = config.get("entities", {}).get("products", {})
        prod_config["catalog"] = config.get("catalogs", {}).get("products", {})
        prod_gen = ProductGenerator(prod_config)

        # Pass stores to customer gen if available
        customers_lazy = cust_gen.generate(
            n_customers, stores_df=stores_df).with_row_index("cust_idx")
            
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
        if not ids_summer: ids_summer = [0]
        if not ids_winter: ids_winter = [0]
        if not ids_allyear: ids_allyear = range(n_products)

        # 2. Generar Órdenes (Temporal Engine Strategy)
        # 2.1 Configurar Perfil Retail Perú
        
        # Weekly: Mon-Wed (Quiet), Thu (Pickup), Fri-Sun (Peak)
        weekly_weights = [0.9, 0.9, 1.0, 1.05, 1.2, 1.3, 1.1]
        
        # Recurring Holidays (Month, Day) -> Factor
        holidays = {
            (1, 1): 0.5,    # New Year (Low)
            (2, 14): 1.4,   # Valentin
            (5, 1): 1.1,    # Labor Day
            (7, 28): 1.8,   # Fiestas Patrias
            (7, 29): 1.8,   # Fiestas Patrias
            (10, 31): 1.3,  # Halloween / Criolla
            (12, 25): 0.2,  # Christmas Day (Closed/Low)
            (12, 31): 1.5,  # New Year's Eve
        }
        
        
        # Dynamic Special Dates with Ramp-up
        special_dates = {}
        s_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        e_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        def add_ramp(dates_dict, target_date, days_before, start_factor, peak_factor):
            """Generates a linear ramp-up to a target date."""
            slope = (peak_factor - start_factor) / days_before
            for i in range(days_before + 1):
                day = target_date - timedelta(days=days_before - i)
                factor = start_factor + (slope * i)
                # Keep the highest factor if overlap
                current = dates_dict.get(day, 0.0)
                dates_dict[day] = max(current, factor)

        for year in range(s_date_obj.year, e_date_obj.year + 1):
            # 1. Mother's Day: 2nd Sunday of May
            may_1 = date(year, 5, 1)
            first_sunday = 1 + (6 - may_1.weekday()) 
            second_sunday = first_sunday + 7
            mothers_day = date(year, 5, second_sunday)
            
            # Ramp-up 7 days before (1.1x -> 3.0x)
            add_ramp(special_dates, mothers_day, 7, 1.1, 3.0)
            
            # 2. Fiestas Patrias: July 28-29
            # Ramp-up from July 15 (Gratificaciones) -> July 28
            fpatrias = date(year, 7, 28)
            add_ramp(special_dates, fpatrias, 13, 1.2, 2.5)
            special_dates[date(year, 7, 29)] = 2.5 # Day 2 also high
            
            # 3. Cyber Days (Simulated) - 3 Days Flat High
            # Mid-July (15-17)
            for d in [date(year, 7, 15), date(year, 7, 16), date(year, 7, 17)]:
                 special_dates[d] = max(special_dates.get(d, 0), 1.8)
            
            # Mid-Nov (14-16)
            for d in [date(year, 11, 14), date(year, 11, 15), date(year, 11, 16)]:
                 special_dates[d] = max(special_dates.get(d, 0), 1.8)

            # 4. Christmas: Ramp-up from Nov 20 -> Dec 24
            xmas_eve = date(year, 12, 24)
            # Long ramp from Nov 20 (approx 34 days)
            # Nov 20 (1.1x) -> Dec 24 (3.5x)
            nov_20 = date(year, 11, 20)
            days_ramp = (xmas_eve - nov_20).days
            add_ramp(special_dates, xmas_eve, days_ramp, 1.1, 3.5)

        retail_profile = TimeProfile(
            name="Retail Peru",
            weekly_weights=weekly_weights,
            holidays=holidays,
            special_dates=special_dates,
            trend_slope=0.005, # +0.5% per month approx
            enable_payday=True
        )

        time_eng = TimeEngine(start_date, end_date, daily_avg, profile=retail_profile)
        chaos_eng = EntropyManager(config)

        orders_lf = time_eng.expand_events(time_eng.generate_timeline())
        orders_lf = orders_lf.with_row_index("order_idx")

        # 3. Lógica Estacional: Asignar Tag Target -> Asignar Product ID
        VAL_TWO_PI = 6.28318530718
        VAL_PEAK_SUMMER = 45

        # Generar Factores de Estacionalidad Continua
        orders_lf = orders_lf.with_columns([
            pl.col("event_date").dt.ordinal_day().cast(pl.Int32).alias("doy"),
            pl.col("event_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("order_idx").hash(100).mod(1000).cast(pl.Float32).truediv(
                1000.0).alias("probs_rnd"),  # Roll de dado 0-1

            # Base logic columns
            pl.col("order_idx").cast(pl.UInt32).alias("order_id"),
            pl.col("event_date").cast(pl.Date).alias("order_date"),
        ])

        # Calcular Onda Estacional con "Chaos"
        orders_lf = orders_lf.with_columns([
            (pl.col("year").hash(2024).mod(31).cast(
                pl.Int32) - 15).alias("yearly_shift"),
            (pl.col("order_idx").hash(999).mod(200).cast(
                pl.Float32).truediv(1000.0) - 0.1).alias("daily_noise")
        ])

        orders_lf = orders_lf.with_columns([
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
        
        orders_lf = orders_lf.with_columns(
            pl.col("event_date").dt.weekday().alias("weekday")
        )

        orders_lf = orders_lf.with_columns([
            pl.when(pl.col("weekday") >= 6)
            .then(pl.col("p_summer") * 1.2)
            .otherwise(pl.col("p_summer"))
            .clip(0.0, 1.0)
            .alias("p_summer")
        ])

        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("probs_rnd") < pl.col(
                "p_summer")).then(pl.lit("summer"))
            .when(pl.col("probs_rnd") < (pl.col("p_summer") + pl.col("p_winter"))).then(pl.lit("winter"))
            .otherwise(pl.lit("all_year"))
            .alias("target_tag")
        )

        # Paso 3.2: Asignar IDs reales
        def make_lookup(ids_list, name_suffix):
            return pl.DataFrame({
                f"idx_{name_suffix}": pl.int_range(0, len(ids_list), dtype=pl.UInt32, eager=True),
                f"cand_{name_suffix}": pl.Series(ids_list, dtype=pl.UInt32)
            }).lazy()

        lookup_summer = make_lookup(ids_summer, "summer")
        lookup_winter = make_lookup(ids_winter, "winter")
        lookup_allyear = make_lookup(ids_allyear, "allyear")

        len_summer = len(ids_summer)
        len_winter = len(ids_winter)
        len_allyear = len(ids_allyear)

        orders_lf = orders_lf.with_columns([
            (pl.col("order_idx") % len_summer).cast(pl.UInt32).alias("idx_summer"),
            ((pl.col("order_idx") + 1) % len_winter).cast(pl.UInt32).alias("idx_winter"),
            ((pl.col("order_idx") + 2) % len_allyear).cast(pl.UInt32).alias("idx_allyear"),
        ])

        orders_lf = orders_lf.join(lookup_summer, on="idx_summer", how="left")
        orders_lf = orders_lf.join(lookup_winter, on="idx_winter", how="left")
        orders_lf = orders_lf.join(lookup_allyear, on="idx_allyear", how="left")

        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("target_tag") == "summer").then(pl.col("cand_summer"))
            .when(pl.col("target_tag") == "winter").then(pl.col("cand_winter"))
            .otherwise(pl.col("cand_allyear"))
            .alias("product_id")
        )

        orders_lf = orders_lf.drop(["idx_summer", "idx_winter", "idx_allyear",
                                    "cand_summer", "cand_winter", "cand_allyear"])

        # 3.3 Customer Assignment (Pareto 80/20 Distribution)
        cutoff_vip_id = int(n_customers * 0.2)

        orders_lf = orders_lf.with_columns([
            pl.col("order_idx").hash(777).mod(1000).cast(
                pl.Float32).truediv(1000.0).alias("pareto_rnd"),
            (pl.col("order_idx").hash(101).mod(
                cutoff_vip_id).cast(pl.UInt32)).alias("idx_vip"),
            (pl.lit(cutoff_vip_id) + pl.col("order_idx").hash(202).mod(n_customers -
             cutoff_vip_id).cast(pl.UInt32)).alias("idx_casual")
        ])

        orders_lf = orders_lf.with_columns(
            pl.when(pl.col("pareto_rnd") < 0.80)
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

        # 4. RAW EXPORT DENORMALIZATION
        orders_flat = orders_lf.join(products_lazy, on="product_id", how="left")

        # JOIN Customers to get preferred store
        # Note: customers_lazy has 'preferred_store_id' if stores_df was passed
        orders_flat = orders_flat.join(customers_lazy, on="cust_idx", how="left")

        # 4.1 Store Assignment Logic
        if "preferred_store_id" in orders_flat.collect_schema().names():
            # If we have stores, use preference logic
            # 80% use preferred, 20% random (simulate travel or other store in city) or just random overall?
            # Let's keep it simple: 90% Preferred, 10% Other.
            
            # To pick "Other", we need a valid store_id. 
            # We can pick a random store_id from the valid range [1, N_STORES].
            # We need to know max_store_id or have the frame.
            if stores_df is not None:
                max_store_id = stores_df.height
                
                orders_flat = orders_flat.with_columns([
                     pl.col("order_idx").hash(888).mod(1000).cast(pl.Float32).truediv(1000.0).alias("store_rnd"),
                     (pl.col("order_idx").hash(999).mod(max_store_id) + 1).cast(pl.UInt32).alias("random_store_id")
                ])
                
                orders_flat = orders_flat.with_columns(
                    pl.when(pl.col("store_rnd") < 0.90)
                    .then(pl.col("preferred_store_id"))
                    .otherwise(pl.col("random_store_id"))
                    .alias("store_id")
                )
                 
                # Fill nulls (if any customer has no preference) with random
                orders_flat = orders_flat.with_columns(
                    pl.col("store_id").fill_null(pl.col("random_store_id"))
                )

                # Generate POS ID and Cashier
                orders_flat = orders_flat.with_columns([
                    # POS: StoreID-BoxNumber (1..8)
                    (pl.col("store_id").cast(pl.String) + "-" + 
                     (pl.col("order_idx").hash(11).mod(8) + 1).cast(pl.String)).alias("pos_id"),
                     
                    # Cashier: StoreID-User (1..20)
                    (pl.col("store_id").cast(pl.String) + "-" + 
                     (pl.col("order_idx").hash(22).mod(20) + 1).cast(pl.String)).alias("cashier_id")
                ])
            else:
                 # Should not happen in this phase if wired correctly
                 orders_flat = orders_flat.with_columns(pl.lit(1).alias("store_id"))
        else:
             orders_flat = orders_flat.with_columns(pl.lit(1).alias("store_id"))

        # Calculate Amounts (Total)
        orders_flat = orders_flat.with_columns([
            ((pl.col("order_idx").hash(555).mod(100).cast(
                pl.Float32) / 1000.0) + 0.95).alias("price_factor")
        ])

        orders_flat = orders_flat.with_columns([
            (pl.col("quantity") * pl.col("base_price") * pl.col("price_factor")
             ).round(2).alias("total_amount")
        ])

        # Add String Noise
        orders_flat = chaos_eng.inject_string_noise(
            orders_flat, ["product_name", "first_name"])

        # Create final Customer mix name
        orders_flat = orders_flat.with_columns(
            (pl.col("first_name") + " " + pl.col("last_name")).alias("customer_name")
        )

        orders_flat = orders_flat.sort("order_date")

        # SELECT FINAL COLUMNS
        # Define columns to select (robust check if they exist)
        cols = [
            "order_id",
            "order_date",
            "customer_id",
            "customer_name",
            "email",
            "phone_number",
            "city",
            "product_id",
            "category",
            "subtype",
            "brand",
            "product_name",
            "seasonal_tag",
            "base_price",
            "quantity",
            "total_amount"
        ]
        
        # Add store cols if exist
        avail_cols = orders_flat.collect_schema().names()
        if "store_id" in avail_cols:
            cols.extend(["store_id", "pos_id", "cashier_id"])
            
        final_table = orders_flat.select(cols)

        # 5. Pagos (Payments)
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
