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

        # Estimación / Configuración de Entidades
        # En el futuro esto vendrá de un "CustomerGrowthProfile"
        # Por ahora, asumimos una base de clientes proporcional al volumen o fija
        n_customers = config.get("domains", {}).get(
            "sales", {}).get("customers_base", 10000)
        n_products = config.get("domains", {}).get(
            "sales", {}).get("products_catalog_size", 500)

        # 1. Generar Catálogos (Lazy)
        cust_gen = CustomerGenerator(config.get(
            "entities", {}).get("customers", {}))
        prod_gen = ProductGenerator(config.get(
            "entities", {}).get("products", {}))

        # Generamos entidades y añadimos índice para el join posterior
        customers_lazy = cust_gen.generate(
            n_customers).with_row_index("cust_idx")
        products_lazy = prod_gen.generate(
            n_products).with_row_index("prod_idx")

        # 2. Generar Órdenes (Temporal Engine Strategy)
        rnd = Randomizer(seed=config.get("seed", 42) + 2)

        # Inicializamos el motor temporal y de entropía
        time_eng = TimeEngine(start_date, end_date, daily_avg)
        chaos_eng = EntropyManager(config)

        # Generamos el esqueleto temporal (Lazy)
        orders_lf = time_eng.expand_events(time_eng.generate_timeline())
        orders_lf = orders_lf.with_row_index("order_idx")

        # Generamos columnas base
        orders_lf = orders_lf.with_columns([
            pl.col("order_idx").cast(pl.UInt32).alias("order_id"),
            pl.col("event_date").cast(pl.Date).alias("order_date"),

            # Foreign Keys (Indices) - Inicialmente limpios
            rnd.random_index_expr(n_customers, "order_idx").alias("cust_idx"),
            rnd.random_index_expr(n_products, "order_idx").alias("prod_idx"),

            (pl.col("order_idx").hash(21).mod(100).map_elements(
                lambda x: 1 if x < 50 else (2 if x < 80 else (
                    3 if x < 90 else (4 if x < 95 else 5))),
                return_dtype=pl.UInt16
            )).alias("quantity")
        ])

        # --- PHASE 3: ENTROPY INJECTION ---
        # 1. Orphans: Romper integridad referencial de clientes
        orders_lf = chaos_eng.inject_orphans(
            orders_lf, "cust_idx", max_id=n_customers)

        # 2. Nulls: Simular errores en fechas (ej. sistemas caídos)
        # Nota: Data real de ordenes raramente tiene fecha nula, pero puede pasar en migraciones
        orders_lf = chaos_eng.inject_nulls(orders_lf, ["order_date"])

        # 3. Enriquecimiento y Denormalización (Orders)
        # Link Customer Name instead of ID
        orders_enriched = orders_lf.join(
            customers_lazy.select(
                ["cust_idx", "customer_id", "first_name", "last_name"]),
            on="cust_idx",
            how="left"
        ).with_columns(
            (pl.col("first_name") + " " + pl.col("last_name")).alias("customer_name")
        ).drop(["first_name", "last_name"])

        # Inject String Noise to Customer Name
        orders_enriched = chaos_eng.inject_string_noise(
            orders_enriched, ["customer_name"])

        # Link Product Info (Keep ID and Name mostly, but user said 'no ids')
        # Let's keep product_id as it's often a SKU string, but here it's int.
        # Ideally we'd map product names too, but let's stick to Customer/Supplier for now as requested examples.

        # Calculate Amounts
        orders_enriched = orders_enriched.join(
            products_lazy.select(["prod_idx", "product_id", "base_price"]),
            on="prod_idx",
            how="left"
        ).with_columns([
            (pl.col("quantity") * pl.col("base_price")
             ).cast(pl.Decimal(10, 2)).alias("total_amount")
        ]).select([
            "order_id", "order_date",
            "customer_name",  # Replaces customer_id
            "product_id", "quantity", "total_amount"
        ])

        # 4. Pagos (Payments)
        # Relación 1:1 o 1:N con órdenes. Para MVP hacemos 1:1 simple.
        # Payment Date = Order Date + Random Delta (0-3 days)
        # Payment Method = Weighted Random

        methods = ["Credit Card", "Debit Card",
                   "PayPal", "Bank Transfer", "Cash"]

        payments_lf = orders_lf.select([
            pl.col("order_id"),
            pl.col("order_idx"),  # Used for deterministic seeding
            pl.col("order_date")
        ])

        payments_lf = payments_lf.with_columns([
            # For 1:1, reusing ID is easiest. Or map it.
            pl.col("order_id").alias("payment_id"),

            # Payment Date
            (pl.col("order_date") + pl.duration(days=pl.col("order_idx").hash(101).mod(4)  # 0 to 3 days delay
                                                )).alias("payment_date"),

            # Amount (We need total_amount from enriched, but this is Lazy.
            # We can re-calculate or join. Joining enriched is safer if we want exact match.)
            # Wait, we need to join with enriched orders to get amount.
        ])

        # Helper to get amount from indices
        # Optimization: We already have prod_idx and quantity in orders_lf,
        # but base_price is in products_lazy.
        # A simple approach is generating payments AFTER enriched join, but that requires materialized dataframe?
        # No, LazyFrame joins work.

        # Simplified: We treat Payments as a separate process that 'knows' the amount
        # by joining with the final orders logic.

        payments_enriched = payments_lf.join(
            orders_enriched.select(["order_id", "total_amount"]),
            on="order_id",
            how="inner"
        ).with_columns([
            # Payment Method
            (pl.col("order_idx").hash(102).mod(100).map_elements(
                lambda x: methods[0] if x < 40 else (    # 40% CC
                    methods[1] if x < 70 else (    # 30% DC
                        methods[2] if x < 85 else (    # 15% PP
                            methods[3] if x < 95 else      # 10% Transfer
                            methods[4])))                  # 5% Cash
                , return_dtype=pl.String).alias("payment_method")),

            # Status: 95% Success, 5% Failed
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
            "orders": orders_enriched,
            "payments": payments_enriched
        }
