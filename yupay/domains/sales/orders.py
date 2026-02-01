import polars as pl
from yupay.core.dataset import BaseDataset
from yupay.domains.sales.customers import CustomerGenerator
from yupay.domains.sales.products import ProductGenerator
from yupay.core.time import TimeEngine
from yupay.core.random import Randomizer


class SalesDataset(BaseDataset):
    """
    Orquestador para el dominio de Ventas.
    Genera Clientes, Productos y Órdenes relacionadas.
    """

    def build(self, config: dict, rows_map: dict[str, int]) -> dict[str, pl.LazyFrame]:
        # 1. Generar Catálogos (Lazy)
        cust_gen = CustomerGenerator(config.get(
            "entities", {}).get("customers", {}))
        prod_gen = ProductGenerator(config.get(
            "entities", {}).get("products", {}))

        # Nota: Aquí materializamos (collect) BREVEMENTE para poder hacer sampling de IDs
        # Esto es un compromiso: Randomizer necesita listas de Python para consistencia.
        # Para optimización extrema, se debería hacer sampling numérico sin IDs strings primero,
        # pero para mantener lógica de negocio clara, usaremos collect() en dimensiones pequeñas.

        customers_lazy = cust_gen.generate(rows_map.get("customers", 1000))
        products_lazy = prod_gen.generate(rows_map.get("products", 100))

        # Materializamos SOLO IDs para el randomizer
        cust_ids = customers_lazy.select("customer_id").collect()[
            "customer_id"].to_list()
        prod_ids = products_lazy.select("product_id").collect()[
            "product_id"].to_list()

        # 2. Generar Órdenes (Core)
        order_rows = rows_map.get("orders", 10000)
        rnd = Randomizer(seed=config.get("seed", 42) + 2)
        time_eng = TimeEngine()

        # Sampling local (rápido para < 10M rows en memoria)
        sampled_cust_ids = rnd.sample_from_list(cust_ids, order_rows)
        sampled_prod_ids = rnd.sample_from_list(prod_ids, order_rows)

        # Crear LazyFrame base de órdenes
        orders_lf = pl.DataFrame({
            "order_id": [f"ORD-{i:08d}" for i in range(order_rows)],
            "order_date": time_eng.random_dates("2023-01-01", "2025-12-31", order_rows),
            "customer_id": sampled_cust_ids,
            "product_id": sampled_prod_ids,
            "quantity": rnd.sample_from_list([1, 2, 3, 4, 5], order_rows, weights=[0.5, 0.3, 0.1, 0.05, 0.05])
        }).lazy()

        # 3. Denormalización LAZY
        # El join ocurre en el motor de Polars, optimizado
        orders_enriched = orders_lf.join(
            products_lazy.select(["product_id", "base_price"]),
            on="product_id",
            how="left"
        ).with_columns(
            total_amount=pl.col("quantity") * pl.col("base_price")
        )

        return {
            "customers": customers_lazy,
            "products": products_lazy,
            "orders": orders_enriched
        }
