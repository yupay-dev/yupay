import polars as pl
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer


class ProductGenerator(BaseGenerator):
    """
    Generador de catálogo de productos.
    """

    def generate(self, rows: int) -> pl.LazyFrame:
        rnd = Randomizer(seed=self.config.get("seed", 42) + 1)

        # La config ahora debe venir con el catálogo de productos
        # config['catalog'] = { 'Electronics': { 'brands': [...], ... }, ... }
        catalog = self.config.get("catalog", {})
        categories = list(catalog.keys())

        if not categories:
            categories = ["General"]
            catalog = {"General": {"brands": ["Generic"], "nouns": [
                "Item"], "adjectives": ["Standard"], "price_factor": 1.0}}

        # Generar lista base de categorías para cada fila
        cats_column = rnd.sample_from_list(categories, rows)

        # Helper para construir nombres y precios
        # Polars no es ideal para lógica muy procedural fila por fila con dicts complejos,
        # pero para < 1M filas funciona bien con map_elements o listas de python pre-generadas.
        # Para eficiencia, generamos vectores NumPy por categoría y luego mezclamos,
        # o generamos listas de python y luego creamos el DataFrame.

        data_rows = []
        for _ in range(rows):
            cat = rnd.choice(categories)
            cat_data = catalog[cat]

            brand = rnd.choice(cat_data.get("brands", ["Generic"]))
            adj = rnd.choice(cat_data.get("adjectives", ["Standard"]))
            noun = rnd.choice(cat_data.get("nouns", ["Product"]))

            name = f"{brand} {adj} {noun}"
            base_price = (rnd.random() * 100 + 10) * \
                cat_data.get("price_factor", 1.0)

            data_rows.append((cat, name, round(base_price, 2)))

        df = pl.DataFrame(data_rows, schema=[
                          "category", "product_name", "base_price"], orient="row")

        df = df.with_columns(
            product_id=pl.int_range(0, rows, dtype=pl.UInt32, eager=True),
            base_price=pl.col("base_price").cast(pl.Decimal(10, 2))
        )

        return df.lazy()
