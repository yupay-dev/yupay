import polars as pl
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer


class ProductGenerator(BaseGenerator):
    """
    Generador de catálogo de productos.
    """

    def generate(self, rows: int) -> pl.LazyFrame:
        rnd = Randomizer(seed=self.config.get("seed", 42) + 1)
        catalog = self.config.get("catalog", {})
        categories = list(catalog.keys())

        if not categories:
            # Fallback
            categories = ["General"]
            catalog = {"General": {"brands": ["Generic"], "nouns": [
                "Item"], "adjectives": ["Standard"], "price_factor": 1.0}}

        # Build flattened recipe list from catalog (Weighted by how many items we need?)
        # Actually for a fixed catalog size (rows), we just populate specific products.
        # But 'rows' comes from config 'products_catalog_size'.

        # New Logic: Generate 'rows' unique products.
        # We will pick a random Category -> Subtype (if exists) -> Components

        data_rows = []

        # Helper to safely get from dict
        def get_components(cat_ctx):
            return (
                cat_ctx.get("brands", ["Generic"]),
                cat_ctx.get("nouns", ["Product"]),
                cat_ctx.get("adjectives", ["Standard"]),
                cat_ctx.get("price_factor", 1.0),
                cat_ctx.get("tags", ["all_year"])
            )

        for _ in range(rows):
            cat_name = rnd.choice(categories)
            cat_data = catalog[cat_name]

            # Check for subtypes
            if "subtypes" in cat_data:
                subtype_data = rnd.choice(cat_data["subtypes"])
                sub_name = subtype_data.get("name", "General")
                brands, nouns, adjs, p_factor, tags = get_components(
                    subtype_data)
            else:
                sub_name = "General"
                brands, nouns, adjs, p_factor, tags = get_components(cat_data)

            # Generate Item
            brand = rnd.choice(brands)
            noun = rnd.choice(nouns)
            adj = rnd.choice(adjs)

            # Pick one tag from the list (usually just one)
            seasonal_tag = rnd.choice(tags) if tags else "all_year"

            name = f"{brand} {adj} {noun}"
            # Modified base for grocery realism
            base_price = (rnd.random() * 50 + 5) * p_factor

            data_rows.append((
                cat_name,
                sub_name,
                brand,
                name,
                seasonal_tag,
                round(base_price, 2)
            ))

        df = pl.DataFrame(data_rows, schema=[
            "category",
            "subtype",
            "brand",  # Added for raw export
            "product_name",
            "seasonal_tag",
            "base_price"
        ], orient="row")
        
        df = df.with_columns(
            product_id=pl.int_range(0, rows, dtype=pl.UInt32, eager=True),
            base_price=pl.col("base_price").cast(pl.Decimal(10, 2))
        )

        # Chaos Injection
        from yupay.core.chaos import ChaosEngine
        chaos = ChaosEngine(self.config)
        df_eager = chaos.apply(df, "products")

        return df_eager.lazy()
