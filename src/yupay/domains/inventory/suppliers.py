import polars as pl
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer


class SupplierGenerator(BaseGenerator):
    """
    Generates B2B Suppliers for the Inventory Domain.
    """

    def generate(self, rows: int) -> pl.LazyFrame:
        rnd = Randomizer(seed=self.config.get("seed", 42))

        # Fallback Catalogs
        company_suffixes = ["S.A.", "S.A.C.",
                            "E.I.R.L.", "Ltda.", "Inc.", "Corp."]
        prefixes = ["Importaciones", "Distribuidora",
                    "Comercial", "Industrias", "Tecnología", "Logística"]
        base_names = ["Andina", "Del Sur", "Norte", "Global",
                      "Express", "Rapido", "Seguro", "Total", "Max", "Pro"]

        categories = ["Electronics", "Clothing",
                      "Home", "Food", "Toys", "Books"]
        countries = ["Peru", "Chile", "Colombia", "USA", "China"]

        # Explicit eager generation for random logic then lazy
        df = pl.DataFrame({
            "supplier_id": pl.int_range(0, rows, dtype=pl.UInt32, eager=True),
            "prefix": rnd.sample_from_list(prefixes, rows),
            "base": rnd.sample_from_list(base_names, rows),
            "suffix": rnd.sample_from_list(company_suffixes, rows),
            "category": rnd.sample_from_list(categories, rows),
            "country": rnd.sample_from_list(countries, rows)
        })

        # Concat name
        df = df.with_columns(
            name=(pl.col("prefix") + " " +
                  pl.col("base") + " " + pl.col("suffix"))
        ).drop(["prefix", "base", "suffix"])

        return df.lazy()
