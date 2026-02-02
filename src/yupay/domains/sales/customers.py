import polars as pl
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer


class CustomerGenerator(BaseGenerator):
    """
    Generador de clientes para el dominio de ventas.
    Usa catálogos para nombres realistas.
    """

    def generate(self, rows: int) -> pl.LazyFrame:
        """
        Genera clientes usando catálogos y probabilidades.
        """
        # Generación base (Eager para uso de numpy/randomizer rápido)
        # Convertimos a Lazy al final para cumplir el contrato
        # Cargar catálogo de nombres (inyectado desde el orquestador o cargado aquí)
        # En este diseño, asumimos que 'config' trae lo necesario o el generador sabe buscarlo
        # Para mantener pureza, mejor pasarlo en la config, pero por ahora simulamos carga simple o uso de listas

        rnd = Randomizer(seed=self.config.get("seed", 42))

        names_cfg = self.config.get("names_catalog", {})
        first_names = names_cfg.get("first_names", {}).get(
            "male", []) + names_cfg.get("first_names", {}).get("female", [])
        last_names = names_cfg.get("last_names", [])

        if not first_names:
            first_names = ["Juan", "Maria", "Jose", "Ana", "Luis"]  # Fallback
        if not last_names:
            last_names = ["Quispe", "Garcia",
                          "Rodriguez", "Flores"]  # Fallback

        email_domains = self.config.get("email_domains", ["gmail.com"])
        email_weights = self.config.get("email_weights", None)

        df = pl.DataFrame({
            "customer_id": pl.int_range(0, rows, dtype=pl.UInt32, eager=True),
            "first_name": rnd.sample_from_list(first_names, rows),
            "last_name": rnd.sample_from_list(last_names, rows),
            "email_domain": rnd.sample_from_list(email_domains, rows, weights=email_weights)
        })

        # Generar email basado en nombres
        df = df.with_columns(
            email=(pl.col("first_name").str.to_lowercase() + "." +
                   pl.col("last_name").str.to_lowercase() + "@" +
                   pl.col("email_domain"))
        ).drop("email_domain")

        return df.lazy()
