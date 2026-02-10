import polars as pl
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer


class CustomerGenerator(BaseGenerator):
    """
    Generador de clientes para el dominio de ventas.
    Usa catálogos para nombres realistas.
    """

    def generate(self, rows: int, stores_df: pl.DataFrame = None) -> pl.LazyFrame:
        """
        Genera clientes usando catálogos y probabilidades.
        Args:
            rows: Cantidad de clientes.
            stores_df: DataFrame (Eager) con la dimensión de tiendas para asignar preferencias.
        """
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

        # Cities Strategy: If stores provided, respect their cities distribution or just plain list
        if stores_df is not None:
             # Extract unique cities from stores to ensure alignment
            available_cities = stores_df["city"].unique().to_list()
            # We could weight by number of stores per city, but for now uniform or config based
            cities = self.config.get("cities", available_cities)
            # Ensure intersection
            cities = [c for c in cities if c in available_cities]
            if not cities:
                cities = available_cities
        else:
            cities = self.config.get(
                "cities", ["Lima", "Arequipa", "Trujillo", "Cusco", "Piura"])

        df = pl.DataFrame({
            "customer_id": pl.int_range(0, rows, dtype=pl.UInt32, eager=True),
            "first_name": rnd.sample_from_list(first_names, rows),
            "last_name": rnd.sample_from_list(last_names, rows),
            "email_domain": rnd.sample_from_list(email_domains, rows, weights=email_weights),
            "city": rnd.sample_from_list(cities, rows)
        })

        # Generar Phone Number determinístico y rápido en Polars puramente
        df = df.with_columns(
            phone_number=pl.lit("9") +
            pl.col("customer_id").hash(42).mod(
                90000000).add(10000000).cast(pl.String)
        )

        # Generar email basado en nombres
        df = df.with_columns(
            email=(pl.col("first_name").str.to_lowercase() + "." +
                   pl.col("last_name").str.to_lowercase() + "@" +
                   pl.col("email_domain"))
        ).drop("email_domain")

        # Assign Preferred Store
        if stores_df is not None:
            # Logic: Match customer city to a store in that city
            # We assign weighted by store count in city?
            # Simple approach: Join locally to assign "preferred_store_id"
            
            # 1. Rank stores per city: 0..N
            stores_ranked = stores_df.with_columns([
                pl.col("store_id").cum_count().over("city").cast(pl.UInt32).alias("store_idx_in_city"),
                pl.count("store_id").over("city").cast(pl.UInt32).alias("city_store_count")
            ])
            
            # 2. Add 'city_store_count' to customers (Join on City)
            # We extract unique counts per city
            city_counts = stores_ranked.select(["city", "city_store_count"]).unique()
            df = df.join(city_counts, on="city", how="left")
            
            # 3. Generate Random Index [0, count)
            df = df.with_columns(
                (pl.col("customer_id").hash(999).mod(pl.col("city_store_count"))).alias("target_store_idx")
            )
            
            # 4. Join back to get StoreID
            target_stores = stores_ranked.select(["city", "store_idx_in_city", "store_id"])
            
            # Lazy join logic (Store assignment)
            if isinstance(target_stores, pl.LazyFrame):
                 target_stores = target_stores.collect()

            df = df.join(
                target_stores, 
                left_on=["city", "target_store_idx"], 
                right_on=["city", "store_idx_in_city"], 
                how="left"
            ).rename({"store_id": "preferred_store_id"}).drop(["city_store_count", "target_store_idx"])

        # Chaos Injection
        from yupay.core.chaos import ChaosEngine
        chaos = ChaosEngine(self.config)
        
        # Apply chaos (df is already Eager)
        df_eager = chaos.apply(df, "customers")
        
        return df_eager.lazy()
