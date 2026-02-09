import polars as pl
import random
from yupay.core.generator import BaseGenerator
from yupay.core.random import Randomizer

class StoreGenerator(BaseGenerator):
    """
    Generador de Tiendas (Stores) para el dominio de ventas.
    Simula una cadena de retail con presencia nacional.
    """

    def generate(self, rows: int = 50) -> pl.LazyFrame:
        """
        Genera un DataFrame de tiendas.
        Args:
            rows: Número de tiendas a generar (default 50).
        """
        rnd = Randomizer(seed=self.config.get("seed", 42) + 10)
        
        # Ciudades soportadas y sus pesos (Más tiendas en Lima)
        cities_cfg = {
            "Lima": {"region": "Costa", "weight": 0.5, "formats": ["Hipermercado", "Supermercado", "Express"]},
            "Arequipa": {"region": "Sierra", "weight": 0.15, "formats": ["Supermercado", "Express"]},
            "Trujillo": {"region": "Costa", "weight": 0.15, "formats": ["Supermercado", "Express"]},
            "Cusco": {"region": "Sierra", "weight": 0.1, "formats": ["Supermercado", "Express"]},
            "Piura": {"region": "Costa", "weight": 0.1, "formats": ["Supermercado", "Express"]},
        }
        
        cities_list = list(cities_cfg.keys())
        cities_weights = [cities_cfg[c]["weight"] for c in cities_list]

        # Generar lista de tiendas
        data = []
        
        for i in range(rows):
            store_id = i + 1
            # Use random directly for weighted choice and randint
            city = random.choices(cities_list, weights=cities_weights, k=1)[0]
            region = cities_cfg[city]["region"]
            fmt = random.choice(cities_cfg[city]["formats"])
            
            # Nombre realista
            name = f"Tienda {city} {fmt} {random.randint(100, 999)}"
            
            # Tamaño en m2 según formato
            if fmt == "Hipermercado":
                size = random.randint(4000, 10000)
            elif fmt == "Supermercado":
                size = random.randint(1000, 4000)
            else: # Express
                size = random.randint(100, 500)
                
            data.append({
                "store_id": store_id,
                "name": name,
                "city": city,
                "region": region,
                "format": fmt,
                "size_m2": size
            })

        df = pl.DataFrame(data)
        
        # Casting explícito importante para Polars
        df = df.with_columns([
            pl.col("store_id").cast(pl.UInt32),
            pl.col("size_m2").cast(pl.UInt32)
        ])

        return df.lazy()
