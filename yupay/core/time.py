import polars as pl
from datetime import datetime, timedelta


class TimeEngine:
    """
    Manejo de fechas, estacionalidad y series de tiempo.
    """
    @staticmethod
    def random_dates(start_date: str, end_date: str, n: int) -> pl.Series:
        """
        Genera fechas aleatorias dentro un rango.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end - start

        # Usar timestamps para aleatoriedad
        start_ts = start.timestamp()
        end_ts = end.timestamp()
        span = end_ts - start_ts

        import random
        # Generación eficiente con list comprehension de Python
        # Para < 10M filas, esto es suficientemente rápido y evita cargar numpy (~200MB RAM)
        random_ts = [start_ts + (random.random() * span) for _ in range(n)]

        # Polars maneja la conversión de float/int a Datetime eficientemente
        # Cast a Datetime (usando microsegundos por defecto si es float)
        return pl.Series("date", random_ts).cast(pl.Datetime("ms"))
