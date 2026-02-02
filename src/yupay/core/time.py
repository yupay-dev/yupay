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

    @staticmethod
    def split_date_range(start_date: str, end_date: str, daily_avg: int, target_rows: int = 5_000_000) -> list[tuple[str, str]]:
        """
        Divide un rango de fechas en bloques basados en el volumen estimado.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Días necesarios para alcanzar target_rows
        days_per_batch = max(1, target_rows // max(1, daily_avg))

        batches = []
        current_start = start
        while current_start <= end:
            current_end = min(
                current_start + timedelta(days=days_per_batch - 1), end)
            batches.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))
            current_start = current_end + timedelta(days=1)

    @staticmethod
    def get_next_batch_window(current_start_date: str, end_date: str, daily_avg: int, target_rows: int = 5_000_000) -> tuple[str, str]:
        """
        Calcula la ventana de tiempo para el siguiente batch basado en target_rows.
        Retorna (batch_start, batch_end).
        """
        start = datetime.strptime(current_start_date, "%Y-%m-%d")
        limit = datetime.strptime(end_date, "%Y-%m-%d")

        # Días necesarios para alcanzar target_rows
        days_per_batch = max(1, target_rows // max(1, daily_avg))

        batch_end = min(start + timedelta(days=days_per_batch - 1), limit)

        return (
            start.strftime("%Y-%m-%d"),
            batch_end.strftime("%Y-%m-%d")
        )
