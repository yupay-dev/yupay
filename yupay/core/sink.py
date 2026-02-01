from abc import ABC, abstractmethod
import polars as pl
from pathlib import Path
from yupay.core.system import DiskGuard


class BaseSink(ABC):
    """
    Responsable de materializar (escribir) los datos.
    Toma decisiones de optimización (streaming vs collected) y valida espacio.
    """

    def __init__(self, root_path: Path, validate_disk_space: bool = True):
        self.root_path = root_path
        self.validate_disk_space = validate_disk_space

    def validate_space(self, rows: int, avg_row_bytes: int = 100) -> bool:
        """
        Validación 'Just-in-Time' antes de escribir.
        """
        if not self.validate_disk_space:
            return True
        estimated = DiskGuard.estimate_size(rows, avg_row_bytes)
        return DiskGuard.check_space(estimated, threshold_gb=20)

    @abstractmethod
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int) -> Path:
        """
        Escribe el LazyFrame al disco.
        """
        pass
