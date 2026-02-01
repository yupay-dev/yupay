from abc import ABC, abstractmethod
import polars as pl


class BaseDataset(ABC):
    """
    Orquestador de dominio. Coordina múltiples generadores para crear
    un conjunto de datos coherente (ej. Sales = Customers + Products + Orders).
    """
    @abstractmethod
    def build(self, config: dict, rows_map: dict[str, int]) -> dict[str, pl.LazyFrame]:
        """
        Orquesta la generación de múltiples tablas relacionadas.
        Retorna un diccionario de LazyFrames.
        """
        pass
