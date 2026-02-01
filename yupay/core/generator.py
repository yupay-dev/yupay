from abc import ABC, abstractmethod
import polars as pl


class BaseGenerator(ABC):
    """
    Contrato base para todos los generadores de tablas individuales.
    Cada generador es responsable de una sola entidad (ej. Clientes, Productos).
    """

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def generate(self, rows: int) -> pl.LazyFrame:
        """
        Retorna un LazyFrame de Polars.
        La ejecución real se difiere hasta que el Sink lo solicite.
        """
        pass
