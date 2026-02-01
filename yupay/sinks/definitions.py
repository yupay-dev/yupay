import polars as pl
from pathlib import Path
from yupay.core.sink import BaseSink


class ParquetSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int) -> Path:
        file_path = self.root_path / f"{name}.parquet"

        # Validación JIT
        # Parquet comprime bien
        if not self.validate_space(rows_estimated, avg_row_bytes=300):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir Parquet.")

        # Optimización: sink_parquet usa streaming y no carga todo en RAM
        lazy_df.sink_parquet(file_path)
        return file_path


class CsvSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int) -> Path:
        file_path = self.root_path / f"{name}.csv"

        # CSV es más pesado
        if not self.validate_space(rows_estimated, avg_row_bytes=150):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir CSV.")

        # Polars LazyCSV writer no siempre soporta todas las ops, collect() suele ser necesario
        # pero streaming=True en collect ayuda
        lazy_df.collect(streaming=True).write_csv(file_path)
        return file_path


class DuckDBSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int) -> Path:
        # En DuckDB, generalmente queremos un solo archivo .duckdb con múltiples tablas
        # Para mantener el contrato de retornar un Path por "tabla", decidimos:
        # Opción A: Archivos separados (name.duckdb) -> Menos común.
        # Opción B: Un solo archivo (database.duckdb) y añadimos tablas.

        # Vamos con Opción B: Un archivo 'dataset.duckdb' en el root_path del run.
        db_path = self.root_path / "dataset.duckdb"

        # Estimado similar a Parquet
        if not self.validate_space(rows_estimated, avg_row_bytes=250):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir DuckDB.")

        # Polars ofrece integração directa via write_database pero requiere sqlalchemy o adbc
        # Una forma más nativa y "lazy" friendly es usar collect() y luego interactuar con duckdb
        # O usar sink_parquet intermedio? No, queremos directo.

        # Estrategia: collect() iterativo o streaming hacia tabla duckdb.
        # Por simplicidad y robustez v1: collect() -> write_database (via adbc o connectorx)
        # Ojo: write_database de polars usa sqlalchemy URI.
        # Alternativa más "custom": importar duckdb, conectar, y registro de arrow.

        import duckdb

        # Conexión persistente en modo 'append' no es trivial con objetos efímeros,
        # pero podemos abrir la conexión, registrar y ejecutar.

        with duckdb.connect(str(db_path)) as con:
            # Materializamos a Arrow (cero copia si es posible) o Streaming
            # O mejor: lazy_df.collect().write_database(...)
            # Pero para evitar dependencias extra (SQLAlchemy), usamos la API de Python de Duckdb:

            # Streaming via Arrow
            # lazy_df.sink_parquet(...) es lo más eficiente en memoria.
            # DuckDB puede leer parquet muy rápido.
            # Escribir directo a formato nativo DuckDB desde Polars Lazy sin memoria es tricky sin connectorx.

            # WORKAROUND EFICIENTE V1:
            # 1. Collect (con streaming interno de Polars si soporta)
            # 2. Convertir a Arrow
            # 3. DuckDB 'CREATE TABLE x AS SELECT * FROM arrow_table'

            # Nota: Si el dataset es gigante, esto puede saturar RAM.
            # Idealmente: sink_parquet -> duckdb import (pero duplica disco).

            # Vamos por el camino Arrow Stream (PyArrow necesario)
            df = lazy_df.collect(streaming=True)
            arrow_table = df.to_arrow()
            con.sql(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM arrow_table")

        return db_path


class SinkFactory:
    @staticmethod
    def get_sink(format: str, root_path: Path, validate_disk_space: bool = True) -> BaseSink:
        if format == "parquet":
            return ParquetSink(root_path, validate_disk_space)
        elif format == "csv":
            return CsvSink(root_path, validate_disk_space)
        elif format == "duckdb":
            return DuckDBSink(root_path, validate_disk_space)
        else:
            raise ValueError(f"Formato desconocido: {format}")
