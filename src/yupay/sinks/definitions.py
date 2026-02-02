import polars as pl
from pathlib import Path
from yupay.core.sink import BaseSink


class ParquetSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int, part_id: int = None) -> tuple[Path, int]:
        if part_id is not None:
            dir_path = self.root_path / name
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"part_{part_id}.parquet"
        else:
            file_path = self.root_path / f"{name}.parquet"

        # Validación JIT
        if not self.validate_space(rows_estimated, avg_row_bytes=300):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir Parquet.")

        # Escribir
        lazy_df.sink_parquet(file_path)

        # Conteo eficiente
        count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
        return file_path, count


class CsvSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int, part_id: int = None) -> tuple[Path, int]:
        if part_id is not None:
            dir_path = self.root_path / name
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"part_{part_id}.csv"
        else:
            file_path = self.root_path / f"{name}.csv"

        if not self.validate_space(rows_estimated, avg_row_bytes=150):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir CSV.")

        df = lazy_df.collect(streaming=True)
        count = df.height
        df.write_csv(file_path)
        return file_path, count


class DuckDBSink(BaseSink):
    def write(self, name: str, lazy_df: pl.LazyFrame, rows_estimated: int, part_id: int = None) -> tuple[Path, int]:
        db_path = self.root_path / "dataset.duckdb"

        if not self.validate_space(rows_estimated, avg_row_bytes=250):
            raise OSError(
                "Espacio en disco insuficiente (< 20GB libres) para escribir DuckDB.")

        import duckdb
        with duckdb.connect(str(db_path)) as con:
            df = lazy_df.collect(streaming=True)
            arrow_table = df.to_arrow()

            # Si part_id > 0, insertamos en lugar de crear
            if part_id is not None and part_id > 0:
                con.sql(f"INSERT INTO {name} SELECT * FROM arrow_table")
            else:
                con.sql(
                    f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM arrow_table")

            count = con.sql(f"SELECT count(*) FROM {name}").fetchone()[0]

        return db_path, count


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
