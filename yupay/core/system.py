import shutil
import click


class DiskGuard:
    """
    Protege el sistema de llenarse el disco.
    Define umbrales de seguridad.
    """
    @staticmethod
    def get_free_space_gb(path: str = ".") -> float:
        total, used, free = shutil.disk_usage(path)
        return free / (1024**3)

    @staticmethod
    def check_space(estimated_bytes: int, threshold_gb: int = 20, path: str = ".") -> bool:
        """
        Verifica si hay suficiente espacio libre (threshold + estimado).
        """
        free_bytes = shutil.disk_usage(path).free
        needed_bytes = estimated_bytes + (threshold_gb * 1024**3)

        return free_bytes >= needed_bytes

    @staticmethod
    def estimate_size(rows: int, avg_row_bytes: int = 100) -> int:
        """
        Estimación heurística simple del tamaño del dataset.
        """
        return rows * avg_row_bytes
