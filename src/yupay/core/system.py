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


class MemoryGuard:
    # Baseline RAM metrics
    _baseline_process_rss_gb = None
    _peak_rss_gb = 0.0
    _peak_system_pct = 0.0
    _ram_total_gb = None
    _initialized = False

    @staticmethod
    def initialize_budget():
        """
        Captura el estado inicial de la RAM para definir el 'presupuesto' de Yupay.
        """
        import psutil
        import os
        vm = psutil.virtual_memory()
        proc = psutil.Process(os.getpid())

        MemoryGuard._ram_total_gb = vm.total / (1024**3)
        MemoryGuard._baseline_used_pct = vm.percent
        MemoryGuard._baseline_available_gb = vm.available / (1024**3)
        MemoryGuard._baseline_process_rss_gb = proc.memory_info().rss / (1024**3)
        MemoryGuard._peak_rss_gb = MemoryGuard._baseline_process_rss_gb
        MemoryGuard._peak_system_pct = vm.percent
        MemoryGuard._initialized = True

    @staticmethod
    def get_ram_usage_pct() -> float:
        import psutil
        pct = psutil.virtual_memory().percent
        if pct > MemoryGuard._peak_system_pct:
            MemoryGuard._peak_system_pct = pct
        return pct

    @staticmethod
    def get_process_rss_gb() -> float:
        import psutil
        import os
        rss_gb = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
        # Actualizar pico propio de Yupay
        if rss_gb > MemoryGuard._peak_rss_gb:
            MemoryGuard._peak_rss_gb = rss_gb
        return rss_gb

    @staticmethod
    def get_budget_usage_pct() -> float:
        """
        Calcula qué porcentaje del presupuesto disponible al inicio está usando Yupay.
        """
        if not MemoryGuard._initialized:
            return 0.0

        # El presupuesto real es lo que estaba libre.
        # El consumo de Yupay lo medimos por su RSS actual menos el inicial.
        process_usage = MemoryGuard.get_process_rss_gb(
        ) - MemoryGuard._baseline_process_rss_gb

        return (process_usage / MemoryGuard._baseline_available_gb) * 100

    @staticmethod
    def get_drift() -> float:
        """
        Calcula cuánto ha cambiado la carga base del sistema (externa a Yupay).
        Retorna la diferencia en GB (positivo si aumentó la presión externa).
        """
        if not MemoryGuard._initialized:
            return 0.0

        import psutil
        vm = psutil.virtual_memory()
        current_available_gb = vm.available / (1024**3)

        # La caída total en RAM disponible
        total_drop = MemoryGuard._baseline_available_gb - current_available_gb
        # Lo que Yupay dice que está usando
        yupay_usage = MemoryGuard.get_process_rss_gb() - MemoryGuard._baseline_process_rss_gb

        # El resto es 'drift' (carga base que cambió)
        return total_drop - yupay_usage

    @staticmethod
    def get_status() -> str:
        """
        Determina el estado según presupuesto y límites globales.
        🟡 60-80% budget (Obs) | 🟠 80-90% budget (Throttle) | 🔴 > 90% budget o > 95% global
        """
        global_usage = MemoryGuard.get_ram_usage_pct()
        budget_usage = MemoryGuard.get_budget_usage_pct()

        if global_usage >= 95.0:
            return "GLOBAL_HARD_STOP"

        if budget_usage >= 90.0:
            return "BUDGET_ABORT"

        if budget_usage >= 80.0:
            return "BUDGET_WARNING"

        if budget_usage >= 60.0:
            return "OBSERVATION"

        return "NORMAL"

    @staticmethod
    def wait_if_critical(threshold_pct: float = 80.0, wait_seconds: int = 5):
        """
        Si el presupuesto está en WARNING o superior, intenta liberar y esperar.
        """
        import gc
        import time
        if MemoryGuard.get_budget_usage_pct() > threshold_pct:
            gc.collect()
            time.sleep(wait_seconds)
            return True
        return False
