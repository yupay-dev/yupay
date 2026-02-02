import pathlib
import datetime
import shutil
from typing import List, Dict


class OutputManager:
    """
    Gestiona la estructura de carpetas de salida.
    root/
      domain/
        data_YYYYMMDD_HHMMSS/
    """

    def __init__(self, root_path: str = "./data"):
        self.root = pathlib.Path(root_path)

    def create_run_dir(self, domain: str) -> pathlib.Path:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = self.root / domain / f"data_{timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def list_runs(self, domain: str = None) -> Dict[str, List[pathlib.Path]]:
        """
        Lista los datasets existentes.
        Retorna {domain: [path1, path2]}
        """
        results = {}

        domains_to_scan = [
            self.root / domain] if domain else [d for d in self.root.iterdir() if d.is_dir()]

        for d_path in domains_to_scan:
            if not d_path.exists():
                continue
            runs = sorted([p for p in d_path.glob("data_*")
                          if p.is_dir()], reverse=True)
            if runs:
                results[d_path.name] = runs

        return results

    def clean(self, domain: str = "all", run_id: str = None) -> int:
        """
        Elimina datasets.
        domain='all' -> borra todo.
        domain='sales' -> borra todo sales.
        run_id -> borra carpeta específica.
        """
        deleted_count = 0

        if domain == "all":
            if self.root.exists():
                shutil.rmtree(self.root)
                self.root.mkdir()  # recrear root vacío
                return 1  # cuenta como 1 gran borrado
            return 0

        domain_path = self.root / domain
        if not domain_path.exists():
            return 0

        if run_id:
            # Borrar run específico
            target = domain_path / run_id
            if target.exists():
                shutil.rmtree(target)
                deleted_count = 1
        else:
            # Borrar todo el dominio
            shutil.rmtree(domain_path)
            deleted_count = 1

        return deleted_count
