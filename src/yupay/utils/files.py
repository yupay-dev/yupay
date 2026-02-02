
import pathlib
import shutil
from typing import List, Dict, Any, Tuple


def get_dir_size(path: pathlib.Path) -> int:
    """Recursively calculates directory size in bytes."""
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def format_size(size_bytes: int) -> str:
    """Formats bytes to MB/GB."""
    if size_bytes > 1024**3:
        return f"{size_bytes / (1024**3):.2f} GB"
    return f"{size_bytes / (1024**2):.2f} MB"


def list_datasets(data_root: str = "data") -> Dict[str, List[Dict[str, Any]]]:
    """
    Scans data_root for datasets.
    Structure: data/[domain]/data_timestamp
    Returns: {domain: [{run_id, size_raw, size_fmt, path, date}]}
    """
    root = pathlib.Path(data_root)
    if not root.exists():
        return {}

    results = {}

    # Iterate over domains (directories in root)
    for domain_dir in [d for d in root.iterdir() if d.is_dir()]:
        domain_name = domain_dir.name
        runs = []

        # Iterate over runs (data_* folders)
        for run_dir in sorted(domain_dir.glob("data_*"), reverse=True):
            if not run_dir.is_dir():
                continue

            size = get_dir_size(run_dir)
            run_id = run_dir.name.replace("data_", "")

            runs.append({
                "run_id": run_id,
                "size_bytes": size,
                "size_formatted": format_size(size),
                "path": str(run_dir),
                "timestamp": run_dir.stat().st_mtime
            })

        if runs:
            results[domain_name] = runs

    return results


def delete_datasets(targets: List[pathlib.Path]) -> int:
    """Deletes the specified directories. Returns count of deleted items."""
    count = 0
    for t in targets:
        if t.exists() and t.is_dir():
            shutil.rmtree(t)
            count += 1
    return count
