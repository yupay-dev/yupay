from datetime import datetime, timedelta
import pathlib
import click
import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm

from yupay.core.settings import Settings
from yupay.core.system import DiskGuard, MemoryGuard
from yupay.core.filesystem import OutputManager
from yupay.core.time import TimeEngine
from yupay.domains.sales.orders import SalesDataset

console = Console()


@click.group()
def main():
    """Yupay: Realistic Synthetic ERP Data Generator"""
    pass


@main.command()
@click.argument("domain")
def generate(domain):
    """Generate synthetic data using YAML configuration (config/main.yaml)."""

    # 0. Imports Lazy (to avoid circularity or early load)
    from yupay.core.estimator import SizeEstimator
    from yupay.sinks.definitions import SinkFactory

    # 1. Cargar Configuración
    settings = Settings()
    try:
        # Load hierarchy: Defaults -> Domain -> User (main.yaml)
        defaults = settings.load_defaults()
        domain_config = settings.load_domain(domain)
        user_config = settings.load_user_config()

        # Merge: defaults -> domain -> user
        config_step1 = settings.merge_configs(defaults, domain_config)
        full_config = settings.merge_configs(config_step1, user_config)

    except FileNotFoundError as e:
        console.print(f"[bold red]Error de configuración:[/bold red] {e}")
        return

    # 2. Setup Output Strictness
    # "Config-as-Code": We rely SOLELY on main.yaml
    full_config.setdefault("output_path", "data")
    full_config.setdefault("output_format", "csv")

    # Force? No, we trust the user config limits. If they want to force,
    # they raise the limits in yaml.

    # OUTPUT STRUCTURE: data/[domain]/data_YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_output = pathlib.Path(full_config["output_path"]) / domain
    run_dir = base_output / f"data_{timestamp}"

    # Resource Check using full path assumption
    # Note: SizeEstimator checks 'output_path' in config, we should sync it
    full_config["output_path"] = str(
        full_config["output_path"])  # Ensure string

    # 3. Estimación y Seguridad (Resource Guard)
    console.print(
        "[yellow]Analizando recursos y estimando volumen...[/yellow]")
    estimator = SizeEstimator(full_config, defaults)
    try:
        stats = estimator.validate_and_estimate(domain=domain)
        console.print(
            f"[green]✔ Estimación Aprobada:[/green] {stats['days']} días | "
            f"~{stats['total_rows_estimated']:,} filas | "
            f"~{stats['estimated_size_gb']:.2f} GB"
        )
    except (ValueError, OSError) as e:
        console.print(f"[bold red]⛔ RECHAZADO POR SEGURIDAD:[/bold red] {e}")
        return

    # 4. Preparación de Salida
    out_mgr = OutputManager(root_path=full_config["output_path"])
    run_dir = out_mgr.create_run_dir(domain)

    # Inicializar Presupuesto de RAM
    MemoryGuard.initialize_budget()
    budget_gb = MemoryGuard._baseline_available_gb
    console.print(
        f"[dim]Initial RAM Load: {MemoryGuard._baseline_used_pct:.1f}% | Budget assigned: {budget_gb:.2f} GB[/dim]")

    try:
        final_format = full_config.get("output_format", "csv")
        sink = SinkFactory.get_sink(
            final_format, run_dir, validate_disk_space=True)
    except ValueError as e:
        console.print(f"[red]Error de formato:[/red] {e}")
        return

    console.print(
        f"[bold green]Iniciando Motor Temporal para:[/bold green] [bold]{domain}[/bold]")
    console.print(f"📂 Salida: [blue]{run_dir}[/blue]")

    # 5. Orquestación y Escritura
    with console.status(f"[bold green]Simulando transacciones...[/bold green]", spinner="dots") as status:
        if domain == "sales":
            from yupay.core.erp import ERPDataset
            dataset = ERPDataset()

            # 1. Config Context
            locale_data = settings.load_locale(
                full_config.get("locale", "es_PE"))
            if "entities" in full_config and "customers" in full_config["entities"]:
                full_config["entities"]["customers"]["names_catalog"] = locale_data.get(
                    "names", {})

            if "inventory" not in full_config.get("domains", {}):
                full_config.setdefault("domains", {})["inventory"] = {
                    "suppliers_base": full_config.get("domains", {}).get("sales", {}).get("customers_base", 1000) // 10
                }

            # 2. Decision: Batching or Monolithic?
            # Umbral: 5M de filas transaccionales (Orders)
            total_trans_est = stats["total_rows_estimated"]
            use_batching = total_trans_est > 5_000_000

            results = []

            # 3. Paso 1: Dimensiones (Siempre en memoria, son pequeñas)
            dims_lazy = dataset.build_dimensions(full_config)
            for table_name, lf in dims_lazy.items():
                print(f"   -> Escribiendo Dimensión: {table_name}")
                est = 100000 if table_name == "customers" else 5000
                out_path, real_count = sink.write(table_name, lf, est)
                results.append((table_name, real_count, out_path.name))

            # 4. Paso 2: Transacciones
            if not use_batching:
                # Monolítico
                trans_lazy = dataset.build_batch(full_config)
                for table_name, lf in trans_lazy.items():
                    budget = MemoryGuard.get_budget_usage_pct()
                    sys_ram = MemoryGuard.get_ram_usage_pct()
                    status.update(
                        f"[bold green]Simulando transacciones Monolíticas... [blue]RAM Budget: {budget:.1f}%[/blue] | [dim]SYS: {sys_ram:.1f}%[/dim][/bold green]")
                    print(
                        f"   -> Planificando tabla (Monolítico): {table_name}")
                    est = total_trans_est if table_name != "stock_movements" else total_trans_est // 10
                    out_path, real_count = sink.write(table_name, lf, est)
                    results.append((table_name, real_count, out_path.name))
            else:
                # 4. BATCHING PROACTIVO E INTELIGENTE (Budget-Aware)
                print(
                    f"   -> ESCALADO DETECTADO: Usando gestión dinámica de presupuesto de RAM.")

                current_start = full_config["start_date"]
                end_limit = full_config["end_date"]
                target_rows = 5_000_000
                batch_idx = 0
                stable_count = 0  # Inercia para recuperación

                while datetime.strptime(current_start, "%Y-%m-%d") <= datetime.strptime(end_limit, "%Y-%m-%d"):
                    # A. Evaluación Proactiva (Budget-Aware)
                    ram_status = MemoryGuard.get_status()
                    current_ram = MemoryGuard.get_ram_usage_pct()
                    budget_usage = MemoryGuard.get_budget_usage_pct()
                    drift_gb = MemoryGuard.get_drift()

                    # Actualizar estado visual dinámico
                    status.update(
                        f"[bold green]Simulando transacciones... [blue]RAM Budget: {budget_usage:.1f}%[/blue] | [dim]SYS: {current_ram:.1f}%[/dim][/bold green]")

                    # Log de Drift si es significativo (> 0.5 GB)
                    if abs(drift_gb) > 0.5:
                        if drift_gb > 0:
                            console.print(
                                f"[dim grey]ℹ️ Desviación externa: [bold red]▲ +{drift_gb:.2f} GB[/bold red] (Carga del sistema)[/dim grey]")
                        else:
                            console.print(
                                f"[dim grey]ℹ️ Desviación externa: [bold green]▼ {drift_gb:.2f} GB[/bold green] (Liberación RAM)[/dim grey]")

                    if ram_status == "GLOBAL_HARD_STOP":
                        console.print(
                            f"\n[bold red]❌ STOP GLOBAL (AIRBAG): RAM del sistema al {current_ram:.1f}%.[/bold red]")
                        console.print(
                            "[red]Abortando para prevenir congelamiento de Windows.[/red]")
                        return

                    elif ram_status == "BUDGET_ABORT":
                        console.print(
                            f"\n[bold red]❌ LÍMITE DE SEGURIDAD: Yupay alcanzó el {budget_usage:.1f}% de su presupuesto.[/bold red]")
                        console.print(
                            f"[red]RAM Global: {current_ram:.1f}% | Presupuesto Inicial: {MemoryGuard._baseline_available_gb:.2f} GB[/red]")
                        console.print(
                            "[white]Acción sugerida: Reduce 'daily_avg_transactions' o cierra aplicaciones pesadas.[/white]")
                        return

                    elif ram_status == "BUDGET_WARNING":
                        # Acción: Throttling proactivo
                        stable_count = 0  # Reset inercia
                        old_rows = target_rows
                        target_rows = max(500_000, target_rows // 2)
                        console.print(
                            f"[orange3]🟠 Throttle: Presupuesto al {budget_usage:.1f}%. Ajustando batch: {old_rows:,} -> {target_rows:,}[/orange3]")
                        MemoryGuard.wait_if_critical(
                            threshold_pct=80.0)  # Pausa y GC

                    elif ram_status == "OBSERVATION":
                        # Reset inercia (no crecemos si estamos bajo observación)
                        stable_count = 0
                        console.print(
                            f"[yellow]🟡 Observación: Presupuesto al {budget_usage:.1f}%.[/yellow]")

                    else:
                        # NORMAL: Recuperación gradual con Hysteresis (3 batches estables)
                        if target_rows < 5_000_000:
                            stable_count += 1
                            if stable_count >= 3:
                                old_rows = target_rows
                                target_rows = min(
                                    5_000_000, int(target_rows * 1.5))
                                console.print(
                                    f"[green]🟢 Recuperación: {old_rows:,} -> {target_rows:,} filas tras 3 batches estables.[/green]")
                                stable_count = 0

                    # B. Cálculo de ventana JIT
                    s_date, e_date = TimeEngine.get_next_batch_window(
                        current_start, end_limit, full_config["daily_avg_transactions"], target_rows
                    )

                    print(
                        f"   -> Batch {batch_idx + 1}: [{s_date} - {e_date}] @ {target_rows:,} rows")

                    # C. Generación y Escritura
                    trans_batch = dataset.build_batch(
                        full_config, s_date, e_date)

                    for table_name, lf in trans_batch.items():
                        out_path, real_count = sink.write(
                            table_name, lf, target_rows, part_id=batch_idx)

                        # Acumular resultados
                        found = False
                        for idx, (name, count, fname) in enumerate(results):
                            if name == table_name:
                                results[idx] = (
                                    name, count + real_count, "Folder (Partitioned)")
                                found = True
                                break
                        if not found:
                            results.append(
                                (table_name, real_count, f"{table_name}/"))

                    # D. Avanzar puntero
                    current_start = (datetime.strptime(
                        e_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    batch_idx += 1
        else:
            console.print(
                f"[red]Dominio {domain} no implementado actualmente.[/red]")
            return

    # 6. Resumen (Restored)
    table = Table(title="Resumen de Generación")
    table.add_column("Tabla", style="cyan")
    table.add_column("Filas", justify="right")
    table.add_column("Archivo", style="green")

    for t_name, count, fname in results:
        table.add_row(t_name, f"{count:,}", fname)

    console.print(table)
    peak_gb = MemoryGuard._peak_rss_gb
    peak_sys = MemoryGuard._peak_system_pct
    console.print(Panel(
        f"✅ Proceso completado exitosamente.\nRuta: {run_dir}\n[dim]Peak RAM: Yupay {peak_gb:.2f} GB | Sistema {peak_sys:.1f}%[/dim]",
        style="bold green"
    ))


# --- TOOLS GROUP ---
@main.group()
def tools():
    """Utilidades para gestión de datasets."""
    pass


@tools.command("list")
@click.option("--domain", "-d", help="Filtrar por dominio")
@click.option("--all", "-a", is_flag=True, help="Listar todos")
def list_cmd(domain, all):
    """Lista los datasets generados y su tamaño."""
    from yupay.utils.files import list_datasets

    data = list_datasets()
    if not data:
        console.print("[yellow]No se encontraron datasets en ./data[/yellow]")
        return

    table = Table(title="Datasets Disponibles")
    table.add_column("Dominio", style="cyan")
    table.add_column("Run ID", style="green")
    table.add_column("Tamaño", justify="right")
    table.add_column("Fecha", style="dim")

    for dom, runs in data.items():
        if domain and dom != domain:
            continue

        for run in runs:
            # Simple timestamp formatting
            ts = datetime.fromtimestamp(
                run["timestamp"]).strftime("%Y-%m-%d %H:%M")
            table.add_row(dom, run["run_id"], run["size_formatted"], ts)

    console.print(table)


@tools.command("clear")
@click.option("--domain", "-d", help="Limpiar dominio específico")
@click.option("--all", "-a", is_flag=True, help="Limpiar TODO")
@click.option("--force", "-f", is_flag=True, help="Sin confirmación")
def clear_cmd(domain, all, force):
    """Elimina datasets generados."""
    from yupay.utils.files import list_datasets, delete_datasets

    if not (domain or all):
        console.print("[yellow]Especifique --domain [nombre] o --all[/yellow]")
        return

    data = list_datasets()
    targets = []

    for dom, runs in data.items():
        if all or (domain and dom == domain):
            for run in runs:
                targets.append(pathlib.Path(run["path"]))

    if not targets:
        console.print("[yellow]Nada que eliminar.[/yellow]")
        return

    console.print(
        f"[bold red]Se eliminarán {len(targets)} carpetas (Runs).[/bold red]")
    if not force:
        if not Confirm.ask("¿Está seguro?"):
            return

    count = delete_datasets(targets)
    console.print(f"[green]Se eliminaron {count} carpetas.[/green]")


if __name__ == "__main__":
    main()
