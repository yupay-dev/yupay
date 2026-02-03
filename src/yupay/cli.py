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
from yupay.core.time import TimeEngine
from yupay.domains.sales.orders import SalesDataset
from yupay.core.registry import DomainRegistry
from yupay.core.i18n import _

console = Console()


@click.group()
def main():
    """Yupay: Realistic Synthetic ERP Data Generator"""
    # Global I18N Setup (Best Effort)
    try:
        from yupay.core.settings import Settings
        from yupay.core.i18n import setup_i18n

        settings = Settings()
        # Lightweight load just for locale
        defaults = settings.load_defaults()
        user_config = settings.load_user_config()
        # We don't have domain config here, but typically locale is in defaults/main
        final = settings.merge_configs(defaults, user_config)

        loc_setting = final.get("locale", "es_PE")
        lang = loc_setting[:2] if isinstance(loc_setting, str) else "es"
        setup_i18n(lang)

    except Exception:
        # Fallback to system default if config fails
        from yupay.core.i18n import setup_i18n
        setup_i18n()


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
        console.print(
            _("[bold red]Configuration Error:[/bold red] {e}").format(e=e))
        return

    # 1.1 Dynamic Randomness (Global Entropy)
    # If seed is not fixed in config (e.g. 42), we generate a new one.
    # Note: 'defaults' usually has seed=42. We check if user intentionally set it.
    # If user_config doesn't have "seed", we override the default 42 with Time-based.

    if "seed" not in user_config:
        # Dynamic Seed
        import time
        dynamic_seed = int(time.time())
        full_config["seed"] = dynamic_seed
        console.print(
            _("[magenta]🎲 Random Seed Generated:[/magenta] [bold]{seed}[/bold]").format(seed=dynamic_seed))
    else:
        # User defined seed
        console.print(
            _("[magenta]🎲 User Seed Constraints:[/magenta] [bold]{seed}[/bold]").format(seed=full_config["seed"]))

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
        _("[yellow]Analyzing resources and estimating volume...[/yellow]"))
    estimator = SizeEstimator(full_config, defaults)
    try:
        stats = estimator.validate_and_estimate(domain=domain)
        console.print(
            _("[green]✔ Estimation Approved:[/green] {days} days | ~{rows:,} rows | ~{size:.2f} GB").format(
                days=stats['days'],
                rows=stats['total_rows_estimated'],
                size=stats['estimated_size_gb']
            )
        )
    except (ValueError, OSError) as e:
        console.print(
            _("[bold red]⛔ BLOCKED BY SAFETY GUARD:[/bold red] {e}").format(e=e))
        return

    # 4. Preparación de Salida
    out_mgr = OutputManager(root_path=full_config["output_path"])
    run_dir = out_mgr.create_run_dir(domain)

    # Inicializar Presupuesto de RAM
    MemoryGuard.initialize_budget()
    budget_gb = MemoryGuard._baseline_available_gb
    console.print(
        _("[dim]Initial RAM Load: {sys:.1f}% | Budget assigned: {budget:.2f} GB[/dim]").format(
            sys=MemoryGuard._baseline_used_pct,
            budget=budget_gb
        )
    )

    try:
        final_format = full_config.get("output_format", "csv")
        sink = SinkFactory.get_sink(
            final_format, run_dir, validate_disk_space=True)
    except ValueError as e:
        console.print(_("[red]Format Error:[/red] {e}").format(e=e))
        return

    console.print(
        _("[bold green]Starting Temporal Engine for:[/bold green] [bold]{domain}[/bold]").format(domain=domain))
    console.print(_("📂 Output: [blue]{path}[/blue]").format(path=run_dir))

    # 5. Orquestación y Escritura
    with console.status(_("[bold green]Simulating transactions...[/bold green]"), spinner="dots") as status:

        # REGISTER HANDLERS (Ideally this happens via import magic or a loader)
        # For now we explicitly import known domains to trigger valid registration
        try:
            from yupay.domains.sales.handler import SalesHandler
        except ImportError:
            pass

        handler_cls = DomainRegistry.get_handler(domain)

        if not handler_cls:
            console.print(
                _("[red]Domain '{domain}' not registered or not implemented.[/red]").format(domain=domain))
            return

        # Instantiate and Execute
        handler = handler_cls()
        results = handler.execute(full_config, sink, status, console)

    # 6. Resumen (Restored)
    table = Table(title=_("Generation Summary"))
    table.add_column(_("Table"), style="cyan")
    table.add_column(_("Rows"), justify="right")
    table.add_column(_("File"), style="green")

    for t_name, count, fname in results:
        table.add_row(t_name, f"{count:,}", fname)

    console.print(table)
    peak_gb = MemoryGuard._peak_rss_gb
    peak_sys = MemoryGuard._peak_system_pct

    # Calculate Final Size
    from yupay.utils.files import get_dir_size, format_size
    final_size_bytes = get_dir_size(run_dir)
    final_size_str = format_size(final_size_bytes)

    console.print(Panel(
        _("✅ Process completed successfully.\nPath: {path}\nSize: {size}\n[dim]Peak RAM: Yupay {peak_gb:.2f} GB | System {peak_sys:.1f}%[/dim]").format(
            path=run_dir, size=final_size_str, peak_gb=peak_gb, peak_sys=peak_sys
        ),
        style="bold green"
    ))


# --- TOOLS GROUP ---
@main.group()
def tools():
    """Utilities to manage datasets."""
    pass


@tools.command("list")
@click.option("--domain", "-d", help="Filtrar por dominio")
@click.option("--all", "-a", is_flag=True, help="Listar todos")
def list_cmd(domain, all):
    """Lists generated datasets and their size."""
    from yupay.utils.files import list_datasets

    data = list_datasets()
    if not data:
        console.print(_("[yellow]No datasets found in ./data[/yellow]"))
        return

    table = Table(title=_("Available Datasets"))
    table.add_column(_("Domain"), style="cyan")
    table.add_column(_("Run ID"), style="green")
    table.add_column(_("Size"), justify="right")
    table.add_column(_("Date"), style="dim")

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
    """Delete generated datasets."""
    from yupay.utils.files import list_datasets, delete_datasets

    if not (domain or all):
        console.print(_("[yellow]Specify --domain [name] or --all[/yellow]"))
        return

    data = list_datasets()
    targets = []

    for dom, runs in data.items():
        if all or (domain and dom == domain):
            for run in runs:
                targets.append(pathlib.Path(run["path"]))

    if not targets:
        console.print(_("[yellow]Nothing to delete.[/yellow]"))
        return

    console.print(
        _("[bold red]Deleting {count} folders (Runs).[/bold red]").format(count=len(targets)))
    if not force:
        if not Confirm.ask(_("Are you sure?")):
            return

    count = delete_datasets(targets)
    console.print(
        _("[green]Deleted {count} folders.[/green]").format(count=count))


if __name__ == "__main__":
    main()
