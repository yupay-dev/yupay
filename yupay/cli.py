import click
import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm

from yupay.core.settings import Settings
from yupay.core.system import DiskGuard
from yupay.core.filesystem import OutputManager
from yupay.domains.sales.orders import SalesDataset

console = Console()


@click.group()
def main():
    """Yupay: Realistic Synthetic ERP Data Generator"""
    pass


@main.command()
@click.argument("domain")
@click.option("--rows", type=int, help="Override number of rows")
@click.option("--format", type=click.Choice(["csv", "parquet", "duckdb"]), help="Override output format")
@click.option("--output-root", help="Override output root directory")
@click.option("--force", is_flag=True, help="Skip disk space check")
def generate(domain, rows, format, output_root, force):
    """Generate synthetic data using YAML configuration."""

    # 1. Cargar Configuración
    settings = Settings()
    try:
        config = settings.load_domain(domain)
        defaults = settings.load_defaults()
        full_config = settings.merge_configs(defaults, config)
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        return

    # 2. Solver Parámetros
    gen_config = full_config.get("generation", {})
    final_rows = rows if rows is not None else gen_config.get("rows", 10000)
    final_format = format if format is not None else gen_config.get(
        "format", "parquet")
    final_output_root = output_root if output_root is not None else gen_config.get(
        "output_root", "./data")

    # 3. Preparación de Salida
    out_mgr = OutputManager(root_path=final_output_root)
    run_dir = out_mgr.create_run_dir(domain)

    # Instanciar Sink
    from yupay.sinks.definitions import SinkFactory
    try:
        sink = SinkFactory.get_sink(
            final_format, run_dir, validate_disk_space=not force)
    except ValueError as e:
        console.print(f"[red]Error de formato:[/red] {e}")
        return

    console.print(
        f"[green]Iniciando generación (OPTIMIZED-LAZY) para:[/green] [bold]{domain}[/bold]")
    console.print(
        f"📄 Configuración: {final_rows} filas | Formato: {final_format}")
    console.print(f"📂 Salida: [blue]{run_dir}[/blue]")

    # 4. Orquestación y Escritura
    with console.status(f"[bold green]Procesando Pipeline Lazy...[/bold green]", spinner="dots"):
        if domain == "sales":
            dataset = SalesDataset()

            # Carga de localizacón y catálogos
            locale_data = settings.load_locale(
                full_config.get("locale", "es_PE"))
            if "entities" in full_config and "customers" in full_config["entities"]:
                full_config["entities"]["customers"]["names_catalog"] = locale_data.get(
                    "names", {})
            if "entities" in full_config and "products" in full_config["entities"]:
                full_config["entities"]["products"]["catalog"] = full_config.get(
                    "catalogs", {}).get("products", {})

            rows_map = {
                "orders": final_rows,
                "customers": max(10, final_rows // 10),
                "products": max(5, final_rows // 100)
            }

            # Retorna LazyFrames
            lazy_data = dataset.build(full_config, rows_map)

            # 5. Materialización vía Sink
            # El Sink valida espacio y escribe eficientemente
            results = []
            for table_name, lf in lazy_data.items():
                estimated_table_rows = rows_map.get(
                    table_name, final_rows)  # Estimado simple
                try:
                    out_path = sink.write(table_name, lf, estimated_table_rows)
                    results.append(
                        (table_name, estimated_table_rows, out_path.name))
                except OSError as e:
                    console.print(f"[bold red]DiskGuard Error:[/bold red] {e}")
                    # Limpieza parcial si falla
                    out_mgr.clean(domain, run_dir.name)
                    return
        else:
            console.print(f"[red]Dominio {domain} no implementado[/red]")
            return

    # 6. Resumen
    table = Table(title="Resumen de Generación (Lazy Execution)")
    table.add_column("Tabla", style="cyan")
    table.add_column("Filas (Est)", justify="right")
    table.add_column("Archivo", style="green")

    for t_name, count, fname in results:
        table.add_row(t_name, str(count), fname)

    console.print(table)
    console.print(Panel(
        f"✅ Proceso completado exitosamente.\nRuta: {run_dir}",
        style="bold green"
    ))


@main.command("list")
@click.argument("domain", required=False)
@click.option("--root", default="./data", help="Root directory")
def list_datasets(domain, root):
    """List generated datasets."""
    out_mgr = OutputManager(root_path=root)
    results = out_mgr.list_runs(domain)

    if not results:
        console.print("[yellow]No se encontraron datasets.[/yellow]")
        return

    table = Table(title="Datasets Disponibles")
    table.add_column("Dominio", style="cyan")
    table.add_column("Run ID (Carpeta)", style="green")

    for dom, runs in results.items():
        for run in runs:
            table.add_row(dom, run.name)

    console.print(table)


@main.command()
@click.argument("domain")
@click.option("--id", "run_id", help="Specific Run ID to delete (e.g. data_2023...)")
@click.option("--root", default="./data", help="Root directory")
def clean(domain, run_id, root):
    """Clean datasets. Usage: clean sales, clean all, clean sales --id ..."""
    out_mgr = OutputManager(root_path=root)

    target_msg = f"dominio '{domain}'" if not run_id else f"run '{run_id}' en '{domain}'"
    if domain == "all":
        target_msg = "TODO el directorio de datos"

    if Confirm.ask(f"¿Estás seguro de eliminar {target_msg}?"):
        count = out_mgr.clean(domain, run_id)
        if count > 0:
            console.print(f"[bold green]Eliminado exitosamente.[/bold green]")
        else:
            console.print(
                f"[yellow]No se encontró nada para eliminar.[/yellow]")
    else:
        console.print("[dim]Operación cancelada.[/dim]")


if __name__ == "__main__":
    main()
