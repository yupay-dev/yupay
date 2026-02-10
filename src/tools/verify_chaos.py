
import polars as pl
import pathlib
from rich.console import Console
from rich.table import Table

console = Console()

def get_latest_run(base_path: pathlib.Path) -> pathlib.Path:
    runs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name.startswith("data_")])
    if not runs:
        raise FileNotFoundError(f"No data runs found in {base_path}")
    return runs[-1]

def verify_orders(path: pathlib.Path):
    if not path.exists():
        console.print(f"[yellow]⚠️ Orders file not found: {path}[/yellow]")
        return

    df = pl.read_parquet(path)
    total = len(df)
    console.print(f"\n[bold blue]Analyzing Orders ({total} rows)[/bold blue]")

    # Duplicates
    dupes = df.is_duplicated().sum()
    console.print(f"  - Duplicates: {dupes} ({dupes/total:.2%})")

    # Nulls
    if "store_id" in df.columns:
        nulls = df["store_id"].null_count()
        console.print(f"  - Null store_id: {nulls} ({nulls/total:.2%})")
    
    if "order_date" in df.columns:
        nulls = df["order_date"].null_count()
        console.print(f"  - Null order_date: {nulls} ({nulls/total:.2%})")

    # Outliers
    if "total_amount" in df.columns:
        negs = df.filter(pl.col("total_amount") < 0).height
        console.print(f"  - Negative Amounts: {negs} ({negs/total:.2%})")
        
        # High outliers (simple heuristic > 10000 or config based)
        # We assume mean is around 50-100, so > 5000 is likely outlier
        outliers = df.filter(pl.col("total_amount") > 5000).height
        console.print(f"  - High Outliers (>5000): {outliers} ({outliers/total:.2%})")

def verify_customers(path: pathlib.Path):
    if not path.exists():
        console.print(f"[yellow]⚠️ Customers file not found: {path}[/yellow]")
        return

    df = pl.read_parquet(path)
    total = len(df)
    console.print(f"\n[bold blue]Analyzing Customers ({total} rows)[/bold blue]")

    dupes = df.is_duplicated().sum()
    console.print(f"  - Duplicates: {dupes} ({dupes/total:.2%})")

    if "email" in df.columns:
        # Check for malformed emails (simple regex or just not containing @)
        # Contains '??' or spaces often indicates corruption in this context
        corrupt = df.filter(
            pl.col("email").str.contains(r"\?\?") | 
            ~pl.col("email").str.contains("@")
        ).height
        console.print(f"  - Header/Corrupt Emails: {corrupt} ({corrupt/total:.2%})")

def verify_products(path: pathlib.Path):
    if not path.exists():
        console.print(f"[yellow]⚠️ Products file not found: {path}[/yellow]")
        return
    
    df = pl.read_parquet(path)
    total = len(df)
    console.print(f"\n[bold blue]Analyzing Products ({total} rows)[/bold blue]")

    if "category" in df.columns:
        nulls = df["category"].null_count()
        console.print(f"  - Null Category: {nulls} ({nulls/total:.2%})")

def main():
    root = pathlib.Path("data/sales")
    if not root.exists():
        console.print("[red]No data/sales directory found.[/red]")
        return

    try:
        latest = get_latest_run(root)
        console.print(f"[bold green]Verifying run: {latest.name}[/bold green]")
        
        verify_orders(latest / "orders.parquet")
        verify_customers(latest / "customers.parquet")
        verify_products(latest / "products.parquet")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")

if __name__ == "__main__":
    main()
