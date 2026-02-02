import pytest
from click.testing import CliRunner
from yupay.cli import main
from yupay.core.settings import Settings
import polars as pl


@pytest.fixture
def mock_settings(monkeypatch, tmp_path):
    """
    Mock Settings.load_user_config to return a minimal test configuration
    that outputs to the temporary test directory.
    """

    def mock_load_user_config(self):
        return {
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",  # Just 2 days
            "daily_avg_transactions": 100,  # Very small volume
            "output_path": str(tmp_path),  # Use temp dir
            "output_format": "parquet",
            "chaos_level": "low"
        }

    monkeypatch.setattr(Settings, "load_user_config", mock_load_user_config)


def test_cli_generate_sales_smoke(mock_settings, tmp_path):
    """
    Smoke test for 'yupay generate sales'.
    Verifies:
    1. Command runs without error (exit code 0).
    2. Output directory is created.
    3. Essential parquet files are generated.
    """
    runner = CliRunner()
    result = runner.invoke(main, ["generate", "sales"])

    # Debug info if fails
    assert result.exit_code == 0, f"Command failed with code {result.exit_code}.\nOutput: {result.output}\nException: {result.exception}"

    # Verify structure
    # yupay creates: output_path/sales/data_TIMESTAMP/...
    sales_dir = tmp_path / "sales"
    assert sales_dir.exists(), "Sales domain folder not created"

    # Find the run directory (data_YYYYMMDD_...)
    run_dirs = list(sales_dir.glob("data_*"))
    assert len(
        run_dirs) == 1, f"Expected 1 run directory, found {len(run_dirs)}"
    run_dir = run_dirs[0]

    # Verify key files
    # Dimensions
    assert (run_dir / "customers.parquet").exists()
    assert (run_dir / "products.parquet").exists()

    # Transactions (Batched or Monolithic, depending on volume)
    # With 100 daily * 2 days = 200 rows, it should be monolithic, so single files?
    # No, orders/payments are partitioned folders if batched, but monolithic write creates direct files?
    # Let's check Sink.write:
    # "out_path, real_count = sink.write(table_name, lf, est)"
    # If using parquet sink:
    # It writes to `file_path = self.path / f"{table_name}.parquet"` if not partitioned?
    # Actually, let's just check existence of "orders" either as file or dir.

    orders_path = run_dir / "orders.parquet"
    # Or strict check:
    # Based on cli.py:
    # if not use_batching: Monolithic write.
    # Sink (parquet) writes to name.parquet.

    assert orders_path.exists(), "Orders file not generated (Monolithic mode expected)"

    # Verify content (light validation)
    df = pl.read_parquet(orders_path)
    assert df.height > 0
    assert "order_id" in df.columns
