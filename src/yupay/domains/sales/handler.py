from typing import Dict, Any, List
from datetime import datetime, timedelta
from rich.console import Console
from rich.status import Status

from yupay.core.registry import DomainRegistry, DomainHandler
from yupay.core.system import MemoryGuard
from yupay.core.time import TimeEngine
from yupay.core.erp import ERPDataset


@DomainRegistry.register("sales")
class SalesHandler:
    def execute(self, config: Dict[str, Any], sink: Any, status: Status, console: Console) -> List[tuple]:
        """
        Executes the Sales domain generation logic.
        """
        dataset = ERPDataset()
        full_config = config

        # 1. Config Context (Specific to Sales)
        # Note: We rely on Settings being passed or available, but here we receive 'config' which is full_config
        # We need to access Settings to load locale?
        # Actually in CLI, "settings" was used to load locale.
        # Ideally, config should already have everything or we re-instantiate Settings?
        # For now, let's assume the caller (CLI) handles the locale merge if possible,
        # OR we just handle it here if it's domain specific.
        # The original code loaded locale based on config["locale"].
        # We can do:
        from yupay.core.settings import Settings
        settings = Settings()  # Assuming standard path

        locale_data = settings.load_locale(full_config.get("locale", "es_PE"))
        if "entities" in full_config and "customers" in full_config["entities"]:
            full_config["entities"]["customers"]["names_catalog"] = locale_data.get(
                "names", {})

        if "inventory" not in full_config.get("domains", {}):
            full_config.setdefault("domains", {})["inventory"] = {
                "suppliers_base": full_config.get("domains", {}).get("sales", {}).get("customers_base", 1000) // 10
            }

        # 2. Decision: Batching or Monolithic?
        # We need stats? Original code used 'total_trans_est' from estimator.
        # But estimator runs BEFORE handler.
        # We can re-estimate or strict check config.
        # Better: Recalculate or just check daily * days.
        # Let's perform a lightweight calc since we have the config.
        start = datetime.strptime(full_config["start_date"], "%Y-%m-%d")
        end = datetime.strptime(full_config["end_date"], "%Y-%m-%d")
        days = (end - start).days + 1
        daily = full_config["daily_avg_transactions"]
        total_trans_est = days * daily

        use_batching = total_trans_est > 5_000_000
        results = []

        # 3. Paso 1: Dimensiones
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
                print(f"   -> Planificando tabla (Monolítico): {table_name}")
                est = total_trans_est if table_name != "stock_movements" else total_trans_est // 10
                out_path, real_count = sink.write(table_name, lf, est)
                results.append((table_name, real_count, out_path.name))
        else:
            # BATCHING
            print(
                f"   -> ESCALADO DETECTADO: Usando gestión dinámica de presupuesto de RAM.")

            current_start = full_config["start_date"]
            end_limit = full_config["end_date"]
            target_rows = 5_000_000
            batch_idx = 0
            stable_count = 0

            while datetime.strptime(current_start, "%Y-%m-%d") <= datetime.strptime(end_limit, "%Y-%m-%d"):
                # A. Evaluación Proactiva
                ram_status = MemoryGuard.get_status()
                current_ram = MemoryGuard.get_ram_usage_pct()
                budget_usage = MemoryGuard.get_budget_usage_pct()
                drift_gb = MemoryGuard.get_drift()

                status.update(
                    f"[bold green]Simulando transacciones... [blue]RAM Budget: {budget_usage:.1f}%[/blue] | [dim]SYS: {current_ram:.1f}%[/dim][/bold green]")

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
                    # Return partial? Or raise? Original returned.
                    return results

                elif ram_status == "BUDGET_ABORT":
                    console.print(
                        f"\n[bold red]❌ LÍMITE DE SEGURIDAD: Yupay alcanzó el {budget_usage:.1f}% de su presupuesto.[/bold red]")
                    console.print(
                        f"[red]RAM Global: {current_ram:.1f}% | Presupuesto Inicial: {MemoryGuard._baseline_available_gb:.2f} GB[/red]")
                    console.print(
                        "[white]Acción sugerida: Reduce 'daily_avg_transactions' o cierra aplicaciones pesadas.[/white]")
                    return results

                elif ram_status == "BUDGET_WARNING":
                    stable_count = 0
                    old_rows = target_rows
                    target_rows = max(500_000, target_rows // 2)
                    console.print(
                        f"[orange3]🟠 Throttle: Presupuesto al {budget_usage:.1f}%. Ajustando batch: {old_rows:,} -> {target_rows:,}[/orange3]")
                    MemoryGuard.wait_if_critical(threshold_pct=80.0)

                elif ram_status == "OBSERVATION":
                    stable_count = 0
                    console.print(
                        f"[yellow]🟡 Observación: Presupuesto al {budget_usage:.1f}%.[/yellow]")

                else:
                    if target_rows < 5_000_000:
                        stable_count += 1
                        if stable_count >= 3:
                            old_rows = target_rows
                            target_rows = min(
                                5_000_000, int(target_rows * 1.5))
                            console.print(
                                f"[green]🟢 Recuperación: {old_rows:,} -> {target_rows:,} filas tras 3 batches estables.[/green]")
                            stable_count = 0

                # B. Cálculo de ventana
                s_date, e_date = TimeEngine.get_next_batch_window(
                    current_start, end_limit, full_config["daily_avg_transactions"], target_rows
                )

                print(
                    f"   -> Batch {batch_idx + 1}: [{s_date} - {e_date}] @ {target_rows:,} rows")

                # C. Generación
                trans_batch = dataset.build_batch(full_config, s_date, e_date)

                for table_name, lf in trans_batch.items():
                    out_path, real_count = sink.write(
                        table_name, lf, target_rows, part_id=batch_idx)

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

                current_start = (datetime.strptime(
                    e_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                batch_idx += 1

        return results
