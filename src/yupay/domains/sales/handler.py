from typing import Dict, Any, List
from datetime import datetime, timedelta
from rich.console import Console
from rich.status import Status

from yupay.core.registry import DomainRegistry, DomainHandler
from yupay.core.system import MemoryGuard
from yupay.core.time import TimeEngine
from yupay.core.erp import ERPDataset
from yupay.domains.sales.stores import StoreGenerator  # New Import

@DomainRegistry.register("sales")
class SalesHandler:
    def execute(self, config: Dict[str, Any], sink: Any, status: Status, console: Console) -> List[tuple]:
        """
        Executes the Sales domain generation logic.
        """
        # Note: We import SalesDataset here to avoid circular imports if any, 
        # but standard is top-level. 
        from yupay.domains.sales.orders import SalesDataset
        
        dataset = SalesDataset() # Use specific dataset class
        full_config = config
        results = []

        # 1. Config Context
        from yupay.core.settings import Settings
        settings = Settings()

        locale_data = settings.load_locale(full_config.get("locale", "es_PE"))
        if "entities" in full_config and "customers" in full_config["entities"]:
            full_config["entities"]["customers"]["names_catalog"] = locale_data.get(
                "names", {})

        if "inventory" not in full_config.get("domains", {}):
            full_config.setdefault("domains", {})["inventory"] = {
                "suppliers_base": full_config.get("domains", {}).get("sales", {}).get("customers_base", 1000) // 10
            }

        # 2. Decision: Batching or Monolithic?
        start = datetime.strptime(full_config["start_date"], "%Y-%m-%d")
        end = datetime.strptime(full_config["end_date"], "%Y-%m-%d")
        days = (end - start).days + 1
        daily = full_config["daily_avg_transactions"]
        total_trans_est = days * daily

        use_batching = total_trans_est > 5_000_000
        
        # 3. GENERATE STORES (New Step)
        console.print(f"   -> Generando Dimension: stores (Tiendas)")
        store_gen = StoreGenerator(full_config)
        # We need Eager DF for passing to Customers/Orders
        # Default 50 stores or config
        n_stores = full_config.get("domains", {}).get("sales", {}).get("stores_count", 50)
        stores_lazy = store_gen.generate(n_stores)
        stores_df = stores_lazy.collect()
        
        # Write Stores
        out_path, real_count = sink.write("stores", stores_lazy, n_stores)
        results.append(("stores", real_count, out_path.name))

        # 4. Paso 1: Dimensiones (Customers, Products) & Paso 2: Transacciones
        # The SalesDataset.build method returns everything including dimensions
        
        # We pass stores_df to build
        # Note: 'dataset.build' returns a Dict[str, LazyFrame]
        
        if not use_batching:
            # Monolítico
            # We call build() which returns ALL tables (dims + facts)
            # Actually SalesDataset.build returns {customers, products, orders, payments}
            # We need to distinguish dims from batchable facts?
            # In original code, ERPDataset.build_dimensions was used. 
            # Now we use SalesDataset specific build.
            
            tables_map = dataset.build(full_config, stores_df=stores_df)
            
            for table_name, lf in tables_map.items():
                budget = MemoryGuard.get_budget_usage_pct()
                sys_ram = MemoryGuard.get_ram_usage_pct()
                status.update(
                    f"[bold green]Simulando transacciones Monolíticas... [blue]RAM Budget: {budget:.1f}%[/blue] | [dim]SYS: {sys_ram:.1f}%[/dim][/bold green]")
                print(f"   -> Planificando tabla (Monolítico): {table_name}")
                
                est = total_trans_est
                if table_name == "customers": est = 10000
                elif table_name == "products": est = 1000
                elif table_name == "stores": continue # Already written
                
                out_path, real_count = sink.write(table_name, lf, est)
                results.append((table_name, real_count, out_path.name))

        else:
            # BATCHING LOGIC
            # For massive data, we should write Dimensions first (Customers, Products)
            # Then loop for Orders/Payments.
            
            # A. Write Dimensions
            # We can re-use build() but just take dims?
            # Or assume Customers fits in memory?
            # Let's use build() for dimensions specifically if possible, 
            # OR just instantiate generators directly here.
            
            # To avoid code duplication, we assume 'products' and 'customers' are small enough 
            # to be generated once.
            
            # Generate ALL lazy
            full_lazy_map = dataset.build(full_config, stores_df=stores_df)
            
            # Extract Dimensions
            dims = ["customers", "products"]
            for d in dims:
                 if d in full_lazy_map:
                    print(f"   -> Escribiendo Dimensión: {d}")
                    out_path, real_count = sink.write(d, full_lazy_map[d], 10000)
                    results.append((d, real_count, out_path.name))
            
            # B. Loop for Transactions
            print(f"   -> ESCALADO DETECTADO: Usando gestión dinámica de presupuesto de RAM.")
            current_start = full_config["start_date"]
            end_limit = full_config["end_date"]
            target_rows = 5_000_000
            batch_idx = 0
            stable_count = 0

            while datetime.strptime(current_start, "%Y-%m-%d") <= datetime.strptime(end_limit, "%Y-%m-%d"):
                # ... Memory Guard Checks ...
                ram_status = MemoryGuard.get_status()
                current_ram = MemoryGuard.get_ram_usage_pct()
                budget_usage = MemoryGuard.get_budget_usage_pct()
                
                status.update(f"[bold green]Simulando transacciones... RAM: {budget_usage:.1f}%[/bold green]")
                
                if ram_status == "GLOBAL_HARD_STOP":
                    return results
                
                # ... Calculation ...
                s_date, e_date = TimeEngine.get_next_batch_window(
                    current_start, end_limit, full_config["daily_avg_transactions"], target_rows
                )

                print(f"   -> Batch {batch_idx + 1}: [{s_date} - {e_date}]")

                # Generate Batch
                # We need to call build() with override dates
                # AND we only want 'orders' and 'payments'
                batch_map = dataset.build(
                    full_config, 
                    start_date_override=s_date, 
                    end_date_override=e_date,
                    stores_df=stores_df
                )
                
                trans_tables = ["orders", "payments"]
                for t in trans_tables:
                    if t in batch_map:
                         out_path, real_count = sink.write(
                            t, batch_map[t], target_rows, part_id=batch_idx)
                         
                         # Update results logic
                         found = False
                         for idx, (name, count, fname) in enumerate(results):
                            if name == t:
                                results[idx] = (name, count + real_count, "Folder (Partitioned)")
                                found = True
                                break
                         if not found:
                            results.append((t, real_count, f"{t}/"))

                current_start = (datetime.strptime(
                    e_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                batch_idx += 1

        return results
