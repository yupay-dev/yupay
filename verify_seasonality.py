import polars as pl
from datetime import date

def verify(parquet_path):
    print(f"Verifying: {parquet_path}")
    df = pl.read_parquet(parquet_path)
    
    # 1. Daily Aggregation
    daily = df.group_by("order_date").agg(pl.count().alias("count")).sort("order_date")
    
    avg_vol = daily["count"].mean()
    print(f"Average Daily Volume: {avg_vol:.2f}")

    # 2. Specific Dates Check
    dates_to_check = {
        "New Year (Low)": date(2024, 1, 1),
        "Mother's Day (High)": date(2024, 5, 12), # 2nd Sunday
        "Fiestas Patrias 1 (High)": date(2024, 7, 28),
        "Fiestas Patrias 2 (High)": date(2024, 7, 29),
        "Christmas Eve (Peak)": date(2024, 12, 24),
        "Christmas Day (Low)": date(2024, 12, 25)
    }
    
    print("\n--- Event Verification ---")
    for name, d in dates_to_check.items():
        row = daily.filter(pl.col("order_date") == d)
        if not row.is_empty():
            cnt = row["count"][0]
            factor = cnt / avg_vol
            print(f"{name} [{d}]: {cnt} (x{factor:.2f} vs Avg)")
        else:
            print(f"{name} [{d}]: NO DATA")

    # 3. Weekly Pattern
    print("\n--- Weekly Pattern Verification ---")
    df_wd = df.with_columns(pl.col("order_date").dt.weekday().alias("wd"))
    weekly = df_wd.group_by("wd").agg(pl.count().alias("count")).sort("wd")
    print(weekly)
    
    # Check Mon(0) vs Sat(5)
    mon_vol = weekly.filter(pl.col("wd") == 1)["count"][0]
    sat_vol = weekly.filter(pl.col("wd") == 6)["count"][0] # Polars 1-7 iso
    print(f"Monday Volume: {mon_vol}")
    print(f"Saturday Volume: {sat_vol}")
    ratio = sat_vol / mon_vol
    print(f"Sat/Mon Ratio: {ratio:.2f} (Expected > 1.3)")

    # 4. Trend Check
    print("\n--- Trend Verification ---")
    jan_avg = daily.filter((pl.col("order_date").dt.month() == 1))["count"].mean()
    nov_avg = daily.filter((pl.col("order_date").dt.month() == 11))["count"].mean()
    print(f"Jan Avg: {jan_avg:.2f}")
    print(f"Nov Avg: {nov_avg:.2f}")
    growth = (nov_avg - jan_avg) / jan_avg * 100
    print(f"Growth Jan->Nov: {growth:.2f}%")

if __name__ == "__main__":
    # Update this path to the latest generated folder
    path = r"d:\002. MANUEL VASQUEZ\Yupay\data\sales\data_20260209_181042\orders.parquet"
    verify(path)
