# Yupay User Manual

Welcome to the **Yupay** user manual. This guide focuses on how to configure and run the generator to produce the data you need.

## 1. Quick Start

Run the generation for the Sales domain:

```bash
yupay generate sales
```

This uses the default configuration in `config/defaults.yaml` (technical defaults) and `config/main.yaml` (user overrides).

## 2. Configuration (`config/main.yaml`)

The file `config/main.yaml` is your control center.

### 2.1 Simulation Window
Define the start/end dates. The engine handles leap years and daily volume distribution automatically.

```yaml
start_date: "2010-01-01"
end_date: "2024-12-31"
```

### 2.2 Volume Control
`daily_avg_transactions` controls the "heartbeat" of the simulation. A Poisson distribution is applied to this mean to create natural variance.

```yaml
daily_avg_transactions: 15000 
```

### 2.3 Chaos Levels (Dirty Data)
You can choose how "dirty" the data should be.

- `low`: Almost clean. Occasional nulls.
- `medium`: Realistic errors. Some duplicates, some nulls, rare broken keys.
- `high`: Hostile. Many duplicates, broken keys, time travel paradoxes (Order Date > Ship Date).

```yaml
chaos_level: "medium"
chaos:
  global_seed: 42  # Change this to get a different random outcome
```

## 3. Managing Output
Data is saved by default to `data/[domain]/data_[timestamp]`.

To see what you have generated:
```bash
yupay tools list
```

To clean up old runs:
```bash
yupay tools clear --domain sales
```

## 4. Resource Safety (MemoryGuard)
Yupay protects your PC.
- **Monitoring**: It watches your RAM usage in real-time.
- **Throttling**: If RAM usage > 80%, it slows down generation and reduces batch sizes.
- **Abort**: If RAM usage > 90%, it stops to prevent a system crash.
