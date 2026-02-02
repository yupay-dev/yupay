# Architecture Overview

Yupay is designed to be a **High-Performance**, **Memory-Safe** synthetic data factory.

## Core Components

### 1. Temporal Engine (`core/temporal.py`)
Generates time-series backbones. It does not just loop dates; it calculates distributions (Poisson) to determine how many events happen on a given day, simulating seasonality and trends.

### 2. MemoryGuard (`core/system.py`)
A proactive monitor that runs alongside the generation loop.
- **Budgeting**: Assigns a "RAM Budget" to the process based on system availability.
- **State Machine**: `NORMAL` -> `OBSERVATION` -> `THROTTLE` -> `ABORT`.
- **JIT Adjustment**: Resizes batches dynamically. If 5M rows is too heavy, it drops to 2.5M, then 1.25M, etc.

### 3. Entropy Engine (`core/entropy.py`)
Injects dirty data deterministically.
- **Injectors**: Classes like `NullInjector` or `OrphanInjector`.
- **Seeding**: A master `global_seed` is hashed and salted for every column and operation. This guarantees that `run 1` and `run 2` produce bit-exact same output if the config hasn't changed.

### 4. Domain Registry (`core/registry.py`)
Decouples the CLI from specific business logic.
- The CLI provides the *context* (Config, Sink, Console).
- The Domain Handler provides the *logic* (Dataset construction).

## Data Flow

1. **CLI** reads `main.yaml` and initializes `Settings`.
2. **SizeEstimator** predicts output size and blocks run if Disk/RAM is insufficient.
3. **DomainHandler** is invoked.
4. **ERPDataset** builds lazy Polars plans (Dimensions first, then Transactions).
5. **Sink** executes the plans (streaming or batched) and writes Parquet files.
6. **MemoryGuard** watches every step.
