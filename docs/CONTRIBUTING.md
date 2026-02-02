# Contributing to Yupay

Thank you for your interest in improving Yupay!

## 1. Development Setup

We recommend using a virtual environment (Python 3.10+):

```bash
python -m venv venv
# Windows
.\venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e .[dev]
```

## 2. Running Tests
We use `pytest`.

```bash
pytest tests/
```

## 3. Project Structure (Src-Layout)

```
src/
└── yupay/
    ├── cli.py              # Entry point (Click)
    ├── core/               # Shared logic
    │   ├── entropy.py      # Chaos engine
    │   ├── system.py       # MemoryGuard & DiskGuard
    │   ├── registry.py     # Domain plugin system
    │   └── ...
    └── domains/
        └── sales/
            ├── handler.py  # Sales domain execution logic
            └── ...
```

## 4. Adding a New Domain
Yupay is modular. To add a `logistics` domain:

1. Create `src/yupay/domains/logistics/handler.py`.
2. Implement the `DomainHandler` protocol (execute method).
3. Decorate your class:
   ```python
   from yupay.core.registry import DomainRegistry

   @DomainRegistry.register("logistics")
   class LogisticsHandler:
       ...
   ```
4. Update `cli.py` to import your handler (or implement auto-discovery).

## 5. Code Style
- Use **Type Hints** everywhere.
- Use **Polars** for data manipulation (No Pandas/Numpy unless necessary).
- Keep `main.yaml` as the single source of truth for configuration.
