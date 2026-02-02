# Yupay: Realistic Synthetic ERP Generator

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-stable-brightgreen)

Yupay is a high-performance synthetic data generator designed to simulate realistic ERP scenarios. It generates dirty data, simulates temporal patterns (seasonality, trends), and manages resource usage efficiently.

## Documentation

- **[User Manual](docs/USER_MANUAL.md)**: Configuration guide and usage instructions.
- **[Architecture](docs/ARCHITECTURE.md)**: Deep dive into MemoryGuard, Entropy Engine, and Registry.
- **[Contributing](docs/CONTRIBUTING.md)**: Development setup and how to add new domains.
- **[Changelog](CHANGELOG.md)**: Version history.

## Key Features

- **Time-Driven Generation**: Simulates years of data with Poisson-distributed daily volumes.
- **Dirty Data Engine**: Injects nulls, duplicates, broken FKs, and string noise.
- **Memory Safety**: `MemoryGuard` prevents OOM crashes with tiered thresholds.
- **Proactive Batching**: Automatically switches from Monolithic to Batched mode for large datasets.

## Installation

```bash
# Clone the repository
git clone https://github.com/manuelvasquezab/yupay.git
cd yupay

# Install dependencies (Python 3.10+)
pip install -e .
```

## License

MIT
