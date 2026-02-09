# Changelog

## [1.3.0] - 2026-02-09
### Added
- **Composite Time Engine**: Demand now driven by `Trend * Seasonality * Weekly * Shock`.
- **Event Calendar**: Built-in Peruvian holidays (Fiestas Patrias, Xmas) and dynamic events (Mother's Day).
- **Weekly Patterns**: Configurable day-of-week weights (e.g., higher traffic on weekends).
- **Linear Trend**: Support for annual growth simulation.

## [1.2.0] - 2026-02-09
### Added
- **Store Dimension**: Added `dim_stores` representing physical locations (City, Format, Size).
- **Geographic Preference**: Customers are now assigned a `preferred_store_id` matching their city.
- **POS Logic**: Orders now track `store_id`, `pos_id`, and `cashier_id` simulating real retail operations.
- **Store Generator**: New generator class handling weighting by city (e.g., Lima has 50% of stores).

## [1.1.0] - 2026-02-03
### Added
- **Deep Realism**: Continuous Cosine Seasonality, Payday/Weekend effects, and Daily Jitter.
- **Customer Behavior**: Pareto 80/20 distribution for order assignment.
- **Product Bias**: Weekend probability boost for seasonal products.
- **Dynamic Seeding**: Automatic time-based seed generation for unique runs.

### Changed
- **CLI Output**: Added folder size report and removed debug text.
- **Sorting**: Guaranteed chronological order for `orders` and `payments` tables.

## [1.0.0] - 2026-02-02
### Changed
- **First Public Release**: Project is out of beta.
- **I18N**: Added internationalization support (English/Spanish).
- **Architecture**: Decoupled Registry and CLI.
- **Dependencies**: Aligned `duckdb` and `click` with production environment.
- **Documentation**: Added architecture guide and user manual.


All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-01 (Beta Release)

### Added
- **Core Engine**: Temporal simulation with Poisson distribution for realistic daily transaction volumes.
- **MemoryGuard**: Tiered RAM protection (Log -> Throttle -> Abort) to prevent system freezes.
- **Entropy Engine**: Deterministic chaos injection (Nulls, Orphans, String Noise) configurable via `global_seed`.
- **Architecture**: Modular `DomainRegistry` allowing easy extension of ERP domains.
- **CLI**: `yupay generate` command with rich terminal UI and real-time resource monitoring.
- **Batched Processing**: Automatic switching to batch mode for datasets > 5M rows to manage RAM.

### Changed
- Refactored project structure to `src-layout` for better packaging.
- Decoupled `sales` domain logic into `SalesHandler`.

### Fixed
- Hardcoded entropy seeds removed in favor of `chaos.global_seed`.
- Fixed circular imports in `sales` domain.
