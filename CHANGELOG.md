# Changelog

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
