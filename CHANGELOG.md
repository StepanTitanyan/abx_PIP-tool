# Changelog

All notable changes to **abx** will be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Releases will follow Semantic Versioning **after the first public release**.

> This project is **not released yet**. Everything lives under **[Unreleased]**.

---

## [Unreleased]

### Added
- `ab convert unit`: convert unit-level datasets into canonical schema (`user_id`, `variant`, `outcome`) with optional `--keep`.
- `ab convert events`: convert event-level logs into user-level metrics using repeatable `--metric`.
- Preview mode (`--preview`) that prints `head(30)` without writing output.
- Output writing to `.csv` and `.parquet` / `.pq` via `--out`.
- JSON config support:
  - `--save-config` to persist effective args into a JSON config file
  - `--config` to load config values (fills missing CLI args)
- Exposure anchoring for event-level conversion via `--exposure` + `--window`.
- Multi-row resolution flags:
  - `--dedupe error|first|last` (unit-level)
  - `--multiexposure error|first|last` (events)
  - `--multivariant error|first|last|mode|from_exposure` (events)
  - `--unassigned error|drop|keep` (events)
- `ab doctor`: validate converted user-level datasets (integrity, variants, missingness, metric sanity, distribution, allocation/SRM-style).

### Changed
- Docs and examples use the installed CLI name `ab` (package name remains `abx`).

### Fixed
- PowerShell usage docs: quote metric specs that contain parentheses.

## Release notes template (future)

When you publish versions, add entries like:

## [0.1.0] - YYYY-MM-DD
### Added
- ...
### Changed
- ...
### Fixed
- ...

---

## Versioning notes

- Until the first release, changes should go under **[Unreleased]**.
- When releasing, move items from **[Unreleased]** into a new version section and tag the release in Git.