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
- Unit metric DSL: repeatable `--metric NAME=TYPE:fix(COL[, ...])` supporting `binary|continuous|count|string`.
- `ab convert unit --examples` and `ab convert events --examples` to print ready-to-copy DSL specs.
- Segments: repeatable `--segment` with `--segment-rule` and `--segment-fix`/`--segment-fix-opt` for both unit and events; events also supports `from_exposure`.
- Events DSL expansions: `event_count_ge`, `unique_event_days`, `median_value`, `last_value`, `last_time`, `time_to_nth_event`.
- Events per-metric value overrides: `value=COL` inside metric specs (overrides `--value` per metric).
- Doctor improvements: `--fail-on`, `--no-exit`, `--only`, `--ignore`, `--skip`, and per-metric arm size checks via `--min-n` and `--min-n-metric`.
- Markdown/JSON reports in doctor via `--report` and preview mode via `--preview`.

### Changed
- Docs and examples use the installed CLI name `ab` (package name remains `abx`).

### Fixed
- Fixed CLI edge cases and parser robustness across convert/doctor (duplicates, missing required columns, config loading, and DSL parsing).
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