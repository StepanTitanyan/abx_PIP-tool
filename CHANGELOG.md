# Changelog

All notable changes to **abx** will be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and adheres to Semantic Versioning when releases begin.

---

## [Unreleased]

### Added
- `abx convert unit`: convert unit-level datasets into canonical schema (`user_id`, `variant`, `outcome`).
- `abx convert events`: convert event-level logs into user-level metrics.
- Preview mode (`--preview`) that prints `head(30)` without writing output.
- Output writing to `.csv` and `.parquet` / `.pq` via `--out`.
- JSON config support:
  - `--save-config` to persist effective args into a JSON config file
  - `--config` to load config values (fills missing CLI args)
- Exposure anchoring for event-level conversion via `--exposure`.
- Outcome windows after exposure via `--window` (parsed as a pandas duration).
- Multiple exposure handling via `--multiexposure` (`error`, `first`, `last`).
- Multiple variant handling via `--multivariant` (`error`, `first`, `last`, `mode`, `from_exposure`).
- Metric DSL (`--metric`, repeatable) supporting:
  - `binary:event_exists(EVENT)`
  - `count:count_event(EVENT)`
  - `continuous:sum_value(EVENT)` *(requires `--value`)*
  - `continuous:mean_value(EVENT)` *(requires `--value`)*
  - `continuous:max_value(EVENT)` *(requires `--value`)*
  - `time:first_time(EVENT)`
  - `time:time_to_event(EVENT, unit=s|m|h|d)` *(requires `--exposure`)*

### Changed
- N/A (initial development)

### Fixed
- N/A (initial development)

### Removed
- N/A (initial development)

---

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
