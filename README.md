# abx — Canonical A/B Experiment Tables from Messy Data (CLI)

`abx` converts **raw experiment datasets** into a **canonical, user-level table** you can feed directly into A/B analysis (t‑tests, Mann–Whitney, CUPED, regression, Bayesian, etc.).

It’s built for the common real-world situation where your data is **messy**:

- user IDs are numeric sometimes, strings other times
- timestamps come in multiple formats / timezones
- event names vary by casing / whitespace
- purchase amounts include symbols, commas, or text
- users can appear multiple times, or have multiple exposures, or multiple variants

`abx` makes those cases explicit and controllable via CLI flags, with predictable outputs and clear errors.

> **CLI name:** the installed command is **`ab`** (package name is `abx`).

---

## What you get

### 1) `convert unit` — unit-level → canonical unit-level
Input: one row per user (already aggregated)  
Output: one row per user, renamed to the canonical schema:

- `user_id`
- `variant` *(lowercased & trimmed)*
- `outcome` *(as provided; numeric stays numeric)*
- optional kept columns via `--keep`

### 2) `convert events` — event log → canonical user-level metrics
Input: one row per event  
Output: one row per user with:

- `user_id`
- `variant` *(lowercased & trimmed)*
- optional exposure anchoring: `exposure_time`, `window_end`
- one output column per metric defined via `--metric` (repeatable)

### 3) `doctor` — validate a converted dataset before analysis
Input: already-converted user-level data (`convert unit` / `convert events`)  
Output: a readiness report (console + optional `.md` / `.json`) across integrity, missingness, metric sanity, and more.

---

## Install

```bash
pip install abx
```

If you're developing locally:

```bash
pip install -e .
```

---

## Quick start

### Unit-level

Preview (prints `head(30)`):

```bash
ab convert unit \
  --data data/unit.csv \
  --user user_id \
  --variant exp_group \
  --outcome outcome \
  --preview
```

Write output:

```bash
ab convert unit \
  --data data/unit.csv \
  --user user_id \
  --variant exp_group \
  --outcome outcome \
  --out out/unit_converted.csv
```

Keep extra columns:

```bash
ab convert unit \
  --data data/unit.csv \
  --user user_id \
  --variant exp_group \
  --outcome outcome \
  --keep country,device \
  --preview
```

Resolve duplicate users (multiple rows per user):

```bash
ab convert unit \
  --data data/unit.csv \
  --user user_id \
  --variant exp_group \
  --outcome outcome \
  --dedupe first \
  --out out/unit_converted.csv
```

---

### Event-level (metrics)

Compute basic metrics (repeat `--metric` to add more columns):

```bash
ab convert events \
  --data data/events.csv \
  --user user_id \
  --variant exp_group \
  --time timestamp \
  --event event_name \
  --metric "conversion=binary:event_exists(purchase)" \
  --metric "n_purchases=count:count_event(purchase)" \
  --preview
```

Value-based metrics (requires `--value`):

```bash
ab convert events \
  --data data/events.csv \
  --user user_id \
  --variant exp_group \
  --time timestamp \
  --event event_name \
  --value amount \
  --metric "revenue=continuous:sum_value(purchase)" \
  --metric "aov=continuous:mean_value(purchase)" \
  --out out/user_metrics.parquet
```

Exposure-anchored outcomes (scope metrics **after exposure** and **within a window**):

```bash
ab convert events \
  --data data/events.csv \
  --user user_id \
  --variant exp_group \
  --time timestamp \
  --event event_name \
  --exposure experiment_exposed \
  --window 7d \
  --multiexposure first \
  --multivariant from_exposure \
  --unassigned error \
  --metric "conversion=binary:event_exists(purchase)" \
  --metric "ttp_hours=time:time_to_event(purchase, unit=h)" \
  --out out/after_exposure.csv
```

> **PowerShell note:** always quote metrics that include parentheses:
> ```powershell
> --metric "conversion=binary:event_exists(purchase)"
> ```

---

## Doctor (readiness check)

Run doctor on a converted file:

```bash
ab doctor --data out/after_exposure.csv
```

Generate a Markdown report:

```bash
ab doctor --data out/after_exposure.csv --report reports/doctor.md
```

---

## Metric DSL

Metrics are specified with repeatable `--metric` arguments:

```
--metric NAME=TYPE:RULE(EVENT[, key=value ...])
```

Examples:

- `conversion=binary:event_exists(purchase)`
- `n_purchases=count:count_event(purchase)`
- `revenue=continuous:sum_value(purchase)` *(requires `--value`)*
- `aov=continuous:mean_value(purchase)` *(requires `--value`)*
- `max_purchase=continuous:max_value(purchase)` *(requires `--value`)*
- `first_purchase=time:first_time(purchase)`
- `ttp=time:time_to_event(purchase, unit=h)` *(requires `--exposure`)*

Full spec, edge cases, and rule list: **`docs/metrics.md`**

---

## Config workflow (reproducibility)

Save a working CLI run into a JSON config:

```bash
ab convert events \
  --data data/events.csv \
  --user user_id --variant exp_group --time timestamp --event event_name \
  --exposure experiment_exposed --window 7d \
  --metric "conversion=binary:event_exists(purchase)" \
  --save-config configs/after_exposure.json \
  --preview
```

Re-run later on another file:

```bash
ab convert events \
  --config configs/after_exposure.json \
  --data data/events_new.csv \
  --out out/after_exposure_new.csv
```

Details: **`docs/config.md`**

---

## Data contract (what abx guarantees)

**Inputs supported**
- CSV (`.csv`)
- Parquet (`.parquet`, `.pq`)

**Cleaning behavior**
- `user`, `variant`, `event`: coerced to string, trimmed; `variant` & `event` are lowercased
- time column: parsed with `pd.to_datetime(..., errors="coerce", utc=True, format="mixed")`; invalid timestamps are dropped
- `--value`: optional; parsed to numeric after removing non-numeric characters; invalid values become `NaN`

**Outputs**
- `user_id` always string
- `variant` always lowercased string
- exposure outputs (`exposure_time`, `window_end`) are UTC timestamps when enabled
- metric columns are deterministic based on your metric specs

Full contract: **`docs/data-contract.md`**

---

## Common failure modes (and how abx helps)

- **Missing columns** → fails fast with a list of missing + available columns
- **Multiple exposures per user** → `--multiexposure error|first|last`
- **Multiple variants per user** → `--multivariant error|first|last|mode|from_exposure`
- **Users with no variant after cleaning** → `--unassigned error|drop|keep`
- **Bad metric spec** → explicit parser errors showing which `--metric` is malformed
- **PowerShell interpreting parentheses** → quote `--metric "..."`

More: **`docs/troubleshooting.md`**

---

## Project docs

- `docs/convert.md` — conversion commands
- `docs/doctor.md` — dataset readiness checks
- `docs/metrics.md` — metric DSL reference
- `docs/config.md` — config save/load
- `docs/data-contract.md` — what inputs/outputs guarantee
- `docs/troubleshooting.md` — common issues

---

## Roadmap (later)

- `ab analyse` (built-in A/B analysis on converted tables)
- richer metric rules (funnels, unique counts, rolling windows)
- better ergonomics for event parameters and value columns

---

## License

MIT (see `LICENSE`).
