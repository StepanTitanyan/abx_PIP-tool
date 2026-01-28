# Conversion (`abx convert`)

This document is the **single source of truth** for the `abx convert` command family:
- deep conversion logic,
- every supported CLI attribute/flag,
- the metric DSL (formerly `metrics.md`),
- edge cases and guarantees,
- and practical guidance for correct A/B-ready outputs.

> Canonical user-level table = **one row per user** with a single `variant` assignment and one or more outcome/metric columns.

---

## Table of contents

- [Overview](#overview)
- [`abx convert unit`](#abx-convert-unit)
  - [Arguments](#arguments-unit)
  - [Conversion steps](#conversion-steps-unit)
  - [Duplicates](#duplicates-unit)
  - [Examples](#examples-unit)
- [`abx convert events`](#abx-convert-events)
  - [Arguments](#arguments-events)
  - [Core concepts](#core-concepts)
  - [Conversion steps](#conversion-steps-events)
  - [Variant consistency](#variant-consistency)
  - [Exposure anchoring & windows](#exposure-anchoring--windows)
  - [Metric DSL](#metric-dsl)
    - [Syntax](#syntax)
    - [Supported rules](#supported-rules)
    - [Examples](#examples-metrics)
    - [Edge cases](#edge-cases-metrics)
- [Global behaviors](#global-behaviors)
  - [Preview vs output](#preview-vs-output)
  - [Input/output formats](#inputoutput-formats)
  - [Config workflow](#config-workflow)
- [Edge cases and guarantees](#edge-cases-and-guarantees)
- [Practical guidance](#practical-guidance)
- [See also](#see-also)

---

## Overview

`abx` supports two input shapes:

1. **Unit-level** (`abx convert unit`)
   - Input: already one row per user (or *intended* to be)
   - Output: selects + renames columns into a canonical schema

2. **Event-level** (`abx convert events`)
   - Input: one row per event
   - Output: aggregates events into user-level metrics using a repeatable metric DSL (`--metric`)
   - Optional: exposure anchoring (`--exposure`) and post-exposure windows (`--window`)

---

## `abx convert unit`

### Purpose

Use `convert unit` when each row represents a user (unit) and already includes:
- a user identifier,
- a variant/treatment label,
- an outcome value,
- and optionally extra columns you want to keep.

### Canonical output schema

Output always contains:
- `user_id` — string (trimmed)
- `variant` — string (trimmed, lowercased)
- `outcome` — copied from input column (see data contract)

Optionally contains:
- extra columns listed in `--keep` (preserved as-is)

### Arguments (unit)

Required:
- `--data PATH` — input `.csv` or `.parquet`
- `--user COL` — user/unit id column name
- `--variant COL` — treatment/variant column name
- `--outcome COL` — outcome column name

Optional:
- `--keep COL,COL` — comma-separated extra columns to keep
- `--dedupe {error,first,last}` — what to do if multiple rows per user exist (default: `error`)
- `--preview` — print `head(30)` and exit
- `--out PATH` — output `.csv` or `.parquet`
- `--save-config PATH` — save effective args to JSON
- `--config PATH` — load args from JSON (fills missing CLI args)

### Conversion steps (unit)

1. **Load input** (`--data`)
2. **Validate required args** (`--data`, `--user`, `--variant`, `--outcome`)
3. **Validate required columns exist**
   - required columns = `user`, `variant`, `outcome` + any `--keep` columns
4. **Normalize identifiers**
   - user id: string + `.str.strip()`
   - variant: string + `.str.strip().str.lower()`
5. **Select & rename**
   - `user` → `user_id`
   - `variant` → `variant`
   - `outcome` → `outcome`
6. **Handle duplicate users** (`--dedupe`)
7. **Preview or write**

### Duplicates (unit)

A “duplicate user” means the output table contains multiple rows with the same `user_id`.

`--dedupe`:
- `error` (default): stop and report number of duplicate users and rows
- `first`: keep first row per `user_id`
- `last`: keep last row per `user_id`

> Tip: If duplicates are common, your dataset may not actually be unit-level (e.g., multiple sessions/purchases per user). Consider `convert events` instead.

### Examples (unit)

Preview:

```bash
abx convert unit   --data data/unit.csv   --user user_id   --variant group   --outcome conversion   --preview
```

Keep columns + dedupe:

```bash
abx convert unit   --data data/unit.csv   --user uid   --variant treatment   --outcome revenue   --keep country,device   --dedupe last   --out out/unit.parquet
```

---

## `abx convert events`

### Purpose

Use `convert events` when your dataset is an **event log** (one row per event) with at least:
- user id,
- variant label,
- event timestamp,
- event name/type,

and optionally:
- a numeric value column (purchase amount, duration, etc.).

The converter outputs **one row per user** plus **computed metrics**.

### Canonical output schema

Output always contains:
- `user_id` — string (trimmed)
- `variant` — string (trimmed, lowercased)

If `--exposure` is used:
- `exposure_time` — UTC datetime
- `window_end` — UTC datetime (only if `--window` is provided)

Plus:
- one column per metric specified with `--metric`.

### Arguments (events)

Required:
- `--data PATH` — input `.csv` or `.parquet`
- `--user COL` — user/unit id column name
- `--variant COL` — treatment/variant column name
- `--time COL` — event timestamp column name
- `--event COL` — event type/category column name
- at least one `--metric SPEC`

Optional:
- `--value COL` — event numeric value column (e.g., purchase amount)
- `--exposure VALUE` — value in the event column identifying exposure (users without exposure are dropped)
- `--window DURATION` — outcome window after exposure (e.g., `7d`, `24h`, `30m`) **(used only with exposure)**
- `--multiexposure {error,first,last}` — how to handle multiple exposures per user (default: `first`)
- `--multivariant {error,first,last,mode,from_exposure}` — how to handle multiple variants per user (default: `error`)
- `--preview` — print `head(30)` and exit
- `--out PATH` — output `.csv` or `.parquet`
- `--save-config PATH` — save effective args to JSON
- `--config PATH` — load args from JSON (fills missing CLI args)

---

## Core concepts

### 1) Output base table (`users_tbl`) vs scoped events (`df_scoped`)

Internally, `convert events` constructs:
- `users_tbl`: one row per user (the output base)
- `df_scoped`: the event rows eligible to contribute to metric calculations

If exposure/window is enabled, `df_scoped` is filtered:
- only exposed users,
- only events after exposure,
- only events within `window_end`.

### 2) Exposure anchoring & windows

If you specify `--exposure SOME_VALUE`, then:
- the event column (`--event`) is compared to that value (case-insensitive),
- users without exposure are dropped,
- each user gets one `exposure_time` (first/last/error).

If you also specify `--window DURATION`, then:
- a `window_end` column is created as `exposure_time + window`,
- only events at or before `window_end` are eligible for metrics.

### 3) Variant consistency per user

A/B analysis assumes each user belongs to exactly one variant.
If the input contains multiple variants per user, behavior depends on `--multivariant`.

---

## Conversion steps (events)

1. **Load input** (`--data`)
2. **Validate required args**
3. **Validate required columns exist**
4. **Normalize string columns**
   - user id: string + trimmed
   - variant: string + trimmed + lowercased
   - event: string + trimmed + lowercased
   - drop rows where user_id is missing/empty after cleaning
5. **Parse numeric values** (if `--value` is provided)
   - remove non-numeric characters
   - parse float; invalid → `NaN`
6. **Parse timestamps**
   - parse `--time` into UTC datetimes
   - invalid timestamps are dropped
   - sort by `(user, time)`
7. **Resolve multi-variant users** (if needed)
8. **Resolve exposure events** (if `--exposure`)
9. **Build output base table (`users_tbl`)**
10. **Build scoped events table (`df_scoped`)**
11. **Compute metrics** (repeatable `--metric`)
12. **Preview or write**

---

## Variant consistency

If any user has more than one variant label in the input, `abx` detects it.

`--multivariant` strategies:
- `error` (default): stop and show example users
- `first`: choose the first observed variant by time
- `last`: choose the last observed variant by time
- `mode`: choose the most frequent variant label per user
- `from_exposure`: choose variant value on the exposure event (**requires `--exposure`**)

> Recommendation: keep `error` unless you have a strong reason and a clear interpretation.

---

## Exposure anchoring & windows

### Multiple exposures per user (`--multiexposure`)

If a user has multiple exposure events, behavior depends on `--multiexposure`:

- `error`: stop and show example users
- `first` (default): pick earliest exposure per user
- `last`: pick latest exposure per user

### Users without exposure

If `--exposure` is set, users without any exposure event are excluded.
If **no exposure events exist at all**, the command stops with an error.

### Window parsing

`--window` is parsed using pandas `to_timedelta`.

Examples:
- `7d` (7 days)
- `24h` (24 hours)
- `30m` (30 minutes)

---

## Metric DSL

Metrics are computed on the (possibly scoped) events table and merged into the one-row-per-user output.

### Syntax

Each metric is specified with a repeatable `--metric` argument:

```
--metric NAME=TYPE:RULE(EVENT[, key=value ...])
```

Where:
- `NAME` is the output column name (must be unique across metrics)
- `TYPE` is a broad output type (`binary`, `count`, `continuous`, `time`)
- `RULE` defines the computation within that type
- `EVENT` is the event name in your `--event` column (case-insensitive after cleaning)
- `key=value` are optional rule arguments (used by some rules)

**General notes**
- You can repeat `--metric` many times to create many output columns.
- Metric names must be unique; duplicates stop the run with an error.
- `EVENT` matching uses the cleaned lowercased event column.

### Supported rules

#### 1) Binary

**`binary:event_exists(EVENT)`**  
Returns `1` if the event occurs at least once for the user in `df_scoped`, else `0`.

- Output dtype: integer (`0/1`)
- Missing users are filled with 0

#### 2) Count

**`count:count_event(EVENT)`**  
Counts how many times `EVENT` occurs for the user in `df_scoped`.

- Output dtype: integer
- Missing users are filled with 0

#### 3) Continuous (requires `--value`)

All continuous rules require `--value COL` so a numeric column exists.

**`continuous:sum_value(EVENT)`**  
Sum of `--value` for matching event rows.

- Output dtype: float
- Missing users are filled with `0.0`
- Values that fail numeric parsing may become `NaN` and affect sums

**`continuous:mean_value(EVENT)`**  
Mean of `--value` for matching event rows.

- Output dtype: float
- Missing users are filled with `0.0`

**`continuous:max_value(EVENT)`**  
Max of `--value` for matching event rows.

- Output dtype: float
- Missing users are filled with `0.0`

#### 4) Time

**`time:first_time(EVENT)`**  
Returns the earliest timestamp at which `EVENT` occurs for a user (UTC datetime).
If the event never occurs, the value is missing (`NaT`).

**`time:time_to_event(EVENT, unit=s|m|h|d)`** *(requires `--exposure`)*  
Returns time from `exposure_time` to the first occurrence of `EVENT` within `df_scoped`.

- If the event never occurs, result is missing (`NaN`)
- `unit` defaults to `s` if omitted
- Supported units:
  - `s` seconds
  - `m` minutes
  - `h` hours
  - `d` days

### Examples (metrics)

Basic conversion + binary and counts:

```bash
abx convert events   --data data/events.csv   --user user_id   --variant variant   --time ts   --event event   --metric conversion=binary:event_exists(purchase)   --metric n_purchases=count:count_event(purchase)   --preview
```

Value-based metrics:

```bash
abx convert events   --data data/events.csv   --user user_id   --variant variant   --time ts   --event event   --value amount   --metric revenue=continuous:sum_value(purchase)   --metric aov=continuous:mean_value(purchase)   --metric max_order=continuous:max_value(purchase)   --out out/metrics.parquet
```

Exposure-anchored + window + time-to-event:

```bash
abx convert events   --data data/events.csv   --user user_id   --variant variant   --time ts   --event event   --exposure exposure   --window 7d   --metric conversion=binary:event_exists(purchase)   --metric ttp_hours=time:time_to_event(purchase, unit=h)   --out out/post_exposure.csv
```

### Edge cases (metrics)

- **Users missing the metric event**
  - `binary:event_exists` → `0`
  - `count:count_event` → `0`
  - `continuous:*` → `0.0`
  - `time:first_time` → missing (`NaT`)
  - `time:time_to_event` → missing (`NaN`)

- **Bad numeric values in `--value`**
  - are coerced to `NaN`
  - aggregates follow pandas semantics (e.g., sums may ignore missing depending on parameters; means may become `NaN` for all-missing)

- **Event name casing**
  - input events are normalized (`.str.lower()`), so specs should be written case-insensitively

- **Duplicate metric names**
  - stops with a clear error. Names must be unique.

---

## Global behaviors

### Preview vs output

You must choose exactly one:
- `--preview` prints metadata + `head(30)` and exits
- `--out PATH` writes output to `.csv` or `.parquet`

### Input/output formats

Input:
- `.csv`
- `.parquet` / `.pq`

Output:
- `.csv`
- `.parquet` / `.pq`

### Config workflow

- `--save-config PATH`: writes the effective args to JSON (excluding internal argparse values)
- `--config PATH`: loads JSON and fills missing CLI args

For details, see `docs/config.md`.

---

## Edge cases and guarantees

### Timestamp parsing

Event timestamps are parsed into UTC datetimes.
Rows with unparseable timestamps are dropped.

### Sorting

Event rows are sorted by `(user, time)` before:
- choosing first/last variants,
- choosing first/last exposure events,
- and for time-based metrics.

### Users with empty user ids

Event rows with missing/empty user ids (after trimming) are dropped in event conversion.

### Exposure absence

If `--exposure` is provided but no exposure rows exist, conversion stops with an error.
If exposure exists for some users, only those users appear in output.

---

## Practical guidance

### When to use `unit` vs `events`

Use `unit` if:
- each row is truly one user, and outcome already exists

Use `events` if:
- you have event logs and need aggregated outcomes
- you need exposure windows or time-to-event metrics
- you want multiple derived metrics

### Recommended practice for exposure analysis

- Anchor on exposure events for causal interpretation (`--exposure`).
- Add a window to standardize the measurement period (`--window`).
- Prefer strict variant consistency (`--multivariant error`) unless you know your instrumentation produces multiple variant values.

---

## See also

- Config workflow: [`config.md`](config.md)
- Data contract: [`data-contract.md`](data-contract.md)
- Troubleshooting: [`troubleshooting.md`](troubleshooting.md)
