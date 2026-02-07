# Data contract

This document defines the **input expectations** and the **cleaning guarantees** provided by `ab convert`.

It is intended to be strict enough for reliable pipelines, but practical enough for real messy datasets.

---

## Scope

This contract applies to:

- `ab convert unit`
- `ab convert events`

It covers:
- supported file types,
- required columns,
- accepted data types,
- cleaning/normalization steps,
- what is guaranteed in outputs,
- what is *not* guaranteed (and must be validated separately).

---

## Supported file formats

### Input
- `.csv`
- `.parquet` / `.pq`

### Output
- `.csv`
- `.parquet` / `.pq`

If an input/output file type is not supported, `ab` stops with an error.

---

## Column naming and selection

`ab` does **not** require specific column names in your raw data.
Instead, you point `ab` to the correct columns using CLI flags like `--user`, `--variant`, `--time`, etc.

Example:

```bash
ab convert events --user uid --variant grp --time timestamp --event action ...
```

`ab` validates that the chosen column names exist in the input dataset before proceeding.

---

## `convert unit` contract

### Required columns (as specified by CLI)
You must provide:

- `--user COL` (user identifier)
- `--variant COL` (treatment/variant label)
- **one of**:
  - `--outcome COL` (legacy single outcome column), **or**
  - one or more `--metric SPEC` (metric DSL; repeatable)

Optionally:
- `--keep COL,COL,...` (extra columns to keep)
- `--segment COL` (segment columns; repeatable)


All columns referenced by these flags must exist in the input dataset.

### Input expectations
- **User column**: should identify a user/unit. Can be string or numeric; it will be coerced to string.
- **Variant column**: should be a categorical label. It will be coerced to string.
- **Outcome column**: can be numeric or categorical depending on your use case. `ab` currently copies it as-is after string cleaning logic in the implementation (see note below).

> Note: In legacy `--outcome` mode, `ab` attempts to coerce the outcome to a sensible type based on context and does **not** intentionally lowercase numeric outcomes. In DSL mode, each metric is typed/cleaned by its spec.

### Cleaning guarantees (unit)
- `user_id` output is always string-like (pandas `string`) and whitespace-trimmed.
- `variant` output is always string-like, whitespace-trimmed, and lowercased.
- Canonical output columns always include `user_id` and `variant`.
- Legacy mode (`--outcome`): output includes `outcome`.
- DSL mode (`--metric`): output includes one column per metric spec (typed/cleaned).
- Segment columns (`--segment`) are carried through after being resolved per-user (see below).
- `--keep` columns are preserved and included unmodified.


### Segment stability contract (unit)
If you include `--segment COL` (repeatable), segments must be stable per user (e.g., `country`, `device`).

- If a user has multiple non-null values for a segment column, `--segment-rule {error,first,last,mode}` controls resolution.
- If `--segment-fix` is enabled, segment strings are standardized (e.g., lowercasing, whitespace normalization) **before** stability resolution.

**Guarantee:** after conversion finishes successfully, each output user row has **at most one value** per segment column.

### Duplicates contract (unit)
- If multiple rows exist per user:
  - default behavior is to stop with an error (`--dedupe error`)
  - you can choose `--dedupe first` or `--dedupe last`

**Guarantee:** after conversion finishes successfully, the output will have **at most one row per user**.

---

## `convert events` contract

### Required columns (as specified by CLI)
You must provide:

- `--user COL`
- `--variant COL`
- `--time COL`
- `--event COL`
- at least one `--metric SPEC`

Optionally:
- `--value COL` (default value column for value-based metrics)
- `--segment COL` (segment columns; repeatable)
- `--exposure VALUE` (event label identifying exposure/assignment moment)
- `--window DURATION` (time window after exposure; requires `--exposure`)
- `--unassigned {error,drop,keep}` (events only: what to do if a user ends up with an empty variant after cleaning)

All referenced columns must exist in input data.

### Input expectations
- **User column**: user/unit identifier. Can be string or numeric; coerced to string.
- **Variant column**: categorical label; coerced to string and normalized.
- **Time column**: parseable timestamp values (string, datetime, numeric epoch-like supported by pandas inference). Unparseable rows are dropped.
- **Event column**: categorical event name/type; coerced to string and normalized.
- **Value column (optional)**: numeric-ish values; `ab` attempts to coerce to float (see below).

### Cleaning guarantees (events)

#### User id (`--user`)
- coerced to string and `.str.strip()` applied
- rows with missing/empty user ids after cleaning are dropped

**Guarantee:** output `user_id` contains no empty strings.

#### Variant (`--variant`)
- coerced to string
- `.str.strip().str.lower()` applied

#### Event (`--event`)
- coerced to string
- `.str.strip().str.lower()` applied

This normalization ensures:
- event matching in `--metric` is effectively case-insensitive
- variant labels are case-insensitive

#### Time (`--time`)
- parsed via `pd.to_datetime(..., utc=True, errors="coerce")`
- unparseable timestamps become `NaT` and those rows are dropped
- events are sorted by `(user, time)` after parsing

**Guarantee:** all event rows used for metrics have valid UTC timestamps.

#### Segments (`--segment`, optional)
If provided:
- segment columns are carried through to the output
- segment values are standardized when `--segment-fix` is enabled
- if a segment is inconsistent per user, `--segment-rule {error,first,last,mode,from_exposure}` controls resolution
  - `from_exposure` requires `--exposure` and takes segment values from the exposure row

**Guarantee:** each output user row has at most one resolved value per segment column.

#### Value (`--value`, optional)
If provided:
- coerced to string and trimmed
- a regex strips non-numeric characters except `0-9`, `.`, `-`
- parsed to float via `pd.to_numeric(errors="coerce")`
- invalid values become `NaN`

**Guarantee:** output of value-based metrics is numeric float.

Per-metric overrides:
- In the events Metric DSL, rules that use values accept `value=COL` to override `--value` **for that metric only**.
- All referenced value columns are required and cleaned once before aggregation.
**Non-guarantee:** if your input contains non-numeric garbage, those rows may contribute `NaN` and affect aggregates. Validate separately if needed.

---

## Variant consistency contract (events)

A/B analysis assumes a user belongs to one variant.

`ab` checks if a user has multiple variants in the event log:

- If `--multivariant error` (default): stops and prints example users
- Otherwise, it forces a single variant per user based on the chosen strategy:
  - `first`, `last`, `mode`, `from_exposure`

**Guarantee:** successful output has exactly one `variant` per `user_id`.

---

## Exposure and window contract (events)

### Exposure (`--exposure`)
If `--exposure VALUE` is provided:
- `VALUE` is matched against the normalized event column (`--event`)
- users without at least one exposure event are dropped
- a per-user exposure timestamp is selected based on `--multiexposure`

**Guarantee:** output includes only users with exposure.
**Guarantee:** output includes `exposure_time` for every user.

### Window (`--window`)
If `--window DURATION` is provided:
- it is parsed via `pd.to_timedelta`
- `window_end = exposure_time + window`
- metrics are computed only using events at/after exposure and at/before window_end

**Guarantee:** `window_end` exists and is consistent with `exposure_time` and the parsed duration.
**Non-guarantee:** If window is provided without exposure, current code may treat it as unused—prefer pairing it with exposure.

---

## Metric output guarantees (events)

Metrics are computed using the normalized event column and (optional) scoped event set.

For each user:
- binary and count metrics are filled with `0` when missing
- continuous metrics are filled with `0.0` when missing
- time metrics may be missing (`NaT` / `NaN`) if the event never occurs

**Guarantee:** output is one row per user.
**Guarantee:** every metric name becomes exactly one output column.

---

## What `ab` does *not* guarantee

You may need additional validation outside `ab` for:

- strict numeric validity (e.g., forbid NaNs in value column)
- outlier handling and winsorization
- timezone correctness if raw timestamps are ambiguous
- user identity consistency across sources
- deduplication semantics beyond simple first/last selection
- bot filtering / internal traffic removal
- missingness rules (e.g., require that every user has an outcome)

A dedicated validator command exists: use `ab doctor` to run integrity/missingness/metric checks before analysis.

---

## Examples of “good” inputs

### Unit-level dataset example (conceptual)

Columns:
- `uid`, `treatment`, `revenue`, `country`

Run:
```bash
ab convert unit --user uid --variant treatment --outcome revenue --keep country --preview
```

### Event-level dataset example (conceptual)

Columns:
- `user_id`, `variant`, `ts`, `event`, `amount`

Run:
```bash
ab convert events   --user user_id --variant variant --time ts --event event --value amount   --metric conversion=binary:event_exists(purchase)   --metric revenue=continuous:sum_value(purchase)   --preview
```

---

## See also

- Full conversion + metric reference: [`convert.md`](convert.md)
- Config workflow: [`config.md`](config.md)
- Troubleshooting: [`troubleshooting.md`](troubleshooting.md)
