# Data contract

This document defines the **input expectations** and the **cleaning guarantees** provided by `abx convert`.

It is intended to be strict enough for reliable pipelines, but practical enough for real messy datasets.

---

## Scope

This contract applies to:

- `abx convert unit`
- `abx convert events`

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

If an input/output file type is not supported, `abx` stops with an error.

---

## Column naming and selection

`abx` does **not** require specific column names in your raw data.
Instead, you point `abx` to the correct columns using CLI flags like `--user`, `--variant`, `--time`, etc.

Example:

```bash
abx convert events --user uid --variant grp --time timestamp --event action ...
```

`abx` validates that the chosen column names exist in the input dataset before proceeding.

---

## `convert unit` contract

### Required columns (as specified by CLI)
You must provide:

- `--user COL` (user identifier)
- `--variant COL` (treatment/variant label)
- `--outcome COL` (outcome column)

Optionally:
- `--keep COL,COL,...` (extra columns to keep)

All columns referenced by these flags must exist in the input dataset.

### Input expectations
- **User column**: should identify a user/unit. Can be string or numeric; it will be coerced to string.
- **Variant column**: should be a categorical label. It will be coerced to string.
- **Outcome column**: can be numeric or categorical depending on your use case. `abx` currently copies it as-is after string cleaning logic in the implementation (see note below).

> Note: The current implementation lowercases the `outcome` column in `convert unit` by converting to string and applying `.str.lower()`.
> For many A/B datasets, outcomes are numeric. If your outcomes are numeric, you should ensure they remain numeric (or adjust implementation).
> This document reflects the *intended* contract; if code behavior differs, treat code as source of truth.

### Cleaning guarantees (unit)
- `user_id` output is always string-like (pandas `string`) and whitespace-trimmed.
- `variant` output is always string-like, whitespace-trimmed, and lowercased.
- Output columns are renamed to canonical names:
  - `user_id`, `variant`, `outcome`
- `--keep` columns are preserved and included unmodified.

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
- `--value COL`

All referenced columns must exist in input data.

### Input expectations
- **User column**: user/unit identifier. Can be string or numeric; coerced to string.
- **Variant column**: categorical label; coerced to string and normalized.
- **Time column**: parseable timestamp values (string, datetime, numeric epoch-like supported by pandas inference). Unparseable rows are dropped.
- **Event column**: categorical event name/type; coerced to string and normalized.
- **Value column (optional)**: numeric-ish values; `abx` attempts to coerce to float (see below).

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

#### Value (`--value`, optional)
If provided:
- coerced to string and trimmed
- a regex strips non-numeric characters except `0-9`, `.`, `-`
- parsed to float via `pd.to_numeric(errors="coerce")`
- invalid values become `NaN`

**Guarantee:** output of value-based metrics is numeric float.
**Non-guarantee:** if your input contains non-numeric garbage, those rows may contribute `NaN` and affect aggregates. Validate separately if needed.

---

## Variant consistency contract (events)

A/B analysis assumes a user belongs to one variant.

`abx` checks if a user has multiple variants in the event log:

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

## What `abx` does *not* guarantee

You may need additional validation outside `abx` for:

- strict numeric validity (e.g., forbid NaNs in value column)
- outlier handling and winsorization
- timezone correctness if raw timestamps are ambiguous
- user identity consistency across sources
- deduplication semantics beyond simple first/last selection
- bot filtering / internal traffic removal
- missingness rules (e.g., require that every user has an outcome)

Roadmap item: a dedicated validator command (`abx doctor`) is a natural next step.

---

## Examples of “good” inputs

### Unit-level dataset example (conceptual)

Columns:
- `uid`, `treatment`, `revenue`, `country`

Run:
```bash
abx convert unit --user uid --variant treatment --outcome revenue --keep country --preview
```

### Event-level dataset example (conceptual)

Columns:
- `user_id`, `variant`, `ts`, `event`, `amount`

Run:
```bash
abx convert events   --user user_id --variant variant --time ts --event event --value amount   --metric conversion=binary:event_exists(purchase)   --metric revenue=continuous:sum_value(purchase)   --preview
```

---

## See also

- Full conversion + metric reference: [`convert.md`](convert.md)
- Config workflow: [`config.md`](config.md)
- Troubleshooting: [`troubleshooting.md`](troubleshooting.md)
