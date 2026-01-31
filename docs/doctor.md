# Doctor (`ab doctor`)

`ab doctor` is a validator for **already-converted, user-level datasets**.

Use it after:

- `ab convert unit ... --out converted.csv`
- `ab convert events ... --out converted.csv`

It checks that your dataset is **canonical** (one row per user, valid variants, sane metric columns) and helps you decide whether it’s ready for analysis.

---

## Table of contents

- [Quick start](#quick-start)
- [What doctor expects (data contract)](#what-doctor-expects-data-contract)
- [Checks](#checks)
  - [integrity](#integrity)
  - [variants](#variants)
  - [missingness](#missingness)
  - [metrics](#metrics)
  - [consistency](#consistency)
  - [distribution](#distribution)
  - [allocation (SRM-style)](#allocation-srm-style)
- [CLI reference](#cli-reference)
- [Examples](#examples)
- [Reports](#reports)
- [Exit codes](#exit-codes)
- [Config workflow](#config-workflow)
- [Troubleshooting](#troubleshooting)
- [See also](#see-also)

---

## Quick start

Run doctor on a converted file:

```bash
ab doctor --data out/converted.csv
```

By default, doctor will:

- assume `user_id` and `variant` column names,
- infer metric columns automatically,
- run a full check suite,
- print a human-readable report to the console (with examples if `--preview`).

---

## What doctor expects (data contract)

Doctor expects a **user-level** dataset:

- exactly **one row per user**
- a `variant` column with a non-empty assignment per user
- metric columns (binary/count/continuous/time-to-event) that can be analyzed

Doctor does **not** compute outcomes. It only validates what you already produced with `ab convert`.

---

## Checks

Doctor runs checks selected by `--check` (and can skip some with `--skip`).

Default check suite is:

```
integrity,variants,missingness,metrics,consistency,distribution,allocation
```

### integrity

Hard requirements for canonical user-level data:

- missing/empty `user` values → **ERROR**
- missing/empty `variant` values → **ERROR**
- duplicate `user` rows (more than one row per user) → **ERROR**

### variants

Variant overview and basic arm sanity:

- INFO table: number of users per variant + percentages
- WARN if only one variant is present
- optional `--min-n N`: WARN if any arm has fewer than N users

### missingness

For each metric column:

- missing rate overall
- missing rate by variant (worst/best arm)
- warns on unusually high missingness

Use this to catch:
- columns that were not computed correctly
- time-to-event columns that are missing for almost everyone
- unexpected NaNs in continuous metrics

### metrics

Metric-type sanity checks (best-effort):

- binary-like columns should look like 0/1
- count-like columns should be non-negative integers
- continuous columns should be numeric
- time columns should parse / have reasonable values

Doctor cannot infer your *intent* perfectly; it flags suspicious patterns and shows examples.

### consistency

Consistency warnings that often indicate upstream cleaning problems:

- variant column has casing/whitespace inconsistencies (should be strip + lower)
- placeholder-like variant values (`none`, `null`, `nan`, `n/a`, `undefined`, `?`) → WARN

### distribution

Distribution heuristics:

- extreme outliers
- heavily skewed distributions
- suspicious constant columns

This check is helpful, but can also flag “normal” patterns like:
- revenue being zero for most users (zero-inflation)
- long-tailed purchase amounts

If you want doctor to be strictly “no warnings”, you can skip distribution:

```bash
ab doctor --data out/converted.csv --skip distribution
```

### allocation (SRM-style)

Allocation checks compare observed counts vs expected allocation proportions.

Enable by providing `--allocation`:

- `equal` — expects equal split across variants
- explicit spec: `A=0.5,B=0.3,C=0.2`

Doctor computes a chi-square statistic and (if SciPy is available) a p-value.

Important notes:

- allocation checks assume each user is counted once (canonical table)
- for many-armed experiments, small arms can trigger warnings unless you set `--min-n`

---

## CLI reference

### Required

- `--data PATH` — converted `.csv` or `.parquet`

### Common options

- `--user COL` — user column name (default: `user_id`)
- `--variant COL` — variant column name (default: `variant`)
- `--metrics COL,COL` — metric columns to check (default: all columns except user/variant)
- `--ignore COL,COL` — columns to exclude from metric checks (for “keep” cols)
- `--check NAME,NAME` — which checks to run
- `--skip NAME,NAME` — checks to skip
- `--preview` — print example rows (enabled by default when no `--report` is given)
- `--report PATH` — write report to `.md` or `.json`
- `--only errors|warnings|all` — filter console output
- `--fail-on error|warn` — exit nonzero on errors only, or on errors+warnings
- `--no-exit` — always exit 0 (useful in interactive debugging)

### Allocation options

- `--allocation SPEC` — `equal` or `A=0.5,B=0.3`
- `--alpha FLOAT` — significance threshold for allocation/SRM checks (default: 0.01)
- `--min-n N` — warn if any variant has fewer than N users

### Config options

- `--save-config PATH` — save args to JSON
- `--config PATH` — load args from JSON (fills only `None` args)

---

## Examples

### 1) Default run (best first command)

```bash
ab doctor --data out/converted.csv
```

### 2) Explicit checks + explicit metrics

```bash
ab doctor --data out/converted.csv \
  --check integrity,variants,missingness,metrics,consistency \
  --metrics conversion,purchases,revenue,refunds,tt_purchase
```

### 3) Allocation check (equal split)

```bash
ab doctor --data out/converted.csv --check allocation --allocation equal
```

### 4) Allocation check (explicit proportions)

```bash
ab doctor --data out/converted.csv --check allocation \
  --allocation "control=0.50,treatment=0.50" --alpha 0.01
```

### 5) “Strict mode”: fail on warnings too

```bash
ab doctor --data out/converted.csv --fail-on warn
```

### 6) Generate a Markdown report

```bash
ab doctor --data out/converted.csv --report reports/doctor.md
```

### 7) Skip distribution (common for revenue-like metrics)

```bash
ab doctor --data out/converted.csv --skip distribution
```

---

## Reports

Doctor can write:

- `.md` — human-readable report with sections
- `.json` — machine-readable list of findings

```bash
ab doctor --data out/converted.csv --report reports/doctor.json
```

---

## Exit codes

Doctor determines “failure” based on severities:

- `ERROR` findings always count as failure
- `WARN` findings count as failure only if `--fail-on warn`
- `--no-exit` forces exit code 0

This lets you use doctor in pipelines:

- strict CI: `--fail-on warn`
- permissive dev: default (fail only on errors)
- exploratory: `--no-exit`

---

## Config workflow

Save a doctor config:

```bash
ab doctor --data out/converted.csv \
  --check integrity,variants,missingness,metrics,consistency \
  --metrics conversion,revenue \
  --save-config configs/doctor_basic.json
```

Reuse it (override only the dataset path):

```bash
ab doctor --config configs/doctor_basic.json --data out/converted_new.csv
```

---

## Troubleshooting

- If allocation checks fail with SciPy errors, install SciPy:

```bash
python -m pip install scipy
```

- If you see warnings about variant casing/whitespace, fix upstream conversion so variants are normalized (strip + lower).

- If you want fewer warnings for expected long-tailed metrics (revenue), skip distribution.

---

## See also

- Conversion reference: [`convert.md`](convert.md)
- Config workflow: [`config.md`](config.md)
- Data contract: [`data-contract.md`](data-contract.md)
- Troubleshooting: [`troubleshooting.md`](troubleshooting.md)
