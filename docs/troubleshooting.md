# Troubleshooting

This guide covers the most common issues when running `abx` on real datasets—especially on Windows/PowerShell—and how to fix them quickly.

---

## Table of contents

- [CLI basics](#cli-basics)
- [PowerShell quirks](#powershell-quirks)
- [File path issues](#file-path-issues)
- [CSV/Parquet issues](#csvparquet-issues)
- [Timestamp parsing pitfalls](#timestamp-parsing-pitfalls)
- [Numeric parsing pitfalls (`--value`)](#numeric-parsing-pitfalls---value)
- [Variant/exposure logic errors](#variantexposure-logic-errors)
- [Metric DSL errors](#metric-dsl-errors)
- [Pandas / environment pitfalls](#pandas--environment-pitfalls)
- [Performance tips](#performance-tips)
- [Getting a minimal repro](#getting-a-minimal-repro)

---

## CLI basics

### “Command not found: abx”
**Cause:** `abx` is not installed in the active Python environment.

**Fix:**
```bash
python -m pip install abx
```
or for local dev:
```bash
python -m pip install -e .
```

Verify:
```bash
abx --help
```

### “I ran it but nothing was written”
Most likely you used `--preview` (which intentionally does not write output).

Use `--out` to write:
```bash
abx convert unit ... --out out/result.csv
```

---

## PowerShell quirks

### Line continuation: use backticks or use one line
In PowerShell, the Bash-style `\` line continuation does **not** work the same way.

!!!Preferred: run commands on one line.

Or use PowerShell backticks:

```powershell
abx convert events `
  --data data\events.csv `
  --user user_id `
  --variant variant `
  --time ts `
  --event event `
  --metric conversion=binary:event_exists(purchase) `
  --preview
```

### Quoting parentheses in `--metric`
Parentheses are usually fine, but if you see parsing issues, quote the metric:

```powershell
--metric "conversion=binary:event_exists(purchase)"
```

If you use commas and key/value args, quoting is strongly recommended:

```powershell
--metric "ttp=time:time_to_event(purchase, unit=h)"
```

---

## File path issues

### “File not found” but the file exists
Common causes:
- running from a different working directory than you think
- path contains spaces and isn’t quoted
- using backslashes incorrectly in some shells

Fix:
- Use absolute paths or quote paths:
```bash
abx convert unit --data "C:\Users\me\Desktop\unit.csv" ...
```

- Verify current directory:
```bash
pwd
```
(PowerShell: `Get-Location`)

### “Unsupported file type”
`abx` supports only:
- `.csv`
- `.parquet` / `.pq`

Rename or convert your file.

---

## CSV/Parquet issues

### CSV opens weird / columns shifted
Possible causes:
- delimiter is not comma (e.g., `;`)
- quoting issues
- encoding issues

Current `abx` uses `pandas.read_csv` defaults.
If your CSV uses `;`, you may need to pre-convert or update your loader to accept `--sep` in future versions.

Quick pre-convert in Python:
```python
import pandas as pd
df = pd.read_csv("input.csv", sep=";")
df.to_csv("fixed.csv", index=False)
```

### Parquet read/write fails
Parquet requires an engine:
- `pyarrow` (recommended) or `fastparquet`

Fix:
```bash
python -m pip install pyarrow
```

---

## Timestamp parsing pitfalls

### “All rows got dropped” or output empty
Event conversion drops rows whose timestamps fail parsing.

Symptoms:
- output has far fewer rows than expected
- or none at all

Fix:
- Inspect the time column:
  - does it contain multiple formats?
  - is it Unix epoch?
  - is it missing timezone info?
  - is it full of invalid strings?

Try loading and parsing in a quick check:
```python
import pandas as pd
s = pd.to_datetime(df["ts"], errors="coerce", utc=True)
print(s.isna().mean())
```

If your timestamps are epoch seconds/milliseconds, you may need to convert before using `abx`:
```python
df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)  # or unit="ms"
```

---

## Numeric parsing pitfalls (`--value`)

### “sum_value requires --value”
Value-based metrics require `--value`.

Fix:
```bash
--value amount --metric revenue=continuous:sum_value(purchase)
```

### Values contain currency symbols (e.g., `$12.50`, `AMD 5000`)
`abx` strips non-numeric characters and parses float.
This is usually fine, but some edge cases exist:
- commas as decimal separators (e.g., `12,5`) may parse incorrectly
- thousands separators may parse incorrectly

Recommendation:
- preprocess the value column into a clean numeric column before running `abx` if your locale formatting is complex.

---

## Variant/exposure logic errors

### “Multiple variants per user exist...”
Your event log contains users with more than one variant value.

Fix options:
- clean upstream data so variant is stable per user
- or choose a policy:

```bash
--multivariant first
# or: last, mode, from_exposure
```

### “--multivariant from_exposure requires --exposure”
Self-explanatory: `from_exposure` only makes sense if exposure exists.

Fix:
- add `--exposure exposure_event_name`
- or change multivariant strategy

### “no event rows found for exposure=...”
The exposure value you provided does not match any events after normalization.

Checklist:
- event names are lowercased internally
- leading/trailing spaces are stripped

Fix:
- ensure the exposure name matches your event column after cleaning
- try printing unique event names from your dataset

---

## Metric DSL errors

### “Bad --metric spec” / “expected RULE(...)”
Your metric string must follow:

```
NAME=TYPE:RULE(EVENT[, key=value ...])
```

Examples:
- `conversion=binary:event_exists(purchase)`
- `revenue=continuous:sum_value(purchase)`
- `ttp=time:time_to_event(purchase, unit=h)`

PowerShell tip: wrap metrics in quotes.

### “Duplicate metric name”
Each metric name becomes an output column, so names must be unique.

Fix:
- rename the metric output column:
  - `revenue_7d=continuous:sum_value(purchase)`

### “Unsupported metric...”
Only the documented metric rules are supported.
See `docs/convert.md` for the authoritative list.

---

## Pandas / environment pitfalls

### Pandas version issues
If you see errors about datetime parsing or `format="mixed"` behavior, your pandas may be too old.

Fix:
```bash
python -m pip install -U pandas
```

### Parquet engine missing
Install `pyarrow` (recommended):
```bash
python -m pip install pyarrow
```

### Mixed-type columns become object and behave unexpectedly
This is common in CSV imports.
Recommendation:
- explicitly clean/normalize columns upstream if you depend on strict typing
- use `--preview` frequently and inspect the first 30 rows

---

## Performance tips

- Prefer Parquet for large datasets.
- Avoid unnecessary columns in your input if possible.
- If the dataset is extremely large, consider pre-filtering to relevant date ranges or event types.

---

## Getting a minimal repro

When something fails, create a minimal dataset with:
- 5–20 rows
- only the required columns
- the smallest set of events that triggers the issue

Then run with `--preview` and share:
- the command you used
- the first few rows of the dataset
- the exact error message

This makes debugging and bug reports fast and high quality.

---

## See also

- Full conversion reference: [`convert.md`](convert.md)
- Config workflow: [`config.md`](config.md)
- Data contract: [`data-contract.md`](data-contract.md)
