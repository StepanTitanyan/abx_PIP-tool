# Config files (`--config`, `--save-config`)

`ab` supports saving and loading command arguments via JSON config files.

> Note: config loading only fills arguments whose current value is `None`. Flags like `--preview` (default `False`) will **not** be turned on by a config; pass them on the CLI each run.
This makes conversions reproducible and easy to rerun across datasets and environments.

---

## Why configs exist

Configs solve three practical problems:

1. **Reproducibility**
   - You can rerun the exact same conversion later without retyping a long CLI command.

2. **Team collaboration**
   - You can commit configs into your repo and use the same conversion definition in CI, notebooks, or across teammates.

3. **Less CLI boilerplate**
   - Only override what changes (usually just `--data` or `--out`).

---

## Quick start

### Save a config

Example: event conversion with revenue metric

```bash
ab convert events   --data data/events.csv   --user user_id   --variant variant   --time ts   --event event   --value amount   --metric revenue=continuous:sum_value(purchase)   --metric conversion=binary:event_exists(purchase)   --save-config configs/events_revenue.json   --preview
```

This writes a JSON config at `configs/events_revenue.json`.

### Reuse a config

```bash
ab convert events   --config configs/events_revenue.json   --out out/revenue.csv
```

> Tip: If your config includes `--data`, you don’t need to pass `--data` again.
> If you want to reuse the same conversion with a different dataset, see the “Override strategy” section below.

---

## What gets saved

When you run with `--save-config PATH`, `ab` writes “effective arguments” to JSON:

- Excludes internal argparse values (like the `func` callback)
- Excludes `save_config` itself
- Excludes `config` (to avoid config files referencing themselves)
- Drops keys whose value is `None`

Implementation detail: boolean flags like `preview` may be saved as `false`; that is fine, but remember configs won’t turn `--preview` on because loading only fills `None` values.

**Example output (illustrative):**
```json
{
  "data": "data/events.csv",
  "user": "user_id",
  "variant": "variant",
  "time": "ts",
  "event": "event",
  "value": "amount",
  "metric": [
    "revenue=continuous:sum_value(purchase)",
    "conversion=binary:event_exists(purchase)"
  ],
  "multivariant": "error",
  "multiexposure": "first",
  "preview": true
}
```

> Note: your exact JSON keys match the argparse option names without the leading `--`.

---

## Loading behavior (current implementation)

When you pass `--config PATH`, `ab`:

1. reads the JSON file into a dict,
2. for each key in the config:
   - if the argparse namespace has that attribute AND
   - the current CLI value is `None`,
   - then it sets the value from config.

### Important consequence

Config values only fill **missing** CLI values (i.e., args that are `None`).

That means config **cannot override** options whose argparse defaults are not `None`.

Examples:

- `--preview` defaults to `False` (not `None`), so config cannot switch it to `true`
- some flags may behave similarly if they have non-None defaults

### Recommended practice with current behavior

- Use configs primarily for “structural” fields that default to `None`:
  - `data`, `user`, `variant`, `time`, `event`, `outcome`, `value`, `window`, `exposure`, `metric`, etc.
- Prefer passing `--preview` or `--out` explicitly on the CLI each run.

If/when config precedence is upgraded in future versions, this document will be updated accordingly.

---

## Override strategy: reusing the same conversion with a new dataset

If you saved a config that includes `data`, but now want to run it on another file, you have two common workflows:

### 1) Save one config per dataset
- simplest, most explicit
- best for long-term reproducibility

### 2) Create a “template config” without `data`
You can manually edit the JSON and remove `"data": ...` so you always pass it in CLI.

Example:

```bash
ab convert events   --config configs/template_events.json   --data data/events_new.csv   --out out/new.csv
```

Since CLI provides `--data`, config will not fill it anyway (it’s already non-None).

---

## Where to store configs

Recommended layout (repo-friendly):

```
abx/
  configs/
    unit_basic.json
    events_post_exposure_7d.json
    events_revenue.json
```

Configs can safely be committed to Git, as long as they don’t contain secrets (they normally shouldn’t).

---

## Validation and failure modes

When loading a config, `ab` will fail fast if:
- the config file does not exist
- JSON is invalid (parse error)

After loading config, conversion still validates:
- required CLI args are set (from CLI or config)
- required input columns exist in the data
- preview/output mode is valid (must choose one)

---

## Tips

- Use `--save-config` together with `--preview` when building a conversion interactively.
- Name configs by intent: `events_exposure_7d_purchase.json` is better than `config1.json`.
- Keep metric specs in configs so your pipeline is stable.

---

## See also

- Full conversion + metric DSL reference: [`convert.md`](convert.md)
- Data contract: [`data-contract.md`](data-contract.md)
- Troubleshooting: [`troubleshooting.md`](troubleshooting.md)


## Repeatable / list flags (metrics, segments)

Some CLI options are **repeatable** (you can pass them multiple times). In configs, these appear as JSON arrays.

Common ones:

- `metric`: list of metric specs (e.g., `["rev=continuous:fix(revenue)", "buy=binary:fix(purchase)"]`)
- `segment`: list of segment columns (e.g., `["country", "device"]`)
- `segment_fix_opt`: list of `KEY=VAL` strings (e.g., `["lower=1", "spaces=underscore"]`)

Doctor also has list-like options:

- `metrics`: either a comma-separated string or an array of metric column names
- `check` / `skip`: comma-separated strings of check names

> Note: boolean flags that default to `False` (like `preview` or `segment_fix`) are **not** enabled by config loading, because configs only fill values that are currently `None`. Pass those flags explicitly on the CLI when you want them.



## Examples

### Doctor config example (per-metric minimum arm size)

```json
{
  "data": "out/converted.csv",
  "user": "user_id",
  "variant": "variant",
  "check": "integrity,variants,missingness,metrics,metric_arm_n,allocation",
  "min_n": 200,
  "min_n_metric": "revenue=500,signup=300",
  "fail_on": "warn",
  "report": "reports/doctor.md"
}
```
