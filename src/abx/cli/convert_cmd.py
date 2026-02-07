import argparse
import pandas as pd
from pathlib import Path
import json
_EVENTS_METRIC_EXAMPLES_TEXT = Path(__file__).with_name("EVENTS_METRIC_EXAMPLES_TEXT.txt")
_UNIT_METRIC_EXAMPLES_TEXT = Path(__file__).with_name("UNIT_METRIC_EXAMPLES_TEXT.txt")

def add_convert_subcommand(subparsers: argparse._SubParsersAction) -> None:
    convert_parser = subparsers.add_parser( "convert", help="| Convert data to canonical user-level format")
    convert_subparsers = convert_parser.add_subparsers(dest="convert_cmd", required=True)

    #ab convert unit
    unit_parser = convert_subparsers.add_parser("unit", help="| Manual conversion/standardization for unit-level data (one row per user)")
    unit_parser.add_argument("--data", metavar="PATH", default=None, help="| Path to CSV or Parquet file")
    unit_parser.add_argument("--user", metavar="COL", default=None, help="| User/unit id column name")
    unit_parser.add_argument("--variant", metavar="COL", default=None, help="| Treatment/variant column name")
    unit_parser.add_argument("--outcome", metavar="COL", default=None, help="| Outcome/metric column name")
    unit_parser.add_argument("--metric", metavar="SPEC", action="append", default=None, help="| Metric spec (repeatable). Unit: NAME=TYPE:fix(COL). See: ab convert unit --examples")
    unit_parser.add_argument("--examples",action="store_true",help="| Print metric DSL examples and exit")
    unit_parser.add_argument("--segment", metavar="COL", action="append", default=None, help="| Segment column (repeatable). Example: --segment country --segment device")
    unit_parser.add_argument("--segment-rule", choices=["error", "first", "last", "mode"], default="error", help="| How to resolve inconsistent segment values per user (default: error)")
    unit_parser.add_argument("--segment-fix", action="store_true", help="| Apply string standardization to all segment columns before resolving.")
    unit_parser.add_argument("--segment-fix-opt", action="append", default=None, metavar="KEY=VAL", help="| Segment fix option (repeatable). Example: --segment-fix-opt lower=1 --segment-fix-opt spaces=underscore")
    unit_parser.add_argument("--keep", metavar="COL,COL",default=None, help="| Comma-separated extra columns to keep (optional)")
    unit_parser.add_argument("--dedupe", choices=["error", "first", "last"], default=None, help="| What to do if multiple rows per user exist")
    unit_parser.add_argument("--out", metavar="PATH", help="| Output path (.csv or .parquet) (either --preview or --out)")
    unit_parser.add_argument("--preview", action="store_true", help="| Preview converted data without outputing (either --preview or --out)")
    unit_parser.add_argument("--save-config", metavar="PATH", default=None, help="| Write merged arguments to a JSON config file (optional, should end in .json)")
    unit_parser.add_argument("--config", metavar="PATH", default=None, help="| Load arguments from a JSON config file (optional, should end in .json)")
    unit_parser.set_defaults(func=_run_unit)

    #ab convert event
    events_parser = convert_subparsers.add_parser("events", help="| Manual conversion for event-level data (one row per event)")
    events_parser.add_argument("--data", metavar = "PATH", default=None, help="| Path to CSV or Parquet file")
    events_parser.add_argument("--user", metavar = "COL", default=None, help="| User/unit id column name")
    events_parser.add_argument("--variant", metavar = "COL", default=None, help="| Treatment/variant column name")
    events_parser.add_argument("--time", metavar = "COL", default=None, help="| Event timestamp column name")
    events_parser.add_argument("--event", metavar = "COL", default=None, help="| Event type/category column name")
    events_parser.add_argument("--value", metavar = "COL", default=None, help="| Event numeric value column name (like purchase amount)")
    events_parser.add_argument("--exposure", metavar = "VALUE", default=None, help="| Value in Event type column identifying exposure. Users without exposure are dropped")
    events_parser.add_argument("--multiexposure", choices=["error", "first", "last"], default=None, help="| A fallback if there are multiple exposure event per user (default: first)")
    events_parser.add_argument("--unassigned", choices=["error", "drop", "keep"], default=None, help="| What to do if a user has no assigned variant after conversion (default: error)")
    events_parser.add_argument("--multivariant", choices=["error", "first", "last", "mode", "from_exposure"], default=None, help="| A fallback if there are multiple variants per user (default: error)")
    events_parser.add_argument("--window", metavar = "DURATION", default=None, help="| Outcome window after exposure (e.g., 7d, 24h). Only used when exposure-event is provided")
    events_parser.add_argument("--metric", metavar="SPEC", action="append", default=None, help="| Metric spec (repeatable). Events: NAME=TYPE:RULE(EVENT[, key=value ...]). See: ab convert events --examples")
    events_parser.add_argument("--examples",action="store_true",help="| Print metric DSL examples and exit")
    events_parser.add_argument("--segment", metavar="COL", action="append", default=None, help="| Segment column (repeatable). Example: --segment country --segment device")
    events_parser.add_argument("--segment-rule", choices=["error", "first", "last", "mode", "from_exposure"], default="error", help="| How to resolve inconsistent segment values per user (default: error). from_exposure requires --exposure.")
    events_parser.add_argument("--segment-fix", action="store_true", help="| Apply string standardization to all segment columns before resolving.")
    events_parser.add_argument("--segment-fix-opt", action="append", default=None, metavar="KEY=VAL", help="| Segment fix option (repeatable). Example: --segment-fix-opt lower=1 --segment-fix-opt spaces=underscore")
    events_parser.add_argument("--out", metavar="PATH", help="| Output path (.csv or .parquet) (either --preview or --out)")
    events_parser.add_argument("--preview", action="store_true", help="| Preview converted data without outputing (either --preview or --out)")
    events_parser.add_argument("--save-config", metavar="PATH", default=None, help="| Write merged arguments to a JSON config file (optional, should end in .json)")
    events_parser.add_argument("--config", metavar="PATH", default=None, help="| Load arguments from a JSON config file (optional, should end in .json)")
    events_parser.set_defaults(func=_run_events)
################################################################################################################
################################################################################################################

def _load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    suf = path.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(path)
    if suf in (".parquet", ".pq"):
        return pd.read_parquet(path)

    raise SystemExit("Unsupported file type. Use .csv or .parquet")


def _write_df(df: pd.DataFrame, out_path: Path) -> None:
    suf = out_path.suffix.lower()
    if suf == ".csv":
        df.to_csv(out_path, index=False)
        return
    if suf in (".parquet", ".pq"):
        df.to_parquet(out_path, index=False)
        return
    raise SystemExit("Unsupported output type. Use .csv or .parquet")


def _require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}\nAvailable columns: {list(df.columns)}")


def _parse_keep(keep: str) -> list[str]:
    keep = keep.strip()
    if not keep:
        return []
    return [c.strip() for c in keep.split(",") if c.strip()]


def _save_config(args: argparse.Namespace, path: Path) -> None:
    cfg = vars(args).copy()

    #Remove argparse internals / things you don’t want persisted
    cfg.pop("func", None)
    cfg.pop("save_config", None)

    #You usually also don’t want to save "config" path inside the config itself
    cfg.pop("config", None)

    #Drop keys with None so the file is clean
    cfg = {k: v for k, v in cfg.items() if v is not None}

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _load_config(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in config file {path}: {e}")

def _strip_edge_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s


def _check_metric(lower_first, name, m_type, rule, kwargs, sing_spec):
        #Validate type/rule early for better UX
        #Unit mode uses lower_first=False in code, events uses lower_first=True.
        if lower_first is False:
            #unit convert: TYPE:fix(COL[, ...])
            allowed_types = {"binary", "continuous", "count", "string"}
            allowed_rules_by_type = {"binary": {"fix"}, "continuous": {"fix"}, "count": {"fix"}, "string": {"fix"},}
        else:
            #events convert
            allowed_types = {"binary", "continuous", "count", "time"}
            allowed_rules_by_type = {"binary": {"event_exists", "event_count_ge"},"count": {"count_event", "unique_event_days"},"continuous": {"sum_value", "mean_value", "median_value", "max_value", "last_value"},"time": {"first_time", "last_time", "time_to_event", "time_to_nth_event"},}

        if m_type not in allowed_types:
            raise SystemExit(
                f"Bad --metric: unknown type '{m_type}' in '{sing_spec}'. Allowed types: {sorted(list(allowed_types))}")

        if rule not in allowed_rules_by_type.get(m_type, set()):
            allowed_rules = sorted(list(allowed_rules_by_type.get(m_type, set())))
            raise SystemExit(
                f"Bad --metric: unsupported rule '{rule}' for type '{m_type}' in '{sing_spec}'. Allowed for {m_type}: {allowed_rules}")

        if lower_first is True and m_type == "binary" and rule == "event_count_ge":
            if "n" in kwargs:
                n_raw = str(kwargs.get("n", "")).strip()
                try:
                    n = int(n_raw)
                except Exception:
                    raise SystemExit(f"Bad --metric: {name} event_count_ge requires integer n (example: n=2). Got n={n_raw!r}")
                if n < 1:
                    raise SystemExit(f"Bad --metric: {name} event_count_ge requires n>=1. Got n={n}")

        if lower_first is True and m_type == "time" and rule == "time_to_nth_event":
            if "n" in kwargs:
                n_raw = str(kwargs.get("n", "")).strip()
                try:
                    n = int(n_raw)
                except Exception:
                    raise SystemExit(f"Bad --metric: {name} time_to_nth_event requires integer n (example: n=2). Got n={n_raw!r}")
                if n < 1:
                    raise SystemExit(f"Bad --metric: {name} time_to_nth_event requires n>=1. Got n={n}")
            if "unit" in kwargs:
                unit = str(kwargs.get("unit", "")).strip().lower()
                if unit and unit not in {"s", "m", "h", "d"}:
                    raise SystemExit(f"Bad --metric: {name} bad unit='{unit}'. Use s/m/h/d (example: unit=h)")


def _parse_metric_call(call: str, lower_first: bool = True) -> tuple[str, dict]:
    call = call.strip()
    if not call:
        raise SystemExit("Bad --metric: empty parentheses ()")
    if any(p.strip() == "" for p in call.split(",")):
        raise SystemExit("Bad --metric: empty argument (double comma or trailing comma).")

    parts = [p.strip() for p in call.split(",") if p.strip()]
    event = parts[0].strip()
    event = _strip_edge_quotes(event)

    if lower_first:
        event = event.lower()
    if "," in event:
        raise SystemExit("Bad --metric: commas are not allowed inside event names. Use event_exists(purchase) not event_exists(pur,chase).")

    kwargs: dict[str, str] = {}
    for p in parts[1:]:
        if "=" not in p:
            raise SystemExit(f"Bad --metric arg '{p}'. Use key=value (example: unit=h)")
        k, v = p.split("=", 1)
        k = k.strip().lower()
        k = _strip_edge_quotes(k)
        v = v.strip()
        v = _strip_edge_quotes(v)
        if '"' in k or "'" in k:
            clean_k = k.replace('"', "").replace("'", "")
            raise SystemExit(f"Bad --metric arg '{p}': kwarg names must not be quoted (use {clean_k}=..., not quoted).")

        if "," in v:
            raise SystemExit(f"Bad --metric: commas are not allowed inside values for '{k}'. Rename the column or avoid commas.")
        if not k or not v:
            raise SystemExit(f"Bad --metric arg '{p}'. Use key=value (example: unit=h)")

        kwargs[k] = v
    return event, kwargs


def _deconstruct_metric(spec: list[str], lower_first: bool = True) -> dict:
    spec_dict = {}
    seen = set()

    for i, raw in enumerate(spec):
        sing_spec = raw.strip()

        if "=" not in sing_spec or ":" not in sing_spec:
            raise SystemExit(f"Bad --metric spec: {sing_spec}")

        name, rest = sing_spec.split("=", 1)
        name = name.strip()

        if not name:
            raise SystemExit(f"Bad --metric (empty name): {sing_spec}")
        if name in seen:
            raise SystemExit(f"Duplicate metric name '{name}'. Use different metric column names.")
        seen.add(name)

        m_type, rest = rest.split(":", 1)
        m_type = m_type.strip().lower()

        if "(" not in rest or not rest.endswith(")"):
            raise SystemExit(f"Bad rule syntax in --metric: {sing_spec} (expected RULE(...))")

        rule, call = rest.split("(", 1)
        rule = rule.strip().lower()
        call = call[:-1]  # remove trailing ')'
        event, kwargs = _parse_metric_call(call, lower_first=lower_first)
        _check_metric(lower_first, name, m_type, rule, kwargs, sing_spec)

        if not m_type or not rule or not event:
            raise SystemExit(f"Bad syntax in --metric: {sing_spec}")
        spec_dict[i] = [name, m_type, rule, event, kwargs]
    return spec_dict

def _normalize_segment_series(s: pd.Series) -> pd.Series:
    #Keep values as strings, treat empty/whitespace as NA
    x = s.astype("string").str.strip()
    x = x.mask(x.isna() | (x == ""), pd.NA)
    return x


def _parse_kv_list(opts: list[str] | None) -> dict[str, str]:
    if not opts:
        return {}
    out: dict[str, str] = {}
    for raw in opts:
        raw = str(raw).strip()
        if not raw:
            continue
        if "=" not in raw:
            raise SystemExit(f"[Error] Bad --segment-fix-opt '{raw}'. Use KEY=VAL.")
        k, v = raw.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if not k or not v:
            raise SystemExit(f"[Error] Bad --segment-fix-opt '{raw}'. Use KEY=VAL.")
        out[k] = v
    return out


def _string_fix_series(s: pd.Series, kwargs: dict[str, str]) -> pd.Series:
    x = s.astype("string")
    x = x.str.strip()
    default_dropchars = r"/\|:;,."

    lower = str(kwargs.get("lower", 1)).strip().lower()
    spaces = str(kwargs.get("spaces", "underscore")).strip().lower()
    collapse = str(kwargs.get("collapse", 1)).strip().lower()
    strip_separators = str(kwargs.get("strip_separators", 1)).strip().lower()
    dropchars = str(kwargs.get("dropchars", "auto")).strip()
    empty_to_na = str(kwargs.get("empty_to_na", 1)).strip().lower()

    if lower not in {"0","1"}:
        raise SystemExit(f"[Error] --segment-fix-opt bad lower='{lower}'. Use 0/1 (example: lower=1)")
    if spaces not in {"underscore", "dash", "space"}:
        raise SystemExit(f"[Error] --segment-fix-opt bad spaces='{spaces}'. Use underscore/dash/space (example: spaces=underscore)")
    if collapse not in {"0","1"}:
        raise SystemExit(f"[Error] --segment-fix-opt bad collapse='{collapse}'. Use 0/1 (example: collapse=1)")
    if strip_separators not in {"0","1"}:
        raise SystemExit(f"[Error] --segment-fix-opt bad strip_separators='{strip_separators}'. Use 0/1 (example: strip_separators=1)")
    if empty_to_na not in {"0","1"}:
        raise SystemExit(f"[Error] --segment-fix-opt bad empty_to_na='{empty_to_na}'. Use 0/1 (example: empty_to_na=1)")
    if "," in dropchars:
        raise SystemExit("[Error] --segment-fix-opt dropchars cannot contain ',' because metric args are comma-separated. Use dropchars=any other char or dropchars=auto.")

    if dropchars == "auto":
        dropchars = default_dropchars

    if lower == "1":
        x = x.str.lower()

    if dropchars:
        import re
        pat = rf"[{re.escape(dropchars)}]"
        x = x.str.replace(pat, " ", regex=True)
        x = x.str.replace(r"\s+", " ", regex=True)

    if spaces == "underscore":
        x = x.str.replace(r"[\s\-]+", "_", regex=True)
    elif spaces == "dash":
        x = x.str.replace(r"[\s_]+", "-", regex=True)
    else:  #"space"
        x = x.str.replace(r"[_\-]+", " ", regex=True)
        x = x.str.replace(r"\s+", " ", regex=True)

    if collapse == "1":
        if spaces == "underscore":
            x = x.str.replace(r"_+", "_", regex=True)
        elif spaces == "dash":
            x = x.str.replace(r"-+", "-", regex=True)
        else:
            x = x.str.replace(r"\s+", " ", regex=True)
    if strip_separators == "1":
        if spaces == "underscore":
            x = x.str.strip("_")
        elif spaces == "dash":
            x = x.str.strip("-")
        else:
            x = x.str.strip()

    if empty_to_na == "1":
        x = x.mask(x.isna() | (x == "") | (x == "_") | (x == "-") | (x.str.strip() == ""), pd.NA)

    return x


def _resolve_segments(df: pd.DataFrame, user_col: str, seg_cols: list[str], rule: str, time_col: str | None = None, exposure_value: str | None = None, event_col: str | None = None, multiexposure: str | None = None, segment_fix_kwargs: dict[str, str] | None = None) -> pd.DataFrame:
    #Return a table: user_id + segment columns, one row per user.
    if not seg_cols:
        return pd.DataFrame({user_col: df[user_col].drop_duplicates().tolist()})

    #Normalize
    work = df[[user_col] + ([time_col] if time_col else []) + ([event_col] if event_col else []) + seg_cols].copy()
    for c in seg_cols:
        work[c] = _normalize_segment_series(work[c])
        if segment_fix_kwargs is not None:
            work[c] = _string_fix_series(work[c], segment_fix_kwargs)

    #from_exposure: take segment values from the exposure event row per user
    if rule == "from_exposure":
        if not exposure_value or not event_col:
            raise SystemExit("[Stopped] --segment-rule from_exposure requires --exposure and --event.")
        exp = str(exposure_value).strip().lower()
        if exp == "":
            raise SystemExit("[Stopped] --segment-rule from_exposure: exposure value is empty.")
        #exposure rows only
        exp_df = work[work[event_col].astype("string").str.strip().str.lower() == exp].copy()
        if exp_df.empty:
            raise SystemExit(f"[Stopped] --segment-rule from_exposure: no exposure rows found for exposure='{exp}'.")
        if time_col:
            exp_df = exp_df.sort_values([user_col, time_col])
        #choose FIRST exposure row per user
        keep = "first"
        if multiexposure in {"first", "last"}:
            keep = multiexposure

        chosen = exp_df.drop_duplicates(subset=[user_col], keep=keep)[[user_col] + seg_cols].copy()
        return chosen

    #error: <=1 unique non-null per user per segment)
    if rule == "error":
        for c in seg_cols:
            nuniq = work.groupby(user_col)[c].nunique(dropna=True)
            bad_users = nuniq[nuniq > 1]
            if not bad_users.empty:
                ex_users = bad_users.index[:10].tolist()
                ex = work[work[user_col].isin(ex_users)][[user_col, c]].dropna().head(30)
                raise SystemExit(f"[Stopped] Segment column '{c}' is not stable for {len(bad_users)} users. Use --segment-rule first/last/mode/from_exposure or fix upstream. Examples:\n{ex.to_string(index=False)}")
        if time_col:
            work = work.sort_values([user_col, time_col])
        out = work.drop_duplicates(subset=[user_col], keep="first")[[user_col] + seg_cols].copy()
        return out

    #first/last/mode
    if rule in {"first", "last"}:
        if time_col:
            work = work.sort_values([user_col, time_col])
        keep = "first" if rule == "first" else "last"
        out = work.drop_duplicates(subset=[user_col], keep=keep)[[user_col] + seg_cols].copy()
        return out

    if rule == "mode":
        out = work[[user_col] + seg_cols].copy()
        def _mode_pick(series: pd.Series):
            y = series.dropna()
            if y.empty:
                return pd.NA
            vc = y.value_counts()
            top = vc.max()
            tied = sorted(vc[vc == top].index.astype(str).tolist())
            return tied[0] if tied else pd.NA

        agg = {c: _mode_pick for c in seg_cols}
        out = out.groupby(user_col, as_index=False).agg(agg)
        return out

    raise SystemExit(f"[Stopped] Bad --segment-rule '{rule}'.")

################################################################################################################
################################################################################################################
def _run_unit(args: argparse.Namespace) -> None:
    if getattr(args, "examples", False):
        print(_UNIT_METRIC_EXAMPLES_TEXT.read_text(encoding="utf-8"))
        return

    #Load config first (so it can fill args.user/variant/outcome/etc)
    if args.config:
        print("Reading specified config file......")
        cfg = _load_config(Path(args.config))
        for key, val in cfg.items():
            if hasattr(args, key) and getattr(args, key) is None:
                setattr(args, key, val)

    #Defaults
    if args.keep is None:
        args.keep = ""
    if args.dedupe is None:
        args.dedupe = "error"
    if args.preview and args.out:
        raise SystemExit("Use either --preview or --out, not both.")
    if not args.preview and not args.out:
        raise SystemExit("Missing output. Provide --out or use --preview.")

    #Validate required ARGS
    required_args = ["data", "user", "variant"]
    missing = [k for k in required_args if not getattr(args, k)]
    if missing:
        raise SystemExit(
            f"Missing required arguments: {missing}. Provide them on CLI or via --config.")
    if not getattr(args, "metric", None) and not args.outcome:
        raise SystemExit("Provide either --outcome COL (legacy) or at least one --metric NAME=TYPE:fix(COL).")
    if getattr(args, "metric", None) and args.outcome:
        raise SystemExit("Use either --outcome (legacy single-metric) or --metric (new multi-metric), not both.")

    #Save config if needed
    if args.save_config:
        _save_config(args, Path(args.save_config))
        print(f"[config] saved: {args.save_config}")

    #Load data
    in_path = Path(args.data)
    out_path = Path(args.out) if args.out else None
    df = _load_df(in_path)

    #Clean user + variant
    df[args.user] = df[args.user].astype("string").str.strip()
    df[args.variant] = df[args.variant].astype("string").str.strip().str.lower()
    df = df[df[args.user].notna() & (df[args.user] != "")]

    keep_cols = _parse_keep(args.keep)
    segment_cols = args.segment or []


    #If using the unit metric DSL, compute metric columns from existing columns
    if getattr(args, "metric", None):
        metrics = _deconstruct_metric(args.metric, lower_first=False)

        #Collect required columns (user, variant, plus every metric's source column)
        metric_cols = []
        for _, (m_name, m_type, m_rule, m_col, m_kwargs) in metrics.items():
            if m_rule != "fix":
                raise SystemExit(f"Unsupported unit rule: {m_name}={m_type}:{m_rule}(...). Unit supports only :fix(COL) for now.")
            metric_cols.append(m_col)

        required_cols = [args.user, args.variant] + metric_cols + keep_cols

        #Remove duplicates
        seen_cols = set()
        required_cols_clean = []
        for c in required_cols:
            if c not in seen_cols:
                required_cols_clean.append(c)
                seen_cols.add(c)
        required_cols = required_cols_clean

        _require_columns(df, required_cols)

        #Start canonical output
        out = df[required_cols].copy()
        out = out.rename(columns={args.user: "user_id", args.variant: "variant"})

        #Apply fixes
        for _, (m_name, m_type, m_rule, m_col, m_kwargs) in metrics.items():
            s = out[m_col]

            # ------------------------
            #binary: fix(...)
            # ------------------------
            if m_type == "binary" and m_rule == "fix":
                x = s.astype("string").str.strip().str.lower()
                truthy = {"1", "true", "t", "yes", "y"}
                falsy  = {"0", "false", "f", "no", "n"}

                out[m_name] = pd.NA
                out.loc[x.isin(truthy), m_name] = 1
                out.loc[x.isin(falsy), m_name] = 0

                #Also allow numeric 0/1 already present
                s_num = pd.to_numeric(s, errors="coerce")
                out.loc[s_num == 1, m_name] = 1
                out.loc[s_num == 0, m_name] = 0

                out[m_name] = out[m_name].astype("Int64")

            # ------------------------
            #continuous: fix(...)
            # ------------------------
            elif m_type == "continuous" and m_rule == "fix":
                x = s.astype("string").str.strip()
                x = x.str.replace(r"[^0-9\.\-]+", "", regex=True)
                out[m_name] = pd.to_numeric(x, errors="coerce")

            # ------------------------
            #count: fix(...)
            # ------------------------
            elif m_type == "count" and m_rule == "fix":
                x = s.astype("string").str.strip()
                x = x.str.replace(r"[^0-9\.\-]+", "", regex=True)
                v = pd.to_numeric(x, errors="coerce")
                #Basic integer-ish enforcement
                bad_frac = v.notna() & (v % 1 != 0)
                if bad_frac.any():
                    raise SystemExit(f"[Stopped] {m_name} count:fix({m_col}) has non-integer values (examples={v[bad_frac].head(10).tolist()}).")
                out[m_name] = v.astype("Int64")

            # ------------------------
            #string: fix(...)
            # ------------------------
            elif m_type == "string" and m_rule == "fix":
                x = s.astype("string")
                x = x.str.strip()
                default_dropchars = r"/\|:;,."

                lower = str(m_kwargs.get("lower", 1)).strip().lower()
                spaces = str(m_kwargs.get("spaces", "underscore")).strip().lower()
                collapse = str(m_kwargs.get("collapse", 1)).strip().lower()
                strip_separators = str(m_kwargs.get("strip_separators", 1)).strip().lower()
                dropchars = str(m_kwargs.get("dropchars", "")).strip()
                empty_to_na = str(m_kwargs.get("empty_to_na", 1)).strip().lower()
                map_new_values = str(m_kwargs.get("map", "")).strip()

                if lower not in {"0","1"}:
                    raise SystemExit(f"[Error] {m_name} bad lower='{lower}'. Use 0/1 (example: lower=1)")
                if spaces not in {"underscore", "dash", "space"}:
                    raise SystemExit(f"[Error] {m_name} bad spaces='{spaces}'. Use underscore/dash/space (example: spaces=underscore)")
                if collapse not in {"0","1"}:
                    raise SystemExit(f"[Error] {m_name} bad collapse='{collapse}'. Use 0/1 (example: collapse=1)")
                if strip_separators not in {"0","1"}:
                    raise SystemExit(f"[Error] {m_name} bad strip_separators='{strip_separators}'. Use 0/1 (example: strip_separators=1)")
                if "," in dropchars:
                     raise SystemExit(f"[Error] {m_name} dropchars cannot contain ',' because metric args are comma-separated. Use dropchars=any other char (example: dropchars=/\\|) or use dropchars=auto.")
                if empty_to_na not in {"0","1"}:
                    raise SystemExit(f"[Error] {m_name} bad empty_to_na='{empty_to_na}'. Use 0/1 (example: empty_to_na=1)")
                if dropchars == "auto":
                    dropchars = default_dropchars

                if lower == "1":
                    x = x.str.lower()

                if dropchars:
                    import re
                    pat = rf"[{re.escape(dropchars)}]"
                    x = x.str.replace(pat, " ", regex=True)
                    x = x.str.replace(r"\s+", " ", regex=True)

                if spaces == "underscore":
                    x = x.str.replace(r"[\s\-]+", "_", regex=True)
                elif spaces == "dash":
                    x = x.str.replace(r"[\s_]+", "-", regex=True)
                else:  #"space"
                    x = x.str.replace(r"[_\-]+", " ", regex=True)
                    x = x.str.replace(r"\s+", " ", regex=True)

                if collapse == "1":
                    if spaces == "underscore":
                        x = x.str.replace(r"_+", "_", regex=True)
                    elif spaces == "dash":
                        x = x.str.replace(r"-+", "-", regex=True)
                    else:
                        x = x.str.replace(r"\s+", " ", regex=True)
                if strip_separators == "1":
                    if spaces == "underscore":
                        x = x.str.strip("_")
                    elif spaces == "dash":
                        x = x.str.strip("-")
                    else:
                        x = x.str.strip()

                if empty_to_na == "1":
                     x = x.mask(x.isna() | (x == "") | (x == "_") | (x == "-") | (x.str.strip() == ""), pd.NA)

                if map_new_values:
                    path = Path(map_new_values)
                    if not path.exists():
                        raise SystemExit(f"[Error] {m_name} map file not found: {path}")
                    try:
                        mapping = json.loads(path.read_text(encoding="utf-8"))
                    except Exception as e:
                        raise SystemExit(f"[Error] {m_name} invalid JSON map file {path}: {e}")
                    if not isinstance(mapping, dict):
                        raise SystemExit(f"[Error] {m_name} map file must be a JSON object (dict): {path}")

                    print("Mapping from the specified .json file .....")
                    #Apply value mapping
                    x = x.replace(mapping)
                    #If map values include null, turn into pd.NA
                    x = x.map(lambda z: pd.NA if z is None else z).astype("string")
                    #re-apply empty_to_na after mapping
                    if empty_to_na == "1":
                        x = x.mask(x.isna() | (x == "") | (x == "_") | (x == "-") | (x.str.strip() == ""), pd.NA)
                    print(f"[map] {m_name}: loaded {len(mapping)} entries from {path}")

                out[m_name] = x
            else:
                raise SystemExit(f"Unsupported unit metric type: {m_name}={m_type}:{m_rule}(...). Try binary/continuous/count/string with :fix(COL).")

        #Drop source columns unless user asked to keep them
        for _, (m_name, _t, _r, m_col, _k) in metrics.items():
            if m_col in out.columns and m_col not in keep_cols and m_col not in {"user_id", "variant"}:
                #remove original input metric column
                if m_col != m_name:
                    out = out.drop(columns=[m_col])

    else:
        #Legacy mode: single outcome column
        required_cols = [args.user, args.variant, args.outcome] + keep_cols
        _require_columns(df, required_cols)

        #Outcome: keep numeric if numeric; otherwise clean gently and attempt numeric parse
        s_out = df[args.outcome]
        if pd.api.types.is_numeric_dtype(s_out):
            #Keep numeric as-is
            pass
        else:
            s = s_out.astype("string").str.strip()
            s_num = pd.to_numeric(s, errors="coerce")
            non_missing = s.notna().sum()
            numeric_rate = (s_num.notna().sum() / non_missing) if non_missing else 0.0

            if numeric_rate >= 0.95:
                df[args.outcome] = s_num
            else:
                df[args.outcome] = s

        #Select + rename into canonical df
        out = df[required_cols].copy()
        out = out.rename(columns={args.user: "user_id", args.variant: "variant", args.outcome: "outcome"})

    #Handle duplicates
    dup_mask = out["user_id"].duplicated(keep=False)
    if dup_mask.any():
        n_dup_rows = int(dup_mask.sum())
        n_dup_users = int(out.loc[dup_mask, "user_id"].nunique(dropna=True))

        if args.dedupe == "error":
            raise SystemExit(
                f"Duplicate users detected: {n_dup_users} users across {n_dup_rows} rows. Use --dedupe first or --dedupe last if you want an automatic choice.")
        if args.dedupe == "first":
            out = out.sort_index().drop_duplicates(subset=["user_id"], keep="first")
        elif args.dedupe == "last":
            out = out.sort_index().drop_duplicates(subset=["user_id"], keep="last")

    #Resolve segments if requested
    if segment_cols:
        _require_columns(df, segment_cols)

        segment_fix_kwargs = None
        if getattr(args, "segment_fix", False):
            segment_fix_kwargs = _parse_kv_list(getattr(args, "segment_fix_opt", None))

        seg_tbl = _resolve_segments(df=df[[args.user] + segment_cols].copy(), user_col=args.user, seg_cols=segment_cols, rule=args.segment_rule, time_col=None, segment_fix_kwargs=segment_fix_kwargs)
        seg_tbl = seg_tbl.rename(columns={args.user: "user_id"})
        out = out.merge(seg_tbl, on="user_id", how="left")

    print("=== Converted (unit) ===")
    print(f"input:  {in_path}")
    print(f"output: {out_path if out_path is not None else '(preview only)'}")
    print(f"rows:   {len(out)}")
    print(f"cols:   {list(out.columns)}")
    print("\nhead(30):")
    print(out.head(30).to_string(index=False))

    if args.preview:
        return

    _write_df(out, out_path)

#------------------------------------------------------------------------------------------

def _run_events(args: argparse.Namespace) -> None:
    if getattr(args, "examples", False):
        print(_EVENTS_METRIC_EXAMPLES_TEXT.read_text(encoding="utf-8"))
        return
    #Load config first (to fill args.user/variant/etc)
    if args.config:
        print("Reading specified config file......")
        cfg = _load_config(Path(args.config))
        for key, val in cfg.items():
            if hasattr(args, key) and getattr(args, key) is None:
                setattr(args, key, val)

    #Defaults
    if args.multiexposure is None:
        args.multiexposure = "first"
    if args.multivariant is None:
        args.multivariant = "error"
    if args.unassigned is None:
        args.unassigned = "error"

    if args.exposure:
        after_exposure = True
    else:
        after_exposure = False

    if args.window:
        within_window = True
    else:
        within_window = False

    if args.preview and args.out:
        raise SystemExit("Use either --preview or --out, not both.")
    if not args.preview and not args.out:
        raise SystemExit("Missing output. Provide --out or use --preview.")
    if args.window and not args.exposure:
        raise SystemExit("--window requires --exposure (window is defined relative to exposure_time).")


    #Validate required ARGS
    required_args = ["data", "user", "variant", "time", "event"]
    missing = [k for k in required_args if not getattr(args, k)]
    if missing:
        raise SystemExit(f"Missing required arguments: {missing}. Provide them on CLI or via --config.")

    if not args.metric:
        raise SystemExit("Provide at least one --metric. Example: --metric conversion=binary:event_exists(purchase)")

    #Save config if needed
    if args.save_config:
        _save_config(args, Path(args.save_config))
        print(f"[config] saved: {args.save_config}")

    #Load df
    in_path = Path(args.data)
    out_path = Path(args.out) if args.out else None
    df = _load_df(in_path)

    required_cols = [args.user, args.variant, args.time, args.event]
    _require_columns(df, required_cols)

    #Clean columns
    df[args.user] = df[args.user].astype("string").str.strip()
    df[args.variant] = df[args.variant].astype("string").str.strip().str.lower()
    df[args.event] = df[args.event].astype("string").str.strip().str.lower()
    #Drop rows with missing user_id after cleaning
    df = df[df[args.user].notna() & (df[args.user] != "")]

    #Make values numeric (supports both --value and per-metric value=COL)
    metrics_tmp = _deconstruct_metric(args.metric)
    need_value_rules = {"sum_value", "mean_value", "max_value", "median_value", "last_value"}
    value_cols = set()

    if args.value:
        value_cols.add(args.value)

    for _, (m_name, m_type, m_rule, _ev, m_kwargs) in metrics_tmp.items():
        if m_type == "continuous" and m_rule in need_value_rules:
            col = str(m_kwargs.get("value", args.value) or "").strip()
            if not col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            value_cols.add(col)

    if value_cols:
        _require_columns(df, list(value_cols))
        for col in value_cols:
            s = df[col].astype("string").str.strip()
            s = s.str.replace(r"[^0-9\.\-]+", "", regex=True)
            df[col] = pd.to_numeric(s, errors="coerce")

    segment_cols = args.segment or []
    if segment_cols:
        _require_columns(df, segment_cols)
    if segment_cols and args.segment_rule == "from_exposure" and not args.exposure:
        raise SystemExit("[Stopped] --segment-rule from_exposure requires --exposure.")

    #Timestamp to datetime
    df[args.time] = pd.to_datetime(df[args.time], errors="coerce", utc=True, format="mixed")
    df = df.dropna(subset=[args.time])
    #Sort by user - time
    df = df.sort_values([args.user, args.time])

    #Check if variants are consistent (with multivariant handling)
    per_user_nvars = df.groupby(args.user)[args.variant].nunique(dropna=True)
    if (per_user_nvars > 1).any():
        bad = per_user_nvars[per_user_nvars > 1].index[:10].tolist()

        if args.multivariant == "error":
            raise SystemExit(f"[Stopped] Multiple variants per user exist for {int((per_user_nvars>1).sum())} users (examples={bad}). Ensure variant is constant per user or use --multivariant first/last/mode/from_exposure.")

        elif args.multivariant == "first":
            tmp = df.dropna(subset=[args.variant]).sort_values([args.user, args.time])
            chosen = tmp.drop_duplicates(subset=[args.user], keep="first")[[args.user, args.variant]]
            mapping = dict(zip(chosen[args.user].tolist(), chosen[args.variant].tolist()))
            df[args.variant] = df[args.user].map(mapping).fillna(df[args.variant])

        elif args.multivariant == "last":
            tmp = df.dropna(subset=[args.variant]).sort_values([args.user, args.time])
            chosen = tmp.drop_duplicates(subset=[args.user], keep="last")[[args.user, args.variant]]
            mapping = dict(zip(chosen[args.user].tolist(), chosen[args.variant].tolist()))
            df[args.variant] = df[args.user].map(mapping).fillna(df[args.variant])

        elif args.multivariant == "mode":
            tmp = df.dropna(subset=[args.variant])[[args.user, args.variant]].copy()
            chosen = (tmp.groupby(args.user)[args.variant].agg(lambda s: s.value_counts().index[0]).reset_index())
            mapping = dict(zip(chosen[args.user].tolist(), chosen[args.variant].tolist()))
            df[args.variant] = df[args.user].map(mapping).fillna(df[args.variant])

        elif args.multivariant == "from_exposure":
            if not args.exposure:
                raise SystemExit("[Stopped] --multivariant from_exposure requires --exposure.")
            exposure = args.exposure.strip().lower()
            exp_df = df[df[args.event] == exposure].copy()
            if exp_df.empty:
                raise SystemExit(f"[Events] no event rows found for exposure='{exposure}'. Output will be empty.")
            keep = "last" if args.multiexposure == "last" else "first"
            chosen = (exp_df.sort_values([args.user, args.time]).drop_duplicates(subset=[args.user], keep=keep)[[args.user, args.variant]])
            mapping = dict(zip(chosen[args.user].tolist(), chosen[args.variant].tolist()))
            df[args.variant] = df[args.user].map(mapping).fillna(df[args.variant])

        else:
            raise SystemExit(f"[Stopped] Bad --multivariant '{args.multivariant}'. Use error/first/last/mode/from_exposure.")

    #Multiexposure
    if args.exposure:
        exposure = args.exposure.strip().lower()
        exp_df = df[df[args.event] == exposure].copy()
        if exp_df.empty:
            raise SystemExit(f"[Events] no event rows found for exposure='{exposure}'. Output will be empty.")
        exp_per_user = exp_df.groupby(args.user).size()

        if args.multiexposure:
            broken_user = exp_per_user[exp_per_user > 1].index

            if args.multiexposure == "error" and not broken_user.empty:
                raise SystemExit(f"[Stopped] Multiple exposures found for {len(broken_user)} users (examples={list(broken_user[:10])}). Use --multiexposure first/last or clean the data.")

            elif args.multiexposure == "first" and not broken_user.empty:
                print(f"Multiexposures found (examples={list(broken_user[:10])}), taking the first exposure event as base")
                exp_user_clean = exp_df.sort_values([args.user, args.time]).drop_duplicates(args.user, keep="first")


            elif args.multiexposure == "last" and not broken_user.empty:
                print(f"Multiexposures found (examples={list(broken_user[:10])}), taking the last exposure event as base")
                exp_user_clean = exp_df.sort_values([args.user, args.time]).drop_duplicates(args.user, keep="last")


            else:
                exp_user_clean = exp_df.sort_values([args.user, args.time]).drop_duplicates(args.user, keep="first")

            exposure_tbl = exp_user_clean[[args.user, args.variant, args.time]].rename(columns={args.user: "user_id", args.variant: "variant", args.time: "exposure_time"})

            if args.window:
                try:
                    window = pd.to_timedelta(args.window)
                except Exception:
                    raise SystemExit(f"Bad --window '{args.window}'. Examples: 7d, 24h, 30m")
                exposure_tbl["window_end"] = exposure_tbl["exposure_time"] + window

    #Base users table (one row per user)
    if args.exposure:
        users_tbl = exposure_tbl.copy()
        df_scoped = df.copy()
        join_cols = ["user_id", "exposure_time"]
        if "window_end" in users_tbl.columns:
            join_cols.append("window_end")
        df_scoped = df_scoped.merge(users_tbl[join_cols], how="inner", left_on=args.user, right_on="user_id")
        if after_exposure:
            df_scoped = df_scoped[df_scoped[args.time]>=df_scoped["exposure_time"]]
        if within_window and "window_end" in users_tbl.columns:
            df_scoped = df_scoped[df_scoped[args.time] <= df_scoped["window_end"]]

    else:
        users_tbl = (df[[args.user, args.variant]].drop_duplicates(subset=[args.user]).rename(columns={args.user: "user_id", args.variant: "variant"}).reset_index(drop=True))
        df_scoped = df.copy()

    if segment_cols:
        segment_fix_kwargs = None
        if getattr(args, "segment_fix", False):
            segment_fix_kwargs = _parse_kv_list(getattr(args, "segment_fix_opt", None))

        seg_tbl = _resolve_segments(df=df, user_col=args.user, seg_cols=segment_cols, rule=args.segment_rule, time_col=args.time, exposure_value=args.exposure, event_col=args.event, multiexposure = args.multiexposure, segment_fix_kwargs=segment_fix_kwargs).rename(columns={args.user: "user_id"})
        users_tbl = users_tbl.merge(seg_tbl, on="user_id", how="left")


    #Deconstruct atributes and compute metrics
    metrics = _deconstruct_metric(args.metric)

    for _, (m_name, m_type, m_rule, m_event, m_kwargs) in metrics.items():
        target = m_event.strip().lower()
        # ------------------------
        # binary: event_exists(...)
        # ------------------------
        if m_type == "binary" and m_rule == "event_exists":
            per_user = (df_scoped.assign(_hit=(df_scoped[args.event] == target)).groupby(args.user)["_hit"].any().astype(int).reset_index().rename(columns={args.user: "user_id", "_hit": m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0).astype(int)

        # ------------------------
        # binary: event_count_ge(..., n=INT)
        # ------------------------
        elif m_type == "binary" and m_rule == "event_count_ge":
            n_raw = str(m_kwargs.get("n", "1")).strip()
            try:
                n = int(n_raw)
            except Exception:
                raise SystemExit(f"[Error] {m_name} event_count_ge(...) requires integer n (example: n=2). Got n={n_raw!r}")
            if n < 1:
                raise SystemExit(f"[Error] {m_name} event_count_ge(...) requires n>=1. Got n={n}")
            cnt = df_scoped[df_scoped[args.event] == target].groupby(args.user).size()
            per_user = cnt.ge(n).astype(int).rename(m_name).reset_index().rename(columns={args.user: "user_id"})
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0).astype(int)

        # ------------------------
        # count: count_event(...)
        # ------------------------
        elif m_type == "count" and m_rule == "count_event":
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user).size().reset_index(name=m_name).rename(columns={args.user: "user_id"}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0).astype(int)

        # ------------------------
        # count: unique_event_days(...)
        # ------------------------
        elif m_type == "count" and m_rule == "unique_event_days":
            ev = df_scoped[df_scoped[args.event] == target].copy()
            if ev.empty:
                users_tbl[m_name] = 0
                users_tbl[m_name] = users_tbl[m_name].astype(int)
            else:
                ev["_day"] = ev[args.time].dt.floor("D")
                per_user = (ev.groupby(args.user)["_day"].nunique(dropna=True).reset_index().rename(columns={args.user: "user_id", "_day": m_name}))
                users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
                users_tbl[m_name] = users_tbl[m_name].fillna(0).astype(int)

        # ------------------------
        # continuous: sum_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "sum_value":
            val_col = str(m_kwargs.get("value", args.value) or "").strip()
            if not val_col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[val_col].sum(min_count=1).reset_index().rename(columns={args.user: "user_id", val_col: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0.0)
            users_tbl[m_name] = pd.to_numeric(users_tbl[m_name], errors="coerce")


        # ------------------------
        # continuous: mean_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "mean_value":
            val_col = str(m_kwargs.get("value", args.value) or "").strip()
            if not val_col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[val_col].mean().reset_index().rename(columns={args.user: "user_id", val_col: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = pd.to_numeric(users_tbl[m_name], errors="coerce")

        # ------------------------
        # continuous: median_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "median_value":
            val_col = str(m_kwargs.get("value", args.value) or "").strip()
            if not val_col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[val_col].median().reset_index().rename(columns={args.user: "user_id", val_col: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = pd.to_numeric(users_tbl[m_name], errors="coerce")

        # ------------------------
        # continuous: last_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "last_value":
            val_col = str(m_kwargs.get("value", args.value) or "").strip()
            if not val_col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            ev = df_scoped[df_scoped[args.event] == target][[args.user, args.time, val_col]].copy()
            if ev.empty:
                users_tbl[m_name] = pd.NA
            else:
                ev = ev.sort_values([args.user, args.time])
                per_user = (ev.groupby(args.user)[val_col].last().reset_index().rename(columns={args.user: "user_id", val_col: m_name}))
                users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
                users_tbl[m_name] = pd.to_numeric(users_tbl[m_name], errors="coerce")

        # ------------------------
        # continuous: max_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "max_value":
            val_col = str(m_kwargs.get("value", args.value) or "").strip()
            if not val_col:
                raise SystemExit(f"[Error] {m_name} requires a value column. Provide --value or add value=COL in the metric.")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[val_col].max().reset_index().rename(columns={args.user: "user_id", val_col: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = pd.to_numeric(users_tbl[m_name], errors="coerce")

        # ------------------------
        # time: first_time(...)
        # ------------------------
        elif m_type == "time" and m_rule == "first_time":
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.time].min().reset_index().rename(columns={args.user: "user_id", args.time: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")

        # ------------------------
        # time: last_time(...)
        # ------------------------
        elif m_type == "time" and m_rule == "last_time":
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.time].max().reset_index().rename(columns={args.user: "user_id", args.time: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")

        # ------------------------
        # time: time_to_event(..., unit=s|m|h|d)
        # ------------------------
        elif m_type == "time" and m_rule == "time_to_event":
            if not args.exposure:
                raise SystemExit(f"[Error] {m_name} time_to_event(...) requires --exposure (needs exposure_time).")
            if "exposure_time" not in users_tbl.columns:
                raise SystemExit(f"[Error] {m_name} requires exposure_time column, but it was not created.")
            unit = str(m_kwargs.get("unit", "s")).strip().lower()
            if unit not in {"s", "m", "h", "d"}:
                raise SystemExit(f"[Error] {m_name} bad unit='{unit}'. Use s/m/h/d (example: unit=h)")
            #First occurrence time in the *scoped* df (already after exposure + within window if enabled)
            first_tbl = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.time].min().reset_index().rename(columns={args.user: "user_id", args.time: "_first_time"}))
            tmp = users_tbl[["user_id", "exposure_time"]].merge(first_tbl, on="user_id", how="left")
            delta = tmp["_first_time"] - tmp["exposure_time"]  #Timedelta or NaT
            #Convert to requested unit
            seconds = delta.dt.total_seconds()
            if unit == "s":
                outv = seconds
            elif unit == "m":
                outv = seconds / 60.0
            elif unit == "h":
                outv = seconds / 3600.0
            else:  # "d"
                outv = seconds / 86400.0
            users_tbl[m_name] = outv

        # ------------------------
        # time: time_to_nth_event(..., n=INT, unit=s|m|h|d)  (requires --exposure)
        # ------------------------
        elif m_type == "time" and m_rule == "time_to_nth_event":
            if not args.exposure:
                raise SystemExit(f"[Error] {m_name} time_to_nth_event(...) requires --exposure (needs exposure_time).")
            if "exposure_time" not in users_tbl.columns:
                raise SystemExit(f"[Error] {m_name} requires exposure_time column, but it was not created.")
            unit = str(m_kwargs.get("unit", "s")).strip().lower()
            if unit not in {"s", "m", "h", "d"}:
                raise SystemExit(f"[Error] {m_name} bad unit='{unit}'. Use s/m/h/d (example: unit=h)")
            n_raw = str(m_kwargs.get("n", "1")).strip()
            try:
                n = int(n_raw)
            except Exception:
                raise SystemExit(f"[Error] {m_name} time_to_nth_event(...) requires integer n (example: n=2). Got n={n_raw!r}")
            if n < 1:
                raise SystemExit(f"[Error] {m_name} time_to_nth_event(...) requires n>=1. Got n={n}")
            ev = df_scoped[df_scoped[args.event] == target][[args.user, args.time]].copy()
            if ev.empty:
                users_tbl[m_name] = pd.NA
            else:
                ev = ev.sort_values([args.user, args.time])
                ev["_k"] = ev.groupby(args.user).cumcount() + 1
                nth_tbl = (ev[ev["_k"] == n].drop_duplicates(subset=[args.user], keep="first")[[args.user, args.time]].rename(columns={args.user: "user_id", args.time: "_nth_time"}))
                tmp = users_tbl[["user_id", "exposure_time"]].merge(nth_tbl, on="user_id", how="left")
                delta = tmp["_nth_time"] - tmp["exposure_time"]  # Timedelta or NaT
                seconds = delta.dt.total_seconds()
                if unit == "s":
                    outv = seconds
                elif unit == "m":
                    outv = seconds / 60.0
                elif unit == "h":
                    outv = seconds / 3600.0
                else:
                    outv = seconds / 86400.0
                users_tbl[m_name] = outv


        else:
            raise SystemExit(
                f"Unsupported metric: {m_name}={m_type}:{m_rule}(...). Try:\n"
                "| binary:event_exists(event)\n"
                "| binary:event_count_ge(event, n=INT)\n"
                "| count:count_event(event)\n"
                "| count:unique_event_days(event)\n"
                "| continuous:sum_value(event)\n"
                "| continuous:mean_value(event)\n"
                "| continuous:median_value(event)\n"
                "| continuous:max_value(event)\n"
                "| continuous:last_value(event)\n"
                "| time:first_time(event)\n"
                "| time:last_time(event)\n"
                "| time:time_to_event(event, unit=s|m|h|d)   (requires --exposure)\n"
                "| time:time_to_nth_event(event, n=INT, unit=s|m|h|d) (requires --exposure)\n")

    #Unassigned variant handling
    v = users_tbl["variant"].astype("string")
    bad = v.isna() | (v.str.strip() == "")
    if bad.any():
        n_bad = int(bad.sum())

        if args.unassigned == "error":
            ex = users_tbl.loc[bad, ["user_id", "variant"]].head(30)
            raise SystemExit(f"[Stopped] Unassigned users: {n_bad}. Use --unassigned drop to remove them, or fix upstream assignment. Examples:\n{ex.to_string(index=False)}")

        if args.unassigned == "drop":
            users_tbl = users_tbl.loc[~bad].copy()
            print(f"[Unassigned] dropped: {n_bad}")

        elif args.unassigned == "keep":
            users_tbl.loc[bad, "variant"] = "unassigned"
            print(f"[Unassigned] kept: {n_bad} (set variant='unassigned')")


    print("=== Converted (events) ===")
    print(f"input:  {in_path}")
    print(f"output: {out_path if out_path is not None else '(preview only)'}")
    print(f"rows:   {len(users_tbl)}")
    print(f"cols:   {list(users_tbl.columns)}")
    print("\nhead(30):")
    print(users_tbl.head(30).to_string(index=False))

    if args.preview:
        return

    _write_df(users_tbl, out_path)

