import argparse
import pandas as pd
from pathlib import Path
import json


def add_convert_subcommand(subparsers: argparse._SubParsersAction) -> None:
    convert_parser = subparsers.add_parser( "convert", help="Convert data to canonical user-level format")
    convert_subparsers = convert_parser.add_subparsers(dest="convert_cmd", required=True)

    #ab convert unit
    unit_parser = convert_subparsers.add_parser("unit", help="| Manual conversion/standardization for unit-level data (one row per user)")
    unit_parser.add_argument("--data", metavar="PATH", default=None, help="| Path to CSV or Parquet file")
    unit_parser.add_argument("--user", metavar="COL", default=None, help="| User/unit id column name")
    unit_parser.add_argument("--variant", metavar="COL", default=None, help="| Treatment/variant column name")
    unit_parser.add_argument("--outcome", metavar="COL", default=None, help="| Outcome/metric column name")
    unit_parser.add_argument("--keep", metavar="COL,COL",default=None, help="| Comma-separated extra columns to keep (optional)")
    unit_parser.add_argument("--dedupe", choices=["error", "first", "last"], default=None, help="| What to do if multiple rows per user exist")
    unit_parser.add_argument("--out", metavar="PATH", help="| Output path (.csv or .parquet) (either --preview or --out)")
    unit_parser.add_argument("--preview", action="store_true", help="| Preview converted data without outputing (either --preview or --out)")
    unit_parser.add_argument("--save-config", metavar="PATH", default=None, help="| Write merged arguments to a JSON config file (optional, should end in .json)")
    unit_parser.add_argument("--config", metavar="PATH", default=None, help="| Load arguments from a JSON config file (optional, should end in .json)")
    unit_parser.set_defaults(func=_run_unit)

    #ab convert event
    events_parser = convert_subparsers.add_parser("events", help="Manual conversion for event-level data (one row per event)")
    events_parser.add_argument("--data", metavar = "PATH", default=None, help="| Path to CSV or Parquet file")
    events_parser.add_argument("--user", metavar = "COL", default=None, help="| User/unit id column name")
    events_parser.add_argument("--variant", metavar = "COL", default=None, help="| Treatment/variant column name")
    events_parser.add_argument("--time", metavar = "COL", default=None, help="| Event timestamp column name")
    events_parser.add_argument("--event", metavar = "COL", default=None, help="| Event type/category column name")
    events_parser.add_argument("--value", metavar = "COL", default=None, help="| Event numeric value column name (like purchase amount)")
    events_parser.add_argument("--exposure", metavar = "VALUE", default=None, help="| Value in Event type column identifying exposure. Users without exposure are dropped.")
    events_parser.add_argument("--multiexposure", choices=["error", "first", "last"], default=None, help="| A fallback if there are multiple exposure event per user (default: first)")
    events_parser.add_argument("--multivariant", choices=["error", "first", "last", "mode", "from_exposure"], default=None, help="| A fallback if there are multiple variants per user (default: error)")
    events_parser.add_argument("--window", metavar = "DURATION", default=None, help="| Outcome window after exposure (e.g., 7d, 24h). Only used when exposure-event is provided.")
    events_parser.add_argument("--metric", metavar="SPEC", action="append", default=None, help="| Metric spec (repeatable). Used for column computations. Define one output column per metric. ---Syntax: NAME=TYPE:RULE(EVENT[, key=value ...])")
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
    

def _parse_metric_call(call: str) -> tuple[str, dict]:
    call = call.strip()
    if not call:
        raise SystemExit("Bad --metric: empty parentheses ()")

    parts = [p.strip() for p in call.split(",") if p.strip()]
    event = parts[0].strip().lower()

    kwargs: dict[str, str] = {}
    for p in parts[1:]:
        if "=" not in p:
            raise SystemExit(f"Bad --metric arg '{p}'. Use key=value (example: unit=h)")
        k, v = p.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if not k or not v:
            raise SystemExit(f"Bad --metric arg '{p}'. Use key=value (example: unit=h)")
        kwargs[k] = v

    return event, kwargs


def _deconstruct_metric(spec: list[str]) -> dict:
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

        event, kwargs = _parse_metric_call(call)

        if not m_type or not rule or not event:
            raise SystemExit(f"Bad syntax in --metric: {sing_spec}")

        # store: [name, type, rule, event, kwargs]
        spec_dict[i] = [name, m_type, rule, event, kwargs]

    return spec_dict

    
################################################################################################################
################################################################################################################
def _run_unit(args: argparse.Namespace) -> None:
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
    required_args = ["data", "user", "variant", "outcome"]
    missing = [k for k in required_args if not getattr(args, k)]
    if missing:
        raise SystemExit(
            f"Missing required arguments: {missing}\n"
            f"Provide them on CLI or via --config."
        )

    #Save config if needed
    if args.save_config:
        _save_config(args, Path(args.save_config))
        print(f"[config] saved: {args.save_config}")

    #Load data
    in_path = Path(args.data)
    out_path = Path(args.out) if args.out else None
    df = _load_df(in_path)

    #Validate required DATAFRAME COLUMNS
    keep_cols = _parse_keep(args.keep)
    required_cols = [args.user, args.variant, args.outcome] + keep_cols
    _require_columns(df, required_cols)
    #Clean columns
    df[args.user] = df[args.user].astype("string").str.strip()
    df[args.variant] = df[args.variant].astype("string").str.strip().str.lower()
    df[args.outcome] = df[args.outcome].astype("string").str.strip().str.lower()

    #Select + rename into canonical schema
    out = df[required_cols].copy()
    out = out.rename(columns={args.user: "user_id", args.variant: "variant", args.outcome: "outcome"})

    #Handle duplicates (multiple rows per user)
    dup_mask = out["user_id"].duplicated(keep=False)
    if dup_mask.any():
        n_dup_rows = int(dup_mask.sum())
        n_dup_users = int(out.loc[dup_mask, "user_id"].nunique(dropna=True))

        if args.dedupe == "error":
            raise SystemExit(
                f"Duplicate users detected: {n_dup_users} users across {n_dup_rows} rows.\n"
                f"Use --dedupe first or --dedupe last if you want an automatic choice."
            )

        if args.dedupe == "first":
            out = out.sort_index().drop_duplicates(subset=["user_id"], keep="first")
        elif args.dedupe == "last":
            out = out.sort_index().drop_duplicates(subset=["user_id"], keep="last")

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
    #Load config first (so it can fill args.user/variant/outcome/etc)
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
    
    #Validate required ARGS
    required_args = ["data", "user", "variant", "time", "event"]
    missing = [k for k in required_args if not getattr(args, k)]
    if missing:
        raise SystemExit(
            f"Missing required arguments: {missing}\n"
            f"Provide them on CLI or via --config."
        )
    
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
    #Make values numeric
    if args.value:
        _require_columns(df, [args.value])
        s = df[args.value].astype("string").str.strip()
        s = s.str.replace(r"[^0-9\.\-]+", "", regex=True)
        df[args.value] = pd.to_numeric(s, errors="coerce")

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
    
    #Deconstruct atributes and compute metrics
    metrics = _deconstruct_metric(args.metric)
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
        # count: count_event(...)
        # ------------------------
        elif m_type == "count" and m_rule == "count_event":
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user).size().reset_index(name=m_name).rename(columns={args.user: "user_id"}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0).astype(int)

        # ------------------------
        # continuous: sum_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "sum_value":
            if not args.value:
                raise SystemExit(f"[Error] {m_name} requires --value for sum_value(...)")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.value].sum(min_count=1).reset_index().rename(columns={args.user: "user_id", args.value: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0.0)

        # ------------------------
        # continuous: mean_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "mean_value":
            if not args.value:
                raise SystemExit(f"[Error] {m_name} requires --value for mean_value(...)")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.value].mean().reset_index().rename(columns={args.user: "user_id", args.value: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0.0)

        # ------------------------
        # continuous: max_value(...)
        # ------------------------
        elif m_type == "continuous" and m_rule == "max_value":
            if not args.value:
                raise SystemExit(f"[Error] {m_name} requires --value for max_value(...)")
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.value].max().reset_index().rename(columns={args.user: "user_id", args.value: m_name}))
            users_tbl = users_tbl.merge(per_user, on="user_id", how="left")
            users_tbl[m_name] = users_tbl[m_name].fillna(0.0)

        # ------------------------
        # time: first_time(...)
        # ------------------------
        elif m_type == "time" and m_rule == "first_time":
            per_user = (df_scoped[df_scoped[args.event] == target].groupby(args.user)[args.time].min().reset_index().rename(columns={args.user: "user_id", args.time: m_name}))
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
            delta = tmp["_first_time"] - tmp["exposure_time"]  # Timedelta or NaT

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

        else:
            raise SystemExit(
                f"Unsupported metric: {m_name}={m_type}:{m_rule}(...). Supported now:\n"
                "| binary:event_exists(event)\n"
                "| count:count_event(event)\n"
                "| continuous:sum_value(event)\n"
                "| continuous:mean_value(event)\n"
                "| continuous:max_value(event)\n"
                "| time:first_time(event)\n"
                "| time:time_to_event(event, unit=s|m|h|d)   (requires --exposure)\n")


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

