import argparse
import pandas as pd
from pathlib import Path
import json
_FGUIDE_PATH = Path(__file__).with_name("FINDING_GUIDE.txt")


def add_doctor_subcommand(subparsers: argparse._SubParsersAction) -> None:
    doctor_parser = subparsers.add_parser( "doctor", help="| Validate converted datasets, check if ready to be analysed")
    doctor_parser.add_argument("--data", metavar="PATH", default=None, help="| Path to converted CSV or Parquet file")
    doctor_parser.add_argument("--user", metavar="COL", default=None, help="| User/unit id column name in converted data (default: user_id)")
    doctor_parser.add_argument("--variant", metavar="COL", default=None, help="| Treatment/variant column name in converted data (default: variant)")
    doctor_parser.add_argument("--metrics", metavar="COL,COL", default=None, help="| Comma-separated metric columns to check (default: numeric columns except user/variant)")
    doctor_parser.add_argument("--ignore", metavar="COL,COL", default=None, help="| Comma-separated columns to ignore (e.g., keep cols like device,country)")
    doctor_parser.add_argument("--allocation", metavar="SPEC", default=None, help="| Expected allocation: 'equal' or 'A=0.5,B=0.3,C=0.2' (optional)")
    doctor_parser.add_argument("--alpha", metavar="FLOAT", type=float, default=None, help="| Alpha for allocation/SRM-style checks (default: 0.01)")
    doctor_parser.add_argument("--min-n", metavar="N", type=int, default=None, help="| Warn if any variant has fewer than N users (optional)")
    doctor_parser.add_argument("--min-n-metric", metavar="N", type=int, default=None, help="| Warn if any metric has < N non-missing users in any variant (optional)")
    doctor_parser.add_argument("--only", choices=["errors", "warnings", "all"], default=None, help="| What to report: errors, warnings, or all (default: all)")
    doctor_parser.add_argument("--fail-on", choices=["error", "warn"], default=None, help="| Exit nonzero on errors only (default) or errors+warnings")
    doctor_parser.add_argument("--no-exit", action="store_true", help="| Always exit 0 (still prints report)")
    doctor_parser.add_argument("--preview", action="store_true", help="| Preview problem rows/examples")
    doctor_parser.add_argument("--report", metavar="PATH", default=None, help="| Write report to file (.md ot .json) (optional)")
    doctor_parser.add_argument("--check", metavar="NAME,NAME", default=None, help="| Comma-separated checks to run ---(e.g., integrity,variants,missingness,allocation,metrics,consistency)")
    doctor_parser.add_argument("--skip", metavar="NAME,NAME", default=None, help="| Comma-separated checks to skip")
    doctor_parser.add_argument("--save-config", metavar="PATH", default=None, help="| Write merged arguments to a JSON config file (optional, should end in .json)")
    doctor_parser.add_argument("--config", metavar="PATH", default=None, help="| Load arguments from a JSON config file (optional, should end in .json)")
    doctor_parser.set_defaults(func=_run_doctor)

#############################################################################################################################
#############################################################################################################################

def _load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    suf = path.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(path)
    if suf in (".parquet", ".pq"):
        return pd.read_parquet(path)

    raise SystemExit("Unsupported file type. Use .csv or .parquet")


def _fmt_pct(x: float, digits: int = 1) -> str:
    try:
        return f"{100.0 * float(x):.{digits}f}%"
    except Exception:
        return str(x)

def _fmt_pp(x: float, digits: int = 1) -> str:
    try:
        return f"{100.0 * float(x):.{digits}f} pp"
    except Exception:
        return str(x)


def _load_finding_guide() -> dict:
    if not _FGUIDE_PATH.exists():
        return {}
    try:
        txt = _FGUIDE_PATH.read_text(encoding="utf-8")
        return json.loads(txt)
    except Exception:
        return {}

_FINDING_GUIDE = _load_finding_guide()

def _explain_finding(code: str) -> dict | None:
    return _FINDING_GUIDE.get(code, None)

def _format_diagnostics(meta: dict | None) -> str:
    if meta is None or not isinstance(meta, dict) or not meta:
        return ""
    keys = ["metric", "min_n", "min_n_metric", "missing", "missing_rate", "worst_variant", "worst_rate", "best_variant", "best_rate", "gap_pp", "n_rows", "n_users",]
    lines = []
    for k in keys:
        if k not in meta:
            continue
        v = meta.get(k)

        if isinstance(v, float):
            if k == "gap_pp":
                v = _fmt_pp(v)
            elif "rate" in k and 0.0 <= v <= 1.0:
                v = _fmt_pct(v)
            else:
                v = f"{v:.4g}"

        lines.append(f"{k}={v}")

    if "note" in meta and meta["note"]:
        lines.append(f"note={meta['note']}")

    return "\n".join(lines)

def _format_explain_text(code: str, meta: dict | None = None) -> str:
    guide = _explain_finding(code)
    if not guide:
        return ""

    lines = []
    lines.append("What it means: " + guide.get("what", "").strip())
    lines.append("Why it matters: " + guide.get("why", "").strip())

    diag = _format_diagnostics(meta)
    if diag:
        lines.append("Diagnostics:")
        for ln in diag.splitlines():
            lines.append("  - " + ln)

    if meta and isinstance(meta, dict):
        if code == "METRIC_HIGH_MISSING":
            m = meta.get("metric")
            miss = meta.get("missing")
            rate = meta.get("missing_rate")
            if m is not None and miss is not None and rate is not None:
                lines.append(f"Example: '{m}' is missing for {miss} users ({_fmt_pct(rate)}).")

        if code == "METRIC_MISSING_IMBALANCE":
            m = meta.get("metric")
            wv = meta.get("worst_variant")
            wr = meta.get("worst_rate")
            bv = meta.get("best_variant")
            br = meta.get("best_rate")
            gp = meta.get("gap_pp")
            if m is not None and wv is not None and wr is not None and bv is not None and br is not None and gp is not None:
                lines.append(f"Example: '{m}' worst={wv}({_fmt_pct(wr)}), best={bv}({_fmt_pct(br)}), gap={_fmt_pp(gp)}.")

        if code == "VARIANT_TINY_ARM":
            mn = meta.get("min_n")
            note = meta.get("note")
            if mn is not None and note:
                lines.append(f"Example: {note} (min_n={mn}).")

        if code == "METRIC_TINY_ARM":
            mmn = meta.get("min_n_metric")
            note = meta.get("note")
            if mmn is not None and note:
                lines.append(f"Example: {note} (min_n_metric={mmn}).")

    likely = guide.get("likely", []) or []
    nxt = guide.get("next", []) or []

    if likely:
        lines.append("Likely causes:")
        for x in likely:
            lines.append("  - " + str(x))

    if nxt:
        lines.append("Next steps:")
        for x in nxt:
            lines.append("  - " + str(x))

    return "\n".join([l for l in lines if l.strip() != ""])


def _print_preview(report_items: list[dict], only: str = "all", max_example_rows: int = 10) -> None:
    only = (only or "all").lower().strip()
    sev_order = ["ERROR", "WARN", "INFO"]

    def keep(f):
        s = f.get("severity", "INFO")
        if only == "errors":
            return s == "ERROR"
        if only == "warnings":
            return s in ("ERROR", "WARN")
        return True

    items = [f for f in report_items if keep(f)]

    n_err = sum(1 for x in report_items if x.get("severity") == "ERROR")
    n_wrn = sum(1 for x in report_items if x.get("severity") == "WARN")
    n_inf = sum(1 for x in report_items if x.get("severity") == "INFO")

    print("\n=== ab doctor ===")
    print(f"Summary: {n_err} errors, {n_wrn} warnings, {n_inf} info")
    if only != "all":
        print(f"(showing: {only})")
    print("")

    for sev in sev_order:
        group = [f for f in items if f.get("severity") == sev]
        if not group:
            continue

        print(f"{sev}:")
        for f in group:
            code = f.get("code", "")
            msg = f.get("message", "")
            cnt = f.get("count", 0)
            meta = f.get("meta", None)

            print(f"  - [{code}] (count={cnt}) {msg}")

            explain = _format_explain_text(code, meta)
            if explain:
                for ln in explain.splitlines():
                    print("      " + ln)

            ex = f.get("examples")
            if ex:
                keys = list(ex[0].keys())
                keys = keys[:8]

                print("      examples:")
                print("      " + " | ".join(keys))
                print("      " + " | ".join(["---"] * len(keys)))

                for r in ex[:max_example_rows]:
                    row = []
                    for k in keys:
                        v = r.get(k, "")
                        if v is None:
                            v = ""
                        s = str(v).replace("\n", " ")
                        if len(s) > 40:
                            s = s[:37] + "..."
                        row.append(s)
                    print("      " + " | ".join(row))
        print("")


def _write_report(report_items: list[dict], finding: dict, max_rows: int = 30) -> None:
    if report_items is None:
        raise SystemExit("Internal error: report_items is None")
    if not isinstance(finding, dict):
        raise SystemExit("Internal error: finding must be a dict")

    severity = str(finding.get("severity", "")).upper().strip()
    if severity not in {"ERROR", "WARN", "INFO"}:
        raise SystemExit(f"Invalid finding severity: {severity}")

    code = str(finding.get("code", "")).strip()
    if not code:
        raise SystemExit("Finding is missing 'code'")

    message = str(finding.get("message", "")).strip()
    if not message:
        raise SystemExit(f"Finding '{code}' is missing 'message'")

    count = finding.get("count", 0)
    try:
        count = int(count)
    except Exception:
        raise SystemExit(f"Finding '{code}' has non-integer count: {count}")

    meta = finding.get("meta", None)
    if meta is not None and not isinstance(meta, dict):
        raise SystemExit(f"Finding '{code}' meta must be a dict if provided")

    examples_df = finding.get("examples_df", None)
    examples = None
    if examples_df is not None:
        if not isinstance(examples_df, pd.DataFrame):
            raise SystemExit(f"Finding '{code}' examples_df is not a DataFrame")
        ex = examples_df.head(max_rows).copy()
        examples = ex.to_dict(orient="records")

    report_items.append({
        "severity": severity,
        "code": code,
        "message": message,
        "count": count,
        "meta": meta,
        "examples": examples,
    })

def _save_report(report_items: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    suf = out_path.suffix.lower()

    if suf == ".json":
        payload = {"findings": report_items}
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return

    if suf == ".md":
        order = {"ERROR": 0, "WARN": 1, "INFO": 2}
        items = sorted(report_items, key=lambda x: order.get(x["severity"], 99))

        n_err = sum(1 for x in items if x["severity"] == "ERROR")
        n_wrn = sum(1 for x in items if x["severity"] == "WARN")
        n_inf = sum(1 for x in items if x["severity"] == "INFO")

        if n_err > 0:
            readiness = "❌ Not ready for analysis"
            readiness_note = "Fix errors first. These can break or bias analysis."
        elif n_wrn > 0:
            readiness = "⚠️ Mostly ready (proceed with caution)"
            readiness_note = "Warnings may bias results or reduce reliability; investigate before final conclusions."
        else:
            readiness = "✅ Ready for analysis"
            readiness_note = "No major issues detected."

        lines = []
        lines.append('<div align="center">\n')
        lines.append("# ab doctor report\n")
        lines.append(f"**{readiness}**\n")
        lines.append(f"\n{readiness_note}\n")
        lines.append(f"\n**Summary:** {n_err} errors, {n_wrn} warnings, {n_inf} info.\n")
        lines.append("\n---\n")
        lines.append("</div>\n")

        def md_escape(val) -> str:
            s = str(val)
            return s.replace("|", "\\|").replace("\n", " ")

        def records_to_table(records: list[dict], max_cols: int = 8, max_rows: int = 10) -> str:
            if not records:
                return ""

            cols = list(records[0].keys())[:max_cols]

            def cell(v):
                if v is None:
                    return ""
                if isinstance(v, float):
                    if 0.0 <= v <= 1.0:
                        return _fmt_pct(v)
                    return f"{v:.3g}"
                s = str(v).replace("\n", " ")
                if len(s) > 60:
                    s = s[:57] + "..."
                return md_escape(s)

            out = []
            out.append("| " + " | ".join(cols) + " |")
            out.append("| " + " | ".join(["---"] * len(cols)) + " |")
            for r in records[:max_rows]:
                row = [cell(r.get(c, "")) for c in cols]
                out.append("| " + " | ".join(row) + " |")
            return "\n".join(out)

        def _guide_text(code: str) -> str:
            g = _FINDING_GUIDE.get(code, None)
            if not g:
                return ""

            title = str(g.get("title", "")).strip()
            what = str(g.get("what", "")).strip()
            why = str(g.get("why", "")).strip()
            fix = str(g.get("fix", "")).strip()

            out = []
            if title:
                out.append(f"{title}")
            if what:
                out.append(f"• What it means: {what}")
            if why:
                out.append(f"• Why it matters: {why}")
            if fix:
                out.append(f"• How to fix: {fix}")

            return "\n".join(out).strip()

        current = None
        for f in items:
            if f["severity"] != current:
                current = f["severity"]
                lines.append(f"\n## {current}\n")

            lines.append(f"### {f['code']}\n")
            lines.append(f"- **Count:** {f['count']}\n")

            lines.append("\n> **Finding**\n")
            for ln in str(f["message"]).splitlines():
                lines.append("> " + ln)
            lines.append("")

            guide = _guide_text(f["code"])
            explain = _format_explain_text(f["code"], f.get("meta", None))

            if guide or explain:
                lines.append("\n> **Explanation & next steps**\n")

                if guide:
                    for ln in guide.splitlines():
                        lines.append("> " + ln)
                    lines.append("")

                if explain:
                    for ln in explain.splitlines():
                        lines.append("> " + ln)
                    lines.append("")

            if f.get("examples"):
                lines.append("\n<details>\n<summary><b>Examples</b> (click to expand)</summary>\n\n")
                lines.append(records_to_table(f["examples"]))
                lines.append("\n\n</details>\n")

        out_path.write_text("\n".join(lines), encoding="utf-8")
        return

    raise SystemExit("Unsupported report type. Use .json or .md")


def _save_config(args: argparse.Namespace, path: Path) -> None:
    cfg = vars(args).copy()
    #Remove argparse internals
    cfg.pop("func", None)
    cfg.pop("save_config", None)
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

def _require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}\nAvailable columns: {list(df.columns)}")
    
#--------------------- TESTS ---------------------
def _integrity(df: pd.DataFrame, user: str, variant:str, report_items: list[dict], max_rows: int = 30) -> None:
    #Null/empty user
    u = df[user]
    u_str = u.astype("string")
    bad_user = u_str.isna() | (u_str.str.strip() == "")
    if bad_user.any():
        _write_report(report_items, {
            "severity": "ERROR",
            "code": "INTEGRITY_BAD_USER",
            "message": f"Found missing/empty values in '{user}'. Canonical data must have a valid user_id per row.",
            "count": int(bad_user.sum()),
            "meta": {"n_rows": int(len(df)), "n_users": int(df[user].nunique(dropna=True))},
            "examples_df": df.loc[bad_user, [user, variant]].copy(),
        }, max_rows=max_rows)

    #Null/empty variant
    v = df[variant]
    v_str = v.astype("string")
    bad_var = v_str.isna() | (v_str.str.strip() == "")
    if bad_var.any():
        _write_report(report_items, {
            "severity": "ERROR",
            "code": "INTEGRITY_BAD_VARIANT",
            "message": f"Found missing/empty values in '{variant}'. Canonical data must have a variant per row.",
            "count": int(bad_var.sum()),
            "meta": {"n_rows": int(len(df)), "n_users": int(df[user].nunique(dropna=True))},
            "examples_df": df.loc[bad_var, [user, variant]].copy(),
        }, max_rows=max_rows)

    #Duplicate user_id rows
    dup_mask = df.duplicated(subset=[user], keep=False)
    if dup_mask.any():
        dup_users = int(df.loc[dup_mask, user].nunique(dropna=True))
        dup_rows = int(dup_mask.sum())
        _write_report(report_items, {
            "severity": "ERROR",
            "code": "INTEGRITY_DUP_USER",
            "message": f"Duplicate '{user}' rows found. Canonical data must have exactly 1 row per user. (dup_users={dup_users}, dup_rows={dup_rows})",
            "count": dup_rows,
            "meta": {"n_rows": int(len(df)), "n_users": int(df[user].nunique(dropna=True)), "dup_users": dup_users, "dup_rows": dup_rows},
            "examples_df": df.loc[dup_mask, [user, variant]].sort_values(user).copy(),
        }, max_rows=max_rows)

    #Total rows/users
    n_rows = len(df)
    n_users = df[user].nunique(dropna=True)
    _write_report(report_items, {
        "severity": "INFO",
        "code": "INTEGRITY_SUMMARY",
        "message": f"Rows={n_rows}, unique_users={n_users}.",
        "count": n_rows,
        "meta": {"n_rows": int(n_rows), "n_users": int(n_users)},
        "examples_df": None,
    }, max_rows=max_rows)
#---------------------
def _variant_check(df: pd.DataFrame, user: str, variant:str, min_n: int, report_items: list[dict], max_rows: int = 30) -> None:
    tmp = df[variant].astype("string").str.strip()
    counts = tmp.value_counts(dropna=False).reset_index()
    counts.columns = [variant, "n_users"]
    total = float(counts["n_users"].sum()) if len(counts) else 0.0
    counts["pct"] = counts["n_users"].apply(lambda x: (float(x) / total) if total else 0.0)

    #Info: variant counts
    _write_report(report_items, {
        "severity": "INFO",
        "code": "VARIANT_COUNTS",
        "message": "Variant counts (users per variant).",
        "count": int(len(counts)),
        "meta": {"n_users": int(df[user].nunique(dropna=True))},
        "examples_df": counts,
    }, max_rows=max_rows)

    #Warn if only one variant
    n_var = df[variant].astype("string").str.strip().nunique(dropna=True)
    if n_var <= 1:
        _write_report(report_items, {
            "severity": "WARN",
            "code": "VARIANT_SINGLE_ARM",
            "message": "Only one non-null variant detected. This may not be a real experiment (A/B/n).",
            "count": int(n_var),
            "meta": {"n_users": int(df[user].nunique(dropna=True))},
            "examples_df": counts,
        }, max_rows=max_rows)

    #Tiny arms check
    if min_n is not None and min_n > 0:
        tiny = counts[counts["n_users"] < min_n]
        if not tiny.empty:
            note = None
            try:
                r = tiny.iloc[0]
                note = f"{r[variant]} has {int(r['n_users'])} users"
            except Exception:
                note = None

            _write_report(report_items, {
                "severity": "WARN",
                "code": "VARIANT_TINY_ARM",
                "message": f"Some variants have fewer than min_n={min_n} users. Results may be underpowered/unstable.",
                "count": int(len(tiny)),
                "meta": {"min_n": int(min_n), "note": note},
                "examples_df": tiny,
            }, max_rows=max_rows)
#---------------------
def _missingness(df: pd.DataFrame, user: str, variant: str, metrics: list[str], report_items: list[dict], max_rows: int = 30, high_missing_warn: float = 0.95, gap_warn: float = 0.20) -> None:
    n_total = int(len(df))
    rows = []
    highlight_lines = []

    for m in metrics:
        s = df[m]
        missing_total = int(s.isna().sum())
        overall_rate = float(missing_total / n_total) if n_total > 0 else 0.0

        g = df.groupby(variant, dropna=False)[m]
        per = g.agg(users="size", missing=lambda x: int(x.isna().sum())).reset_index()
        per["missing_pct"] = per.apply(lambda r: (float(r["missing"] / r["users"]) if r["users"] else 0.0),axis=1)

        if len(per) > 0:
            worst_idx = int(per["missing_pct"].idxmax())
            best_idx = int(per["missing_pct"].idxmin())
            worst_v = per.loc[worst_idx, variant]
            best_v = per.loc[best_idx, variant]
            worst_rate = float(per.loc[worst_idx, "missing_pct"])
            best_rate = float(per.loc[best_idx, "missing_pct"])
            gap = float(worst_rate - best_rate)
        else:
            worst_v = best_v = ""
            worst_rate = best_rate = 0.0
            gap = 0.0

        rows.append({
            "metric": m,
            "overall_missing": f"{missing_total}/{n_total} ({_fmt_pct(overall_rate)})",
            "worst_variant": f"{worst_v} ({_fmt_pct(worst_rate)})",
            "best_variant": f"{best_v} ({_fmt_pct(best_rate)})",
            "gap_pp": _fmt_pp(gap),
            "overall_missing_rate": overall_rate,
            "gap_rate": gap,
        })

        if overall_rate >= high_missing_warn or gap >= gap_warn or overall_rate >= 0.80:
            highlight_lines.append(
                f"- **{m}**: overall {missing_total}/{n_total} ({_fmt_pct(overall_rate)}), "
                f"worst {worst_v} {_fmt_pct(worst_rate)}, best {best_v} {_fmt_pct(best_rate)}, gap {_fmt_pp(gap)}"
            )

        if overall_rate >= 0.999999:
            _write_report(report_items, {
                "severity": "ERROR",
                "code": "METRIC_ALL_MISSING",
                "message": (
                    f"Metric '{m}' is entirely missing: {missing_total}/{n_total} ({_fmt_pct(overall_rate)}). "
                    "It is not usable for analysis."
                ),
                "count": missing_total,
                "meta": {"metric": m, "missing": missing_total, "missing_rate": overall_rate},
                "examples_df": df.loc[s.isna(), [user, variant, m]].copy(),
            }, max_rows=max_rows)

        elif overall_rate >= high_missing_warn:
            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_HIGH_MISSING",
                "message": (
                    f"Metric '{m}' has high missingness: {missing_total}/{n_total} ({_fmt_pct(overall_rate)})."
                ),
                "count": missing_total,
                "meta": {"metric": m, "missing": missing_total, "missing_rate": overall_rate},
                "examples_df": per[[variant, "users", "missing", "missing_pct"]].copy(),
            }, max_rows=max_rows)

        if gap >= gap_warn:
            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_MISSING_IMBALANCE",
                "message": (
                    f"Metric '{m}' missingness differs across variants. "
                    f"Worst {worst_v} {_fmt_pct(worst_rate)}, best {best_v} {_fmt_pct(best_rate)}; gap {_fmt_pp(gap)}."
                ),
                "count": int(len(per)),
                "meta": {
                    "metric": m,
                    "worst_variant": worst_v,
                    "worst_rate": worst_rate,
                    "best_variant": best_v,
                    "best_rate": best_rate,
                    "gap_pp": gap,
                },
                "examples_df": per[[variant, "users", "missing", "missing_pct"]].copy(),
            }, max_rows=max_rows)

    summary = pd.DataFrame(rows).sort_values(["overall_missing_rate", "gap_rate"], ascending=False)

    if highlight_lines:
        _write_report(report_items, {
            "severity": "INFO",
            "code": "MISSINGNESS_HIGHLIGHTS",
            "message": "Notable missingness issues:\n" + "\n".join(highlight_lines[:8]),
            "count": int(len(highlight_lines)),
            "meta": None,
            "examples_df": None,
        }, max_rows=max_rows)

    _write_report(report_items, {
        "severity": "INFO",
        "code": "MISSINGNESS_SUMMARY",
        "message": "Missingness summary across metrics.",
        "count": int(len(summary)),
        "meta": None,
        "examples_df": summary.drop(columns=["overall_missing_rate", "gap_rate"]),
    }, max_rows=max_rows)
#---------------------
def _metrics_check(df: pd.DataFrame, user: str, variant: str, metrics: list[str], preview: bool, report_items: list[dict], max_rows: int = 30) -> None:
    rows = []
    for m in metrics:
        s0 = df[m]
        #Try numeric view
        s_num = pd.to_numeric(s0, errors="coerce")
        n_total = int(len(df))
        n_missing = int(s0.isna().sum())
        n_nonmissing = int(n_total - n_missing)

        #How many non-missing become NaN after numeric coercion
        bad_cast = int(((~s0.isna()) & (s_num.isna())).sum())
        bad_cast_rate = (bad_cast / n_nonmissing) if n_nonmissing > 0 else 0.0

        #Non-finite (inf / -inf)
        nonfinite = int(pd.Series(s_num).isin([float("inf"), float("-inf")]).sum())

        #Constant metric
        is_constant = False
        if s_num.notna().sum() >= 2:
            is_constant = (int(s_num.dropna().nunique()) <= 1)

        #Binary-ish check
        uniq = s_num.dropna().unique()
        looks_binary = (len(uniq) > 0 and len(uniq) <= 3 and set([float(x) for x in uniq if pd.notna(x)]).issubset({0.0, 1.0}))
        bad_binary = 0
        if looks_binary:
            bad_binary = int((s_num.notna() & ~s_num.isin([0, 1])).sum())

        rows.append({
            "metric": m,
            "dtype": str(s0.dtype),
            "missing": n_missing,
            "bad_numeric_cast": bad_cast,
            "bad_cast_rate": round(bad_cast_rate, 4),
            "nonfinite": nonfinite,
            "constant": int(is_constant),
        })

        #WARN: metric is object/string
        if str(s0.dtype) in {"object", "string"}:
            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_NON_NUMERIC_DTYPE",
                "message": f"Metric '{m}' is dtype={s0.dtype}. For analysis, metrics should usually be numeric (or datetime for time metrics).",
                "count": n_total,
                "examples_df": df[[user, variant, m]].head(30).copy() if preview else None,
            }, max_rows=max_rows)

        #WARN: too many values fail numeric casting
        if bad_cast_rate >= 0.20 and n_nonmissing > 0:
            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_BAD_NUMERIC_CAST",
                "message": f"Metric '{m}' has many non-numeric values (failed cast rate={bad_cast_rate:.1%}). This often means the conversion produced strings like '$12' or 'N/A'.",
                "count": bad_cast,
                "examples_df": df.loc[(~s0.isna()) & (s_num.isna()), [user, variant, m]].copy(),
            }, max_rows=max_rows)

        #ERROR: non-finite numbers
        if nonfinite > 0:
            _write_report(report_items, {
                "severity": "ERROR",
                "code": "METRIC_NONFINITE",
                "message": f"Metric '{m}' contains inf/-inf values. That breaks most analysis.",
                "count": nonfinite,
                "examples_df": df.loc[pd.Series(s_num).isin([float("inf"), float("-inf")]).values, [user, variant, m]].copy(),
            }, max_rows=max_rows)

        #WARN: constant metric
        if is_constant and s_num.notna().sum() > 0:
            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_CONSTANT",
                "message": f"Metric '{m}' is constant (no variation). It will not show treatment effects.",
                "count": int(s_num.notna().sum()),
                "examples_df": df[[user, variant, m]].head(30).copy() if preview else None,
            }, max_rows=max_rows)

        #ERROR: binary-ish metric contains values other than 0/1
        if looks_binary and bad_binary > 0:
            _write_report(report_items, {
                "severity": "ERROR",
                "code": "METRIC_BAD_BINARY_VALUES",
                "message": f"Metric '{m}' looks binary but contains values other than 0/1.",
                "count": bad_binary,
                "examples_df": df.loc[s_num.notna() & ~s_num.isin([0, 1]), [user, variant, m]].copy(),
            }, max_rows=max_rows)

    summary = pd.DataFrame(rows).sort_values(["bad_cast_rate", "missing"], ascending=False)
    _write_report(report_items, {
        "severity": "INFO",
        "code": "METRICS_SUMMARY",
        "message": "Metrics dtype/cast/quality summary (helps quickly spot broken columns).",
        "count": int(len(summary)),
        "examples_df": summary,
    }, max_rows=max_rows)
#---------------------
def _consistency(df: pd.DataFrame, user: str, variant: str, report_items: list[dict], max_rows: int = 30) -> None:
    #Variant cleanup check
    v0 = df[variant].astype("string")
    v_clean = v0.str.strip().str.lower()

    changed = (v0.notna() & (v0 != v_clean))
    if changed.any():
        _write_report(report_items, {
            "severity": "WARN",
            "code": "VARIANT_NEEDS_CLEANING",
            "message": f"Variant column '{variant}' has whitespace/case inconsistencies. It should be normalized (strip + lower).",
            "count": int(changed.sum()),
            "examples_df": df.loc[changed, [user, variant]].copy(),
        }, max_rows=max_rows)

    #Suspicious variant values
    bad_vals = {"none", "null", "nan", "n/a", "na", "undefined", "?"}
    bad_var = v_clean.isin(list(bad_vals))
    if bad_var.any():
        _write_report(report_items, {
            "severity": "WARN",
            "code": "VARIANT_SUSPICIOUS_VALUES",
            "message": f"Variant column '{variant}' contains placeholder-like values (none/null/nan/etc). Usually means assignment failed upstream.",
            "count": int(bad_var.sum()),
            "examples_df": df.loc[bad_var, [user, variant]].copy(),
        }, max_rows=max_rows)

    #User id cleanup check
    u0 = df[user].astype("string")
    u_clean = u0.str.strip()

    bad_user = u0.isna() | (u_clean == "")
    if bad_user.any():
        _write_report(report_items, {
            "severity": "ERROR",
            "code": "USER_BAD_VALUES",
            "message": f"User column '{user}' contains missing/empty values. Canonical data must have user_id for every row.",
            "count": int(bad_user.sum()),
            "examples_df": df.loc[bad_user, [user, variant]].copy(),
        }, max_rows=max_rows)

    changed_user = (u0.notna() & (u0 != u_clean))
    if changed_user.any():
        _write_report(report_items, {
            "severity": "WARN",
            "code": "USER_NEEDS_CLEANING",
            "message": f"User column '{user}' has whitespace issues. Consider strip() in convert, otherwise joins/groupbys can break.",
            "count": int(changed_user.sum()),
            "examples_df": df.loc[changed_user, [user, variant]].copy(),
        }, max_rows=max_rows)
#---------------------
def _distribution(df: pd.DataFrame, user: str, variant: str, metrics: list[str], report_items: list[dict], max_rows: int = 30, outlier_warn_rate: float = 0.01) -> None:
    #Per-variant summary for each metric (numeric only)
    summaries = []
    for m in metrics:
        s_num = pd.to_numeric(df[m], errors="coerce")

        #Skip if fully non-numeric
        if s_num.notna().sum() == 0:
            continue

        #Variant-level stats
        g = df[[variant]].copy()
        g["_x"] = s_num

        stats = g.groupby(variant, dropna=False)["_x"].agg(["count", "mean", "std", "min"]).reset_index()
        #Percentiles separately
        p50 = g.groupby(variant, dropna=False)["_x"].quantile(0.50).reset_index(name="p50")
        p90 = g.groupby(variant, dropna=False)["_x"].quantile(0.90).reset_index(name="p90")
        mx = g.groupby(variant, dropna=False)["_x"].max().reset_index(name="max")

        stats = stats.merge(p50, on=variant, how="left").merge(p90, on=variant, how="left").merge(mx, on=variant, how="left")
        stats.insert(0, "metric", m)
        summaries.append(stats)

        #Outliers (global IQR)
        x = s_num.dropna()
        if len(x) >= 20:
            q1 = float(x.quantile(0.25))
            q3 = float(x.quantile(0.75))
            iqr = q3 - q1
            if iqr > 0:
                lo = q1 - 1.5 * iqr
                hi = q3 + 1.5 * iqr
                out_mask = s_num.notna() & ((s_num < lo) | (s_num > hi))
                n_out = int(out_mask.sum())
                rate = n_out / int(s_num.notna().sum()) if int(s_num.notna().sum()) > 0 else 0.0

                if rate >= outlier_warn_rate and n_out > 0:
                    ex = df.loc[out_mask, [user, variant, m]].copy()
                    ex["_abs"] = pd.to_numeric(ex[m], errors="coerce").abs()
                    ex = ex.sort_values("_abs", ascending=False).drop(columns=["_abs"])

                    _write_report(report_items, {
                        "severity": "WARN",
                        "code": "METRIC_OUTLIERS",
                        "message": f"Metric '{m}' has potential outliers (IQR rule). Outlier rate={rate:.1%}. Consider winsorizing/log transform or check value parsing.",
                        "count": n_out,
                        "examples_df": ex,
                    }, max_rows=max_rows)

    if summaries:
        out = pd.concat(summaries, ignore_index=True)
        _write_report(report_items, {
            "severity": "INFO",
            "code": "DISTRIBUTION_SUMMARY",
            "message": "Per-variant numeric summary (count/mean/std/min/p50/p90/max). Helps you spot obviously broken scales.",
            "count": int(len(out)),
            "examples_df": out,
        }, max_rows=max_rows)
    else:
        _write_report(report_items, {
            "severity": "INFO",
            "code": "DISTRIBUTION_SUMMARY",
            "message": "No numeric metrics detected for distribution summary.",
            "count": 0,
            "examples_df": None,
        }, max_rows=max_rows)

#---------------------
def _metric_arm_n_check(df: pd.DataFrame, user: str, variant: str, metrics: list[str], min_n_metric: int, report_items: list[dict], max_rows: int = 30) -> None:
    if min_n_metric is None or min_n_metric <= 0:
        return

    rows = []
    for m in metrics:
        tmp = df[[variant, m]].copy()
        tmp["_ok"] = tmp[m].notna().astype(int)
        per = tmp.groupby(variant, dropna=False)["_ok"].sum().reset_index()
        per.columns = [variant, "n_nonmissing"]
        per.insert(0, "metric", m)

        if len(per) == 0:
            continue

        worst_idx = int(per["n_nonmissing"].idxmin())
        worst_v = per.loc[worst_idx, variant]
        worst_n = int(per.loc[worst_idx, "n_nonmissing"])

        if worst_n < min_n_metric:
            note = f"{m}: {worst_v} has {worst_n} non-missing users"
            rows.append({"metric": m, "worst_variant": worst_v, "worst_n_nonmissing": worst_n, "min_n_metric": int(min_n_metric)})

            _write_report(report_items, {
                "severity": "WARN",
                "code": "METRIC_TINY_ARM",
                "message": f"Metric '{m}' has too few non-missing users in some variants (min_n_metric={min_n_metric}). This can make results unstable or impossible to compute.",
                "count": int(len(per)),
                "meta": {"metric": m, "min_n_metric": int(min_n_metric), "note": note},
                "examples_df": per,
            }, max_rows=max_rows)

    if rows:
        out = pd.DataFrame(rows).sort_values(["worst_n_nonmissing"])
        _write_report(report_items, {
            "severity": "INFO",
            "code": "METRIC_TINY_ARM_SUMMARY",
            "message": "Metrics with small per-variant non-missing sample sizes.",
            "count": int(len(out)),
            "meta": {"min_n_metric": int(min_n_metric)},
            "examples_df": out,
        }, max_rows=max_rows)

#---------------------
def _allocation_check(df: pd.DataFrame, user: str, variant: str, allocation: str, alpha: float, report_items: list[dict], max_rows: int = 30) -> None:
    if allocation is None:
        return

    v = df[variant].astype("string").str.strip().str.lower()
    v = v.mask(v.isna() | (v == ""), pd.NA).dropna()
    if v.empty:
        return

    counts = v.value_counts(dropna=False).reset_index()
    counts.columns = [variant, "n_users"]

    #Parse expected allocation
    alloc = allocation.strip().lower()
    variants = [str(x) for x in counts[variant].tolist()]

    exp = {}
    if alloc == "equal":
        real_vars = [x for x in variants if x is not None and str(x).strip() != "" and str(x).lower() != "nan"]
        if not real_vars:
            return
        p = 1.0 / len(real_vars)
        for k in real_vars:
            exp[k] = p
    else:
        #Format: A=0.5,B=0.3,C=0.2
        parts = [p.strip() for p in alloc.split(",") if p.strip()]
        for p0 in parts:
            if "=" not in p0:
                raise SystemExit(f"Bad --allocation '{allocation}'. Use 'equal' or 'A=0.5,B=0.3'.")
            k, val = p0.split("=", 1)
            k = k.strip().lower()
            try:
                val = float(val.strip())
            except Exception:
                raise SystemExit(f"Bad --allocation value in '{p0}'. Use floats like 0.5.")
            exp[k] = val

        s = sum(exp.values())
        if s <= 0:
            raise SystemExit(f"Bad --allocation '{allocation}'. Sum of weights must be > 0.")
        #Normalize if user gave weights that don't sum to 1
        if abs(s - 1.0) > 1e-6:
            exp = {k: v / s for k, v in exp.items()}

    #Build observed/expected vectors for chi-square
    obs = {}
    for _, r in counts.iterrows():
        obs[str(r[variant]).strip().lower()] = int(r["n_users"])

    #Only compare variants that exist in expected (common SRM behavior)
    keys = [k for k in exp.keys() if k in obs]
    if not keys:
        _write_report(report_items, {
            "severity": "WARN",
            "code": "ALLOCATION_NO_OVERLAP",
            "message": f"--allocation was provided, but none of its variant names match the data variants. Data has: {sorted(list(obs.keys()))}",
            "count": int(len(obs)),
            "examples_df": counts,
        }, max_rows=max_rows)
        return

    n = sum(obs[k] for k in keys)
    chisq = 0.0
    for k in keys:
        e = exp[k] * n
        if e <= 0:
            continue
        chisq += ((obs[k] - e) ** 2) / e
    dfree = max(1, len(keys) - 1)
    pval = None
    try:
        from scipy.stats import chi2
        pval = float(chi2.sf(chisq, dfree))
    except Exception:
        pval = None

    #Always report INFO summary
    msg = f"Allocation check vs '{allocation}': chi2={chisq:.3f}, df={dfree}"
    if pval is not None:
        msg += f", p={pval:.6g}, alpha={alpha}"
    else:
        msg += " (p-value unavailable: scipy not installed)"

    _write_report(report_items, {
        "severity": "INFO",
        "code": "ALLOCATION_SUMMARY",
        "message": msg,
        "count": int(n),
        "examples_df": counts,
    }, max_rows=max_rows)

    #Flag if significant
    if pval is not None and pval < alpha:
        _write_report(report_items, {
            "severity": "ERROR",
            "code": "ALLOCATION_SRM_FAIL",
            "message": f"Variant split deviates from expected allocation (p={pval:.6g} < alpha={alpha}). This looks like SRM / assignment bias.",
            "count": int(n),
            "examples_df": counts,
        }, max_rows=max_rows)

#############################################################################################################################
#############################################################################################################################
def _run_doctor(args: argparse.Namespace) -> None:
    #Load config first (so it can fill args.user/variant/etc)
    if args.config:
        print("Reading specified config file......")
        cfg = _load_config(Path(args.config))
        for key, val in cfg.items():
            if hasattr(args, key) and getattr(args, key) is None:
                setattr(args, key, val)

    #Required defaults
    if args.user is None:
        args.user = "user_id"
    if args.variant is None:
        args.variant = "variant"
    if args.report is None and not args.preview:
        args.preview = True

    
    #Validate required ARGS
    required_args = ["data", "user", "variant"]
    missing = [k for k in required_args if not getattr(args, k)]
    if missing:
        raise SystemExit(
            f"Missing required arguments: {missing}. Provide them on CLI or via --config.")
    
    in_path = Path(args.data)
    df = _load_df(in_path)
    df.columns = df.columns.str.strip()
    out_path = Path(args.report) if args.report is not None else None
    
    #Secondary defaults
    if args.alpha is None:
        args.alpha = 0.01
    if args.only is None:
        args.only = "all"
    if args.fail_on is None:
        args.fail_on = "error"
    if args.check is None:
        args.check = "integrity,variants,missingness,metrics,consistency,distribution,metric_arm_n,allocation"

    #Default metrics: numeric columns only (so segments like country/device don't spam)
    if args.metrics is None:
        cand = [c for c in df.columns if c not in [args.user, args.variant]]
        args.metrics = [c for c in cand if pd.api.types.is_numeric_dtype(df[c])]

    if args.save_config:
        _save_config(args, Path(args.save_config))
        print(f"[config] saved: {args.save_config}")

    #Dtype formating
    if isinstance(args.metrics, str):
        args.metrics = [c.strip() for c in args.metrics.split(",") if c.strip()]

    if args.ignore:
        ignore_list = [c.strip() for c in args.ignore.split(",") if c.strip()]
        ignore_set = set(ignore_list)
        args.metrics = [m for m in args.metrics if m not in ignore_set]

    
    #Check if columns exist
    required_cols = [args.user, args.variant] + list(args.metrics)
    _require_columns(df, required_cols)

    #Tests that will be conducted
    what_to_check = [c.strip() for c in args.check.split(",") if c.strip()]
    if args.skip:
        what_to_skip = {c.strip() for c in args.skip.split(",") if c.strip()}
        what_to_skip = set(what_to_skip)
        what_to_check = [item for item in what_to_check if item not in what_to_skip]

    #Report structure
    report_items: list[dict] = []
    #Run Tests
    if "integrity" in what_to_check:
        _integrity(df, args.user, args.variant, report_items, max_rows=30)

    if "variants" in what_to_check:
        _variant_check(df, args.user, args.variant, args.min_n, report_items, max_rows=30)

    if "missingness" in what_to_check:
        _missingness(df, args.user, args.variant, args.metrics, report_items, max_rows=30)

    if "metrics" in what_to_check:
        _metrics_check(df, args.user, args.variant, args.metrics, args.preview, report_items, max_rows=30)

    if "consistency" in what_to_check:
        _consistency(df, args.user, args.variant, report_items, max_rows=30)

    if "distribution" in what_to_check:
        _distribution(df, args.user, args.variant, args.metrics, report_items, max_rows=30)

    if "metric_arm_n" in what_to_check:
        _metric_arm_n_check(df, args.user, args.variant, args.metrics, args.min_n_metric, report_items, max_rows=30)

    if "allocation" in what_to_check:
        _allocation_check(df, args.user, args.variant, args.allocation, args.alpha, report_items, max_rows=30)


    #Saving and visualizing
    if out_path is not None:
        _save_report(report_items, out_path)
        print(f"Report saved in {out_path}")
    if args.preview:
        _print_preview(report_items, only=args.only, max_example_rows=10)

    n_err = sum(1 for x in report_items if x.get("severity") == "ERROR")
    n_wrn = sum(1 for x in report_items if x.get("severity") == "WARN")

    exit_code = 0
    no_exit = getattr(args, "no_exit", False)
    fail_on = getattr(args, "fail_on", "error")
    if not no_exit:
        if fail_on == "warn" and (n_err > 0 or n_wrn > 0):
            exit_code = 2
        elif fail_on == "error" and n_err > 0:
            exit_code = 2
    if exit_code != 0:
        raise SystemExit(exit_code)
