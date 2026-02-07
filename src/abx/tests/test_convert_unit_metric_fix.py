import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_unit


def test_unit_metric_fix_binary_truthy_falsy_and_numeric(tmp_path):
    df = pd.DataFrame(
        {
            "uid": ["u1", "u2", "u3", "u4", "u5", "u6"],
            "grp": ["A", "B", "A", "B", "A", "B"],
            "conv_raw": ["yes", "no", "TRUE", "0", 1, ""],
        }
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["conversion=binary:fix(conv_raw)"],
        keep="",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_unit(args)
    out = pd.read_csv(out_path)

    # expected mapping: yes/true/1 -> 1, no/0 -> 0, blank -> NA
    m = dict(zip(out["user_id"].tolist(), out["conversion"].tolist()))
    assert int(m["u1"]) == 1
    assert int(m["u2"]) == 0
    assert int(m["u3"]) == 1
    assert int(m["u4"]) == 0
    assert int(m["u5"]) == 1
    assert pd.isna(m["u6"])


def test_unit_metric_fix_continuous_parses_messy_numbers(tmp_path):
    df = pd.DataFrame(
        {
            "uid": ["u1", "u2", "u3"],
            "grp": ["a", "b", "a"],
            "rev_raw": ["$1,200.50", "  -30  ", "N/A"],
        }
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["revenue=continuous:fix(rev_raw)"],
        keep="",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_unit(args)
    out = pd.read_csv(out_path)

    r = dict(zip(out["user_id"].tolist(), out["revenue"].tolist()))
    assert abs(float(r["u1"]) - 1200.50) < 1e-9
    assert abs(float(r["u2"]) - (-30.0)) < 1e-9
    assert pd.isna(r["u3"])


def test_unit_metric_fix_count_requires_integers(tmp_path):
    df = pd.DataFrame(
        {
            "uid": ["u1", "u2", "u3"],
            "grp": ["a", "a", "b"],
            "cnt_raw": ["10", "2.5", "3"],
        }
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["purchases=count:fix(cnt_raw)"],
        keep="",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)


def test_unit_metric_drops_source_columns_by_default(tmp_path):
    df = pd.DataFrame(
        {
            "uid": ["u1", "u2"],
            "grp": ["a", "b"],
            "rev_raw": ["10", "20"],
        }
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["revenue=continuous:fix(rev_raw)"],
        keep="",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_unit(args)
    out = pd.read_csv(out_path)

    assert "rev_raw" not in out.columns
    assert set(out.columns) == {"user_id", "variant", "revenue"}


def test_unit_metric_keep_preserves_source_column(tmp_path):
    df = pd.DataFrame(
        {
            "uid": ["u1"],
            "grp": ["a"],
            "rev_raw": ["10"],
        }
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["revenue=continuous:fix(rev_raw)"],
        keep="rev_raw",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_unit(args)
    out = pd.read_csv(out_path)

    assert "rev_raw" in out.columns
    assert "revenue" in out.columns


def test_unit_metric_outcome_conflict_errors(tmp_path):
    df = pd.DataFrame({"uid": ["u1"], "grp": ["a"], "out": [1]})
    in_path = tmp_path / "unit.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="out",
        metric=["conversion=binary:fix(out)"],
        keep="",
        dedupe="error",
        out=str(tmp_path / "out.csv"),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)


def test_unit_metric_bad_rule_errors(tmp_path):
    df = pd.DataFrame({"uid": ["u1"], "grp": ["a"], "x": [1]})
    in_path = tmp_path / "unit.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome=None,
        metric=["x=binary:not_fix(x)"],
        keep="",
        dedupe="error",
        out=str(tmp_path / "out.csv"),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)
