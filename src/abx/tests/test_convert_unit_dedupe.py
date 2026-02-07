import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_unit


def _run_unit_to_df(tmp_path, df, *, dedupe):
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="out",
        keep="",
        dedupe=dedupe,
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_unit(args)
    return pd.read_csv(out_path)


def test_unit_dedupe_error_raises(tmp_path):
    df = pd.DataFrame(
        {"uid": ["u1", "u1", "u2"], "grp": ["a", "b", "a"], "out": [1, 0, 1]}
    )

    in_path = tmp_path / "unit.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="out",
        keep="",
        dedupe="error",
        out=str(tmp_path / "out.csv"),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)


def test_unit_dedupe_first_keeps_first_row_order(tmp_path):
    df = pd.DataFrame(
        {"uid": ["u1", "u1", "u2"], "grp": ["a", "b", "a"], "out": [1, 0, 1]}
    )
    out = _run_unit_to_df(tmp_path, df, dedupe="first")

    assert out["user_id"].nunique() == 2
    assert int(out[out["user_id"] == "u1"]["outcome"].iloc[0]) == 1


def test_unit_dedupe_last_keeps_last_row_order(tmp_path):
    df = pd.DataFrame(
        {"uid": ["u1", "u1", "u2"], "grp": ["a", "b", "a"], "out": [1, 0, 1]}
    )
