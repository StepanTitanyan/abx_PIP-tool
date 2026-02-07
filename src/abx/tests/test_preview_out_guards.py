import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_unit, _run_events


def test_unit_preview_and_out_conflict(tmp_path):
    df = pd.DataFrame({"uid": ["u1"], "grp": ["a"], "out": [1]})
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
        preview=True,  # conflict
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)


def test_unit_missing_out_and_preview(tmp_path):
    df = pd.DataFrame({"uid": ["u1"], "grp": ["a"], "out": [1]})
    in_path = tmp_path / "unit.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="out",
        keep="",
        dedupe="error",
        out=None,
        preview=False,  # neither provided
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_unit(args)


def test_events_preview_and_out_conflict(tmp_path):
    df = pd.DataFrame(
        {"user": ["u1"], "variant": ["a"], "ts": ["2025-01-01 00:00:00Z"], "event": ["purchase"]}
    )
    in_path = tmp_path / "events.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="user",
        variant="variant",
        time="ts",
        event="event",
        value=None,
        exposure=None,
        window=None,
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(tmp_path / "out.csv"),
        preview=True,  # conflict
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)
