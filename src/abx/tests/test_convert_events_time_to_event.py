import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_events


def test_time_to_event_units(tmp_path):
    # exposure at 00:00, purchase at 01:30 => 90 minutes => 5400 seconds
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 01:30:00Z"],
            "event": ["exposed", "purchase"],
        }
    )

    in_path = tmp_path / "events.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="user",
        variant="variant",
        time="ts",
        event="event",
        value=None,
        exposure="exposed",
        window=None,
        multiexposure="first",
        multivariant="from_exposure",
        unassigned="error",
        metric=[
            "tt_s=time:time_to_event(purchase, unit=s)",
            "tt_m=time:time_to_event(purchase, unit=m)",
            "tt_h=time:time_to_event(purchase, unit=h)",
            "tt_d=time:time_to_event(purchase, unit=d)",
        ],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)
    r = out[out["user_id"] == "u1"].iloc[0]

    assert abs(float(r["tt_s"]) - 5400.0) < 1e-9
    assert abs(float(r["tt_m"]) - 90.0) < 1e-9
    assert abs(float(r["tt_h"]) - 1.5) < 1e-9
    assert abs(float(r["tt_d"]) - (5400.0 / 86400.0)) < 1e-9


def test_time_to_event_missing_event_is_nan(tmp_path):
    # u1 has exposure but never purchases -> tt should be NaN
    df = pd.DataFrame(
        {
            "user": ["u1"],
            "variant": ["a"],
            "ts": ["2025-01-01 00:00:00Z"],
            "event": ["exposed"],
        }
    )

    in_path = tmp_path / "events.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="user",
        variant="variant",
        time="ts",
        event="event",
        value=None,
        exposure="exposed",
        window=None,
        multiexposure="first",
        multivariant="from_exposure",
        unassigned="error",
        metric=["tt=time:time_to_event(purchase, unit=h)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)
    r = out[out["user_id"] == "u1"].iloc[0]

    assert pd.isna(r["tt"])


def test_time_to_event_requires_exposure(tmp_path):
    # time_to_event should fail without --exposure
    df = pd.DataFrame(
        {
            "user": ["u1"],
            "variant": ["a"],
            "ts": ["2025-01-01 00:00:00Z"],
            "event": ["purchase"],
        }
    )

    in_path = tmp_path / "events.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="user",
        variant="variant",
        time="ts",
        event="event",
        value=None,
        exposure=None,  # <-- no exposure
        window=None,
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=["tt=time:time_to_event(purchase, unit=h)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)
