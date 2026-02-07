import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_events


def _run_events_to_df(tmp_path, df, *, multiexposure):
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
        multiexposure=multiexposure,
        multivariant="from_exposure",  # important: makes variant tie to chosen exposure row
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    return pd.read_csv(out_path)


def test_multiexposure_error_raises(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1"],
            "variant": ["a", "a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 01:00:00Z", "2025-01-01 02:00:00Z"],
            "event": ["exposed", "exposed", "purchase"],
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
        multiexposure="error",
        multivariant="from_exposure",
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)


def test_multiexposure_first_uses_earliest_exposure_time(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1"],
            "variant": ["a", "a", "a"],
            "ts": ["2025-01-01 01:00:00Z", "2025-01-01 00:00:00Z", "2025-01-01 02:00:00Z"],
            "event": ["exposed", "exposed", "purchase"],
        }
    )

    out = _run_events_to_df(tmp_path, df, multiexposure="first")
    r = out[out["user_id"] == "u1"].iloc[0]

    # earliest exposure is 00:00
    assert str(r["exposure_time"]).startswith("2025-01-01 00:00:00")


def test_multiexposure_last_uses_latest_exposure_time(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1"],
            "variant": ["a", "a", "a"],
            "ts": ["2025-01-01 01:00:00Z", "2025-01-01 00:00:00Z", "2025-01-01 02:00:00Z"],
            "event": ["exposed", "exposed", "purchase"],
        }
    )

    out = _run_events_to_df(tmp_path, df, multiexposure="last")
    r = out[out["user_id"] == "u1"].iloc[0]

    # latest exposure is 01:00
    assert str(r["exposure_time"]).startswith("2025-01-01 01:00:00")
