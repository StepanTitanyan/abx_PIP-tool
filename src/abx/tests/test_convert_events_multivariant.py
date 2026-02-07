import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_events


def _run_events_to_df(tmp_path, df, *, multivariant, exposure=None):
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
        exposure=exposure,
        window=None,
        multiexposure="first",
        multivariant=multivariant,
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    return pd.read_csv(out_path)


def test_events_multivariant_error_raises(tmp_path):
    # u1 has variant a then b -> should error in multivariant=error
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u2"],
            "variant": ["a", "b", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z", "2025-01-01 00:00:00Z"],
            "event": ["purchase", "purchase", "purchase"],
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
        exposure=None,
        window=None,
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)


def test_events_multivariant_first(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u2"],
            "variant": ["a", "b", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z", "2025-01-01 00:00:00Z"],
            "event": ["purchase", "purchase", "purchase"],
        }
    )
    out = _run_events_to_df(tmp_path, df, multivariant="first", exposure=None)

    v1 = out[out["user_id"] == "u1"]["variant"].iloc[0]
    assert v1 == "a"


def test_events_multivariant_last(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u2"],
            "variant": ["a", "b", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z", "2025-01-01 00:00:00Z"],
            "event": ["purchase", "purchase", "purchase"],
        }
    )
    out = _run_events_to_df(tmp_path, df, multivariant="last", exposure=None)

    v1 = out[out["user_id"] == "u1"]["variant"].iloc[0]
    assert v1 == "b"


def test_events_multivariant_mode(tmp_path):
    # u1 has a twice and b once -> mode should choose a
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1", "u2"],
            "variant": ["a", "a", "b", "b"],
            "ts": [
                "2025-01-01 00:00:00Z",
                "2025-01-01 00:01:00Z",
                "2025-01-01 00:02:00Z",
                "2025-01-01 00:00:00Z",
            ],
            "event": ["purchase", "purchase", "purchase", "purchase"],
        }
    )
    out = _run_events_to_df(tmp_path, df, multivariant="mode", exposure=None)

    v1 = out[out["user_id"] == "u1"]["variant"].iloc[0]
    assert v1 == "a"


def test_events_multivariant_from_exposure(tmp_path):
    # u1 variant changes, but at exposure time variant is a -> should pick a
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1", "u2", "u2"],
            "variant": ["a", "b", "b", "b", "b"],
            "ts": [
                "2025-01-01 00:00:00Z",  # exposure
                "2025-01-01 00:01:00Z",
                "2025-01-01 00:02:00Z",
                "2025-01-01 00:00:00Z",  # exposure
                "2025-01-01 00:03:00Z",
            ],
            "event": ["exposed", "purchase", "purchase", "exposed", "purchase"],
        }
    )
    out = _run_events_to_df(tmp_path, df, multivariant="from_exposure", exposure="exposed")

    v1 = out[out["user_id"] == "u1"]["variant"].iloc[0]
    assert v1 == "a"
