import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_events


def test_unassigned_error_raises(tmp_path):
    # u1 has blank variant -> should error when unassigned=error
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["", ""],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z"],
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
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)


def test_unassigned_drop_removes_users(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u2", "u2"],
            "variant": ["", "", "a", "a"],
            "ts": [
                "2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z",
                "2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z",
            ],
            "event": ["exposed", "purchase", "exposed", "purchase"],
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
        unassigned="drop",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)

    assert out["user_id"].tolist() == ["u2"]
    assert int(out["conversion"].iloc[0]) == 1
