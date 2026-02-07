import argparse
import pandas as pd
import pytest

from abx.cli.convert_cmd import _run_events


def test_bad_window_raises(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z"],
            "event": ["exposed", "purchase"],
        }
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
        exposure="exposed",
        window="not_a_duration",
        multiexposure="first",
        multivariant="from_exposure",
        unassigned="error",
        metric=["conversion=binary:event_exists(purchase)"],
        out=str(tmp_path / "out.csv"),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)
