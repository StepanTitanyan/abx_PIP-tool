import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_events_window_scopes_counts(tmp_path):
    # u1: purchase at +1h (inside 2h window), purchase at +3h (outside) => count should be 1
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1"],
            "variant": ["a", "a", "a"],
            "ts": [
                "2025-01-01 00:00:00Z",  # exposure
                "2025-01-01 01:00:00Z",  # inside
                "2025-01-01 03:00:00Z",  # outside
            ],
            "event": ["exposed", "purchase", "purchase"],
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
        window="2h",
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=["purchases=count:count_event(purchase)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)

    r1 = out[out["user_id"] == "u1"].iloc[0]
    assert int(r1["purchases"]) == 1
