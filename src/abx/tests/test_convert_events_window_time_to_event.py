import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_time_to_event_respects_window(tmp_path):
    # exposure at 00:00, purchase at +3h, window=2h => event excluded => tt should be NaN
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 03:00:00Z"],
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
        window="2h",
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

    assert pd.isna(out.loc[0, "tt"])
