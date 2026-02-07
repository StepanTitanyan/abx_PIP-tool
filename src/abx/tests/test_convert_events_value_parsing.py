import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_value_parsing_messy_strings(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1"],
            "variant": ["a", "a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z", "2025-01-01 00:02:00Z"],
            "event": ["exposed", "purchase", "purchase"],
            "amount_usd": ["$1,200.50", "300", "N/A"],
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
        metric=["rev=continuous:sum_value(purchase, value=amount_usd)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)

    # exposure row has amount_usd "$1,200.50" but event isn't purchase; purchase rows: "300" and "N/A"
    # sum should be 300.0
    assert abs(float(out.loc[0, "rev"]) - 300.0) < 1e-9
