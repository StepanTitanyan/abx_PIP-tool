import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_events_value_kwarg_column_is_cleaned_to_numeric(tmp_path):
    # revenue column is messy string, but metric uses value=amount_usd, so that column must be cleaned to numeric
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z"],
            "event": ["exposed", "purchase"],
            "amount_usd": ["$1,200.50", "300"],
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
        metric=["revenue=continuous:sum_value(purchase, value=amount_usd)"],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)
    out = pd.read_csv(out_path)

    assert abs(float(out.loc[0, "revenue"]) - 300.0) < 1e-9
