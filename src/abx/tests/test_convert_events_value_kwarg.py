import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_events_value_kwarg_overrides_global_value(tmp_path):
    # purchase uses amount_usd, refund uses refund_usd
    df = pd.DataFrame(
        {
            "user": ["u1", "u1", "u1", "u2", "u2"],
            "variant": ["a", "a", "a", "b", "b"],
            "ts": [
                "2025-01-01 00:00:00Z",
                "2025-01-01 00:01:00Z",
                "2025-01-01 00:02:00Z",
                "2025-01-01 00:00:00Z",
                "2025-01-01 00:05:00Z",
            ],
            "event": ["exposed", "purchase", "refund", "exposed", "purchase"],
            "amount": [None, 999, None, None, 111],          # global --value points here
            "amount_usd": [None, 10.0, None, None, 20.0],    # purchase should use this
            "refund_usd": [None, None, 3.0, None, None],     # refund should use this
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
        value="amount",  # default value col (should NOT be used for revenue/refunds)
        exposure="exposed",
        window=None,
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=[
            "revenue=continuous:sum_value(purchase, value=amount_usd)",
            "refunds=continuous:sum_value(refund, value=refund_usd)",
            "conversion=binary:event_exists(purchase)",
        ],
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    _run_events(args)

    out = pd.read_csv(out_path)

    r1 = out[out["user_id"] == "u1"].iloc[0]
    assert float(r1["revenue"]) == 10.0
    assert float(r1["refunds"]) == 3.0
    assert int(r1["conversion"]) == 1

    r2 = out[out["user_id"] == "u2"].iloc[0]
    assert float(r2["revenue"]) == 20.0
    assert float(r2["refunds"]) == 0.0
    assert int(r2["conversion"]) == 1


import pytest


def test_events_continuous_requires_value_column(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
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
        value=None,           # no global value
        exposure="exposed",
        window=None,
        multiexposure="first",
        multivariant="error",
        unassigned="error",
        metric=["revenue=continuous:sum_value(purchase)"],  # no value= either
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )

    with pytest.raises(SystemExit):
        _run_events(args)
