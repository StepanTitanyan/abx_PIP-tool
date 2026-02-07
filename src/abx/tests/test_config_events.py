import argparse
import json
import pandas as pd

from abx.cli.convert_cmd import _run_events


def test_events_save_and_load_config(tmp_path):
    df = pd.DataFrame(
        {
            "user": ["u1", "u1"],
            "variant": ["a", "a"],
            "ts": ["2025-01-01 00:00:00Z", "2025-01-01 00:01:00Z"],
            "event": ["exposed", "purchase"],
            "amount_usd": ["10", "20"],
        }
    )
    in_path = tmp_path / "events.csv"
    df.to_csv(in_path, index=False)

    cfg_path = tmp_path / "cfg.json"
    out_path_1 = tmp_path / "out1.csv"
    out_path_2 = tmp_path / "out2.csv"

    args1 = argparse.Namespace(
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
        metric=["revenue=continuous:sum_value(purchase, value=amount_usd)"],
        out=str(out_path_1),
        preview=False,
        save_config=str(cfg_path),
        config=None,
    )
    _run_events(args1)

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert cfg["user"] == "user"
    assert cfg["variant"] == "variant"
    assert cfg["time"] == "ts"
    assert cfg["event"] == "event"
    assert cfg["exposure"] == "exposed"
    assert cfg["window"] == "2h"

    args2 = argparse.Namespace(
        data=str(in_path),
        user=None,
        variant=None,
        time=None,
        event=None,
        value=None,
        exposure=None,
        window=None,
        multiexposure=None,
        multivariant=None,
        unassigned=None,
        metric=None,
        out=str(out_path_2),
        preview=False,
        save_config=None,
        config=str(cfg_path),
    )
    _run_events(args2)

    out = pd.read_csv(out_path_2)
    assert "revenue" in out.columns
    assert out.shape[0] == 1
