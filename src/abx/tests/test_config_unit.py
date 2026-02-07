import argparse
import json
import pandas as pd

from abx.cli.convert_cmd import _run_unit


def test_unit_save_and_load_config(tmp_path):
    df = pd.DataFrame({"uid": ["u1"], "grp": ["a"], "out": [1]})
    in_path = tmp_path / "unit.csv"
    df.to_csv(in_path, index=False)

    cfg_path = tmp_path / "cfg.json"
    out_path_1 = tmp_path / "out1.csv"
    out_path_2 = tmp_path / "out2.csv"

    # 1) Save config
    args1 = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="out",
        keep="",
        dedupe="error",
        out=str(out_path_1),
        preview=False,
        save_config=str(cfg_path),
        config=None,
    )
    _run_unit(args1)

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert cfg["user"] == "uid"
    assert cfg["variant"] == "grp"
    assert cfg["outcome"] == "out"

    # 2) Load config, omit user/variant/outcome on args (set to None)
    args2 = argparse.Namespace(
        data=str(in_path),
        user=None,
        variant=None,
        outcome=None,
        keep="",
        dedupe="error",
        out=str(out_path_2),
        preview=False,
        save_config=None,
        config=str(cfg_path),
    )
    _run_unit(args2)

    out = pd.read_csv(out_path_2)
    assert list(out.columns) == ["user_id", "variant", "outcome"]
    assert out.shape[0] == 1
