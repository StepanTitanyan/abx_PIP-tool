import argparse
import pandas as pd

from abx.cli.convert_cmd import _run_unit


def test_unit_basic_csv(tmp_path):
    df = pd.DataFrame(
        {"uid": ["u1", "u2"], "grp": ["A", "B"], "rev": [10.5, 0.0]}
    )
    in_path = tmp_path / "unit.csv"
    out_path = tmp_path / "out.csv"
    df.to_csv(in_path, index=False)

    args = argparse.Namespace(
        data=str(in_path),
        user="uid",
        variant="grp",
        outcome="rev",
        metric=None,          # <-- add this line
        keep="",
        dedupe="error",
        out=str(out_path),
        preview=False,
        save_config=None,
        config=None,
    )


    _run_unit(args)

    out = pd.read_csv(out_path)
    assert list(out.columns) == ["user_id", "variant", "outcome"]
    assert out.shape[0] == 2
