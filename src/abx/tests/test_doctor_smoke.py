import argparse
import pandas as pd
import pytest


def _get_doctor_runner():
    import abx.cli.doctor_cmd as doctor_cmd
    if hasattr(doctor_cmd, "_run_doctor"):
        return doctor_cmd._run_doctor
    if hasattr(doctor_cmd, "run_doctor"):
        return doctor_cmd.run_doctor
    return None


def test_doctor_smoke_and_report(tmp_path):
    run_doctor = _get_doctor_runner()
    if run_doctor is None:
        pytest.skip("doctor runner function not found (expected _run_doctor or run_doctor)")

    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "variant": ["a", "a", "b"],
            "conversion": [1, 0, 1],
            "revenue": [10.0, 0.0, 3.5],
        }
    )
    in_path = tmp_path / "converted.csv"
    df.to_csv(in_path, index=False)

    report_path = tmp_path / "doctor_report.md"

    args = argparse.Namespace(
        data=str(in_path),
        user="user_id",
        variant="variant",
        check="integrity,variants,missingness,metrics,distribution",
        metrics="conversion,revenue",
        ignore=None,
        report=str(report_path),
        only="all",
        min_n=2,
        allocation=None,
        alpha=0.05,
        fail_on = None,
        skip = None,
        config=None,
        save_config=None,
        preview = None,
    )

    run_doctor(args)

    assert report_path.exists()
    txt = report_path.read_text(encoding="utf-8")
    assert "doctor" in txt.lower() or "finding" in txt.lower()
