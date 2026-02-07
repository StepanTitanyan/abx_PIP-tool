import pytest
from abx.cli.convert_cmd import _deconstruct_metric


def test_metric_missing_equals_raises():
    with pytest.raises(SystemExit):
        _deconstruct_metric(["binary:event_exists(purchase)"])


def test_metric_missing_parens_raises():
    with pytest.raises(SystemExit):
        _deconstruct_metric(["x=binary:event_existspurchase"])


def test_metric_bad_kwarg_raises():
    with pytest.raises(SystemExit):
        _deconstruct_metric(["x=time:time_to_event(purchase, unit)"])
