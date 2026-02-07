import pytest
from abx.cli.convert_cmd import _deconstruct_metric


def test_metric_parsing_basic():
    m = _deconstruct_metric(["conversion=binary:event_exists(purchase)"])
    assert len(m) == 1
    _, spec = list(m.items())[0]
    assert spec[0] == "conversion"
    assert spec[1] == "binary"
    assert spec[2] == "event_exists"
    assert spec[3] == "purchase"
    assert spec[4] == {}


def test_metric_parsing_kwargs():
    m = _deconstruct_metric(["tt=time:time_to_event(purchase, unit=h)"])
    _, spec = list(m.items())[0]
    assert spec[4]["unit"] == "h"


def test_metric_duplicate_names_error():
    with pytest.raises(SystemExit):
        _deconstruct_metric([
            "x=binary:event_exists(purchase)",
            "x=count:count_event(purchase)",
        ])
