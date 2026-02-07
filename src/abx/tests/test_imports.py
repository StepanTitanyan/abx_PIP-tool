def test_import_convert():
    from abx.cli.convert_cmd import _run_unit, _run_events
    assert callable(_run_unit)
    assert callable(_run_events)
