from pyric_static.metrics import RunMetrics


def test_run_metrics_summary_includes_three_categories():
    m = RunMetrics()
    m.note_unresolved_subject(10, 999)
    m.note_unresolved_subject(10, 999)
    m.note_unlisted_node(42)
    m.note_deserialize_failed("b17.Foo.0.1")

    lines = m.summary_lines()
    assert len(lines) == 3
    assert "missing type mapping" in lines[0]
    assert "(10, 999):2" in lines[0]
    assert "nodes not in" in lines[1]
    assert "42" in lines[1]
    assert "deserialize failed" in lines[2]
    assert "b17.Foo.0.1" in lines[2]
