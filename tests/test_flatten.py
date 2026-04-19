"""Mirror pyric's ``flatten`` semantics.

Single-key mappings collapse onto the parent key (this is how pyric
unwraps DSDL wrappers like ``{"value": [...]}`` into flat field keys).
"""

from pyric_static.flatten import flatten


def test_scalar_in_single_key_mapping_keeps_key():
    assert flatten({"a": 1}) == {"a": 1}


def test_single_key_nested_dict_collapses():
    assert flatten({"outer": {"inner": 2}}) == {"outer": 2}


def test_multi_key_with_single_key_child_collapses_child_to_parent_key():
    assert flatten({"a": 1, "b": {"c": 2}}) == {"a": 1, "b": 2}


def test_multi_key_nested_dict_joins_with_sep():
    assert flatten({"a": 1, "b": {"c": 2, "d": 3}}) == {"a": 1, "b.c": 2, "b.d": 3}


def test_list_indices_use_parent_key():
    assert flatten({"xs": [10, 20, 30]}) == {"xs[0]": 10, "xs[1]": 20, "xs[2]": 30}


def test_single_key_wrapping_list_collapses_to_parent():
    data = {"a": {"b": [1, 2]}, "c": 3}
    assert flatten(data) == {"a[0]": 1, "a[1]": 2, "c": 3}


def test_keeps_strings_atomic():
    assert flatten({"name": "hello"}) == {"name": "hello"}
