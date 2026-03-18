"""
Unit tests for ydana.utils.functions.get_callable_from_src.
"""

import os

import pytest

from ydana.utils.functions import get_callable_from_src


def test_get_callable_from_src_success():
    fn = get_callable_from_src("os.path.join")
    assert fn is os.path.join


def test_get_callable_from_src_invalid_format():
    with pytest.raises(ValueError, match="dotted path"):
        get_callable_from_src("not_a_dotted_path")


def test_get_callable_from_src_missing_attribute():
    with pytest.raises(AttributeError, match="callable not found"):
        get_callable_from_src("os.not_existing_attr")


def test_get_callable_from_src_not_callable():
    with pytest.raises(TypeError, match="not callable"):
        get_callable_from_src("os.path")


def test_get_callable_from_src_cache_hit():
    get_callable_from_src.cache_clear()

    fn1 = get_callable_from_src("os.path.join")
    info_after_first = get_callable_from_src.cache_info()

    fn2 = get_callable_from_src("os.path.join")
    info_after_second = get_callable_from_src.cache_info()

    assert fn1 is fn2
    assert info_after_first.misses == 1
    assert info_after_first.hits == 0
    assert info_after_second.misses == 1
    assert info_after_second.hits == 1
