#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.tabular` package."""

import pytest  # noqa

import kiara_plugin.tabular


def test_assert():

    assert kiara_plugin.tabular.get_version() is not None
