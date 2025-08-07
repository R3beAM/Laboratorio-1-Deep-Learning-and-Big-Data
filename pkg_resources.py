"""Lightweight stand-in for :mod:`pkg_resources` used by HappyBase.

This module implements only the minimal functions accessed by the
``happybase`` package so that importing it does not trigger the
``pkg_resources`` deprecation warning.
"""
import importlib.metadata
import importlib.resources

class Distribution:
    def __init__(self, name: str):
        self.version = importlib.metadata.version(name)

def get_distribution(name: str) -> Distribution:
    """Return a lightweight distribution object with a ``version`` attribute."""
    return Distribution(name)

def resource_filename(package: str, resource: str) -> str:
    """Return the path to a package resource."""
    return str(importlib.resources.files(package).joinpath(resource))
