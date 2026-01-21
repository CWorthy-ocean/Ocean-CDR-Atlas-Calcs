"""Core package for atlas calculations."""

from . import datasets, parsers
from .datasets import DATASET_REGISTRY
from .utils import dask_cluster

datasets = DATASET_REGISTRY

__all__ = ["datasets", "DATASET_REGISTRY", "parsers", "dask_cluster"]
