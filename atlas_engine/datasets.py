"""Dataset registry for atlas datasets."""

from __future__ import annotations

from .ocean_cdr_atlas_v0 import DatasetSpec

DATASET_REGISTRY = {
    "oae-efficiency-map_atlas-v0": DatasetSpec(
        name="oae-efficiency-map_atlas-v0",
        n_years=15,
        polygon_ids=list(range(0, 690)),
        injection_years=[1999],
        injection_months=[1, 4, 7, 10],
        model_year_align=1999,
        model_year_offset=1652,
    )
}


def get_dataset(name: str) -> DatasetSpec:
    """Return a registered dataset by name."""
    return DATASET_REGISTRY[name]


__all__ = ["DATASET_REGISTRY", "get_dataset", "DatasetSpec"]
