"""Run parameterized notebooks with Papermill using YAML/JSON inputs."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Optional, Union

try:
    import papermill
except ImportError:  # pragma: no cover - exercised via explicit error path
    papermill = None

from parsers import load_app_config
import utils


logger = logging.getLogger(__name__)


def run_notebook(
    notebook_path: Path,
    output_path: Path,
    parameters: Dict[str, Any],
    kernel_name: str = "atlas-calcs",
) -> None:
    """Execute notebooks with papermill and return output paths."""
    try:
        import papermill
    except ImportError:  # pragma: no cover - exercised via explicit error path
        raise RuntimeError("papermill is required to execute notebooks.")
    module = papermill

    output_path.parent.mkdir(parents=True, exist_ok=True)

    return module.execute_notebook(
        str(notebook_path),
        str(output_path),
        parameters=parameters,
        kernel_name=kernel_name,
    )


def parse_args(args: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run parameterized notebooks with papermill.")
    parser.add_argument(
        "yaml_file",
        help="Path to parameters.yml file.",
    )
    return parser.parse_args(args=args)


def main(args: Optional[Iterable[str]] = None) -> int:
    """CLI entrypoint for running notebooks."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parsed = parse_args(args=args)
    app_config = load_app_config(Path(parsed.yaml_file))
    dask_cluster_kwargs = (
        app_config.dask_cluster_kwargs.model_dump()
        if app_config.dask_cluster_kwargs is not None
        else None
    )

    if utils.slurm_available():
        cluster = utils.dask_cluster(**dask_cluster_kwargs) if dask_cluster_kwargs else None
    else:
        cluster = None
    
    completed = []
    failed = []
    try:
        for entry in app_config.notebook_list.notebooks:
            parameters = dict(entry.config.parameters)
            
            if dask_cluster_kwargs is not None:
                parameters["dask_cluster_kwargs"] = dask_cluster_kwargs
            if cluster is not None:
                parameters["dask_cluster_kwargs"]["scheduler_file"] = cluster.scheduler_file
            
            notebook_path = Path(entry.notebook_name)
            if notebook_path.suffix == "":
                notebook_path = notebook_path.with_suffix(".ipynb")
            output_path = Path(entry.config.output_path)
            if output_path.suffix == "":
                output_path = output_path.with_suffix(".ipynb")
            
            logger.info("Running %s -> %s", notebook_path, output_path)
            try:
                run_notebook(
                    notebook_path,
                    output_path=output_path,
                    parameters=parameters,
                )
                completed.append(str(output_path))
                logger.info("Completed %s", output_path)
            except Exception as exc:
                failed.append(str(output_path))
                logger.exception("Failed %s: %s", output_path, exc)
    finally:
        if cluster is not None:
            logger.info("Shutting down cluster")
            cluster.shutdown()
    if failed:
        raise RuntimeError(
            "Notebook execution failures. Completed: {completed}; Failed: {failed}".format(
                completed=completed, failed=failed
            )
        )
    return 0

if __name__ == "__main__":
    sys.exit(main())
