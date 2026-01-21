"""Run parameterized notebooks with Papermill using YAML/JSON inputs."""

from __future__ import annotations

import argparse
import os
import logging
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Dict, Iterable, Optional, Union

try:
    import papermill
except ImportError:  # pragma: no cover - exercised via explicit error path
    papermill = None

from .parsers import load_app_config
from . import utils


logger = logging.getLogger(__name__)

_PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def _render_markdown_placeholders(
    notebook_path: Path, parameters: Dict[str, Any]
) -> Optional[Path]:
    import nbformat

    nb = nbformat.read(str(notebook_path), as_version=4)
    updated = False
    for cell in nb.cells:
        if cell.get("cell_type") != "markdown":
            continue
        source = cell.get("source", "")
        if not source:
            continue

        def _replace(match):
            key = match.group(1)
            if key not in parameters:
                return match.group(0)
            return str(parameters[key])

        new_source = _PLACEHOLDER_PATTERN.sub(_replace, source)
        if new_source != source:
            cell["source"] = new_source
            updated = True

    if not updated:
        return None

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".ipynb",
            delete=False,
            encoding="utf-8",
            dir=str(notebook_path.parent.resolve()),
        ) as handle:
            nbformat.write(nb, handle)
            temp_path = handle.name
    except Exception:
        if temp_path and Path(temp_path).exists():
            Path(temp_path).unlink()
        raise
    return Path(temp_path)


def _notebook_executed_successfully(path: Path) -> bool:
    """Return True if a notebook exists with papermill success metadata."""
    try:
        import nbformat
    except ImportError:  # pragma: no cover - exercised via explicit error path
        return False

    if not path.exists():
        return False
    try:
        nb = nbformat.read(str(path), as_version=4)
    except Exception:
        return False
    papermill_meta = nb.metadata.get("papermill", {})
    if not papermill_meta:
        return False
    if papermill_meta.get("exception") is True:
        return False
    elif papermill_meta.get("exception") is None:
        return True
    return papermill_meta.get("status") == "completed"


def run_notebook(
    notebook_path: Path,
    output_path: Path,
    parameters: Dict[str, Any],
    force_recompute: bool = False,
    kernel_name: str = "cson-atlas",
) -> None:
    """Execute notebooks with papermill and return output paths."""
    try:
        import papermill
    except ImportError:  # pragma: no cover - exercised via explicit error path
        raise RuntimeError("papermill is required to execute notebooks.")
    module = papermill

    output_path = output_path.resolve()
    if _notebook_executed_successfully(output_path) and not force_recompute:
        logger.info("Skipping %s (already completed)", output_path)
        return None
    elif force_recompute and output_path.exists():
        logger.info("Force recomputing %s", output_path)
        output_path.unlink()
    else:
        logger.info("Running %s", output_path)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _render_markdown_placeholders(notebook_path, parameters)
    input_path = temp_path if temp_path is not None else notebook_path
    notebook_dir = notebook_path.parent.resolve()
    repo_root = Path(__file__).resolve().parents[1]
    try:
        previous_cwd = os.getcwd()
        os.chdir(notebook_dir)
        previous_pythonpath = os.environ.get("PYTHONPATH", "")
        if str(repo_root) not in previous_pythonpath.split(os.pathsep):
            updated = os.pathsep.join([str(repo_root), previous_pythonpath]) if previous_pythonpath else str(repo_root)
            os.environ["PYTHONPATH"] = updated
        return module.execute_notebook(
            input_path.name,
            str(output_path),
            parameters=parameters,
            kernel_name=kernel_name,
            cwd=str(notebook_dir),
        )
    finally:
        if "previous_pythonpath" in locals():
            if previous_pythonpath:
                os.environ["PYTHONPATH"] = previous_pythonpath
            else:
                os.environ.pop("PYTHONPATH", None)
        os.chdir(previous_cwd)
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()



def _apply_test_overrides(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "n_test":
                value[key] = 2
                continue
            if key == "test":
                value[key] = True
                continue
            _apply_test_overrides(item)
        return
    if isinstance(value, list):
        for item in value:
            _apply_test_overrides(item)

def parse_args(args: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run parameterized notebooks with papermill.")
    parser.add_argument(
        "yaml_file",
        help="Path to parameters.yml file.",
    )
    parser.add_argument(
        "--kernel",
        default="cson-atlas",
        help="Jupyter kernel name to use when executing notebooks.",
    )
    parser.add_argument(
        "--force-recompute",
        action="store_true",
        help="Force recomputation of notebooks that have already been executed.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Force test settings (n_test=2, test=True) in notebook parameters.",
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

    completed = []
    failed = []
    try:
        for section in app_config.notebook_list.sections:
            cluster = None
            if section.use_dask_cluster and utils.slurm_available():
                cluster = utils.dask_cluster(**dask_cluster_kwargs) if dask_cluster_kwargs else None
            try:
                for entry in section.children:
                    parameters = dict(entry.config.parameters)
                    
                    section_dask_kwargs = dict(dask_cluster_kwargs) if dask_cluster_kwargs else None
                    if cluster is not None and section_dask_kwargs is not None:
                        section_dask_kwargs["scheduler_file"] = cluster.scheduler_file
                    if section_dask_kwargs is not None:
                        parameters["dask_cluster_kwargs"] = section_dask_kwargs
                    if parsed.test:
                        _apply_test_overrides(parameters)
                    
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
                            force_recompute=parsed.force_recompute,                            
                            kernel_name=parsed.kernel,                            
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
    finally:
        pass
    
    if failed:
        raise RuntimeError(
            "Notebook execution failures. Completed: {completed}; Failed: {failed}".format(
                completed=completed, failed=failed
            )
        )
    return 0

if __name__ == "__main__":
    sys.exit(main())
