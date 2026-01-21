# C-SON Atlas

Run a configured set of parameterized notebooks to generate atlas calculations and
export results. The workflow is driven by a single YAML file and a thin CLI wrapper.

## Quick Start

```bash
./run.sh workflow.yml
```

On SLURM systems:

```bash
./run.sh --sbatch workflow.yml
```

## `run.sh` Interface

`run.sh` is the primary entrypoint. It:

- Activates (or creates) the conda environment named in `environment.yml`.
- Installs the matching Jupyter kernel (same name as the environment).
- Runs `atlas_engine.application` with your YAML config.
- Optionally submits a SLURM job with `--sbatch`.

Usage:

```bash
./run.sh [--sbatch] <workflow.yml>
```

The `--sbatch` flag submits a short SLURM wrapper via a heredoc. Edit the SBATCH
directives in `run.sh` to tune wallclock, nodes, or CPU allocation.

## Configuration (`workflow.yml`)

The YAML file defines both cluster settings and the notebooks to execute.

```yaml
dask_cluster_kwargs:
  account: m4632
  queue_name: premium
  n_nodes: 1
  n_tasks_per_node: 128
  wallclock: 02:00:00
  scheduler_file: null

notebooks:
  - title: "Domain Sizing"
    children:
      - notebooks/regional-domain-sizing.ipynb:
          parameters:
            grid_yaml: blueprints/.../_grid.yml
            test: true
          output_path: executed/domain-sizing/example.ipynb
```

Notes:
- Relative paths resolve against the YAML file location.
- `output_path` is used in the MyST TOC and for notebook outputs.

## Dask Cluster Lifecycle

When SLURM is available, `atlas_engine.application` uses `atlas_engine.utils.dask_cluster`
to manage a Dask cluster per section that has `use_dask_cluster: true`.

The connected `scheduler_file` is injected into each notebook’s parameters as
`dask_cluster_kwargs.scheduler_file`, so notebooks in that section reuse the
same cluster. If the scheduler file is stale, the cluster helper will attempt
to relaunch when possible.

If SLURM is not available, the code falls back to a local Dask cluster.

## What `application.py` Does

- Loads `workflow.yml` into validated Pydantic models (`atlas_engine.parsers`).
- Optionally launches or connects to a Dask cluster (`atlas_engine.utils.dask_cluster`).
- Executes each notebook with Papermill using the configured kernel (default: `cson-atlas`).
- Renders `{{ key }}` placeholders in markdown cells using the parameters dict.
- Updates `myst.yml` `project.toc` with the executed notebook paths.
- Logs failures and raises a summary error if any notebooks fail.

## MyST TOC Updates

After execution, `myst.yml` is updated to the layout:

```yaml
project:
  toc:
    - file: README.md
    - title: Domain Sizing
      children:
        - file: executed/domain-sizing/example.ipynb
```

## Development Notes

- Python dependencies live in `environment.yml`.
- Tests are under `tests/` (run with `pytest`).
- Parsing logic lives in `atlas_engine/parsers.py`.
- Dask SLURM cluster management lives in `atlas_engine/utils.py`.

