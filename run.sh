#!/usr/bin/env bash
set -euo pipefail

sbatch_flag=false
test_flag=false
force_recompute=false
env_file="environment.yml"
env_name="$(awk -F': *' '$1=="name"{print $2; exit}' "$env_file" 2>/dev/null)"
if [[ -z ${env_name:-} ]]; then
  echo "Could not determine environment name from ${env_file}."
  exit 1
fi
kernel_name="$env_name"
job_name="atlas-engine"
while [[ ${1:-} == --* ]]; do
  case "$1" in
    --sbatch)
      sbatch_flag=true
      ;;
    --test)
      test_flag=true
      ;;
    --force-recompute)
      force_recompute=true
      ;;
    *)
      echo "Usage: $0 [--sbatch] [--test] [--force-recompute] <parameters.yml>"
      exit 1
      ;;
  esac
  shift
done

if [[ -z ${1:-} ]]; then
  echo "Usage: $0 [--sbatch] [--test] [--force-recompute] <parameters.yml>"
  exit 1
fi
if [[ -n ${2:-} ]]; then
  echo "Usage: $0 [--sbatch] [--test] [--force-recompute] <parameters.yml>"
  exit 1
fi

yaml_file=$1
test_arg=""
if $test_flag; then
  test_arg="--test"
fi
force_recompute_arg=""
if $force_recompute; then
  force_recompute_arg="--force-recompute"
fi

if $sbatch_flag; then
  submit_dir="$PWD"
  sbatch <<EOF
#!/usr/bin/env bash
#SBATCH --job-name ${job_name}
#SBATCH --nodes 1
#SBATCH --ntasks 1
#SBATCH --cpus-per-task 1
#SBATCH --output ${job_name}-%j.out
#SBATCH --error ${job_name}-%j.err
#SBATCH --time 02:00:00

set -euo pipefail
cd "$submit_dir"
./run.sh $test_arg $force_recompute_arg "$yaml_file"
EOF
  exit $?
fi

if ! command -v conda >/dev/null 2>&1; then
  if command -v module >/dev/null 2>&1; then
    module load conda
  fi
fi

if command -v conda >/dev/null 2>&1; then
  set +u
  source "$(conda info --base)/etc/profile.d/conda.sh"
  if ! conda env list | awk '{print $1}' | grep -q "^$kernel_name$"; then
    conda env create -f environment.yml
  fi
  conda activate "$kernel_name"
  set -u
else
  echo "Conda is not installed. Please install it and try again."
  exit 1
fi

if ! python - <<'PY'
import importlib.util
import sys

sys.exit(0 if importlib.util.find_spec("atlas_engine") else 1)
PY
then
  pip install -e .
fi

if ! python - "$kernel_name" <<'PY'
from jupyter_client.kernelspec import KernelSpecManager
import sys

name = sys.argv[1]
specs = KernelSpecManager().find_kernel_specs()
sys.exit(0 if name in specs else 1)
PY
then
  python -m ipykernel install --sys-prefix --name "$kernel_name" --display-name "$kernel_name"
fi

cmd=(python -m atlas_engine.application --kernel "$kernel_name")
if [[ -n ${test_arg:-} ]]; then
  cmd+=("$test_arg")
fi
if [[ -n ${force_recompute_arg:-} ]]; then
  cmd+=("$force_recompute_arg")
fi
cmd+=("$yaml_file")
"${cmd[@]}"