#!/bin/bash
set -uo pipefail

python3 "$(dirname "$0")/plot_results.py" "$@"
