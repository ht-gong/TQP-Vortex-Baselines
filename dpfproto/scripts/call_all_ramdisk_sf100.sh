#!/usr/bin/env bash
set -euo pipefail

# document sf100 ramdisk run
SF=100 DEVICE_SIZE=64G "$(dirname "$0")/run_golap_ramdisk.sh"
