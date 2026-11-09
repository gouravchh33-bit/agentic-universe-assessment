#!/usr/bin/env bash
set -euo pipefail
SIM_URL="${1:?usage: ./run.sh <SIM_URL>}"

export SIMULATOR_URL="$SIM_URL"
exec python controller.py