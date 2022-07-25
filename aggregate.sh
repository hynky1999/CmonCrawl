#!/bin/bash
ROOT_DIR="$1" && shift
cd "$ROOT_DIR" || exit 1
"${ROOT_DIR}/Aggregator/env/bin/python3" -m "Aggregator.aggregator" "$@"


