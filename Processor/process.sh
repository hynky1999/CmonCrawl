#!/bin/bash
SCRIPT=$(realpath "$0")
cd "$(dirname "$SCRIPT")/App" || exit
conda run  --prefix="$ENV_PATH" --no-capture-output python processor.py "$@"

