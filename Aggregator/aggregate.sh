#!/bin/bash
cd "./App" || exit
ENV_PATH="$(shift)"
echo "$ENV_PATH"
# conda run  --prefix="$ENV_PATH" --no-capture-output python aggregator.py "$@"

