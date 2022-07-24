#!/bin/bash
AGG_PATH="$1" && shift
"${AGG_PATH}/env/bin/python3" "$AGG_PATH/App/aggregator.py" "$@"


