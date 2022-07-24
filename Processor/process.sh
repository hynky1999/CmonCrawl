#!/bin/bash
PROC_PATH="$1" && shift
"${PROC_PATH}/env/bin/python3" "$PROC_PATH/App/processor.py" "$@"
