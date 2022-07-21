#!/bin/bash

ENV_PATH="myenv"
if [[ ! -e "$ENV_PATH" ]]; then
    conda env create --prefix "$ENV_PATH" --file environment.yml
fi

conda run --no-capture-output --prefix "$ENV_PATH" python processor.py "$@"

