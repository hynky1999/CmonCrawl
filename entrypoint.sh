#!/bin/bash
# Currently we only support running tests
TEST_NAME="*_tests.py"
if [[ $1 == *_tests.py ]]; then
    TEST_NAME="$1"
fi


echo "Runing tests"
conda run --no-capture-output -n myenv python -m unittest discover -s tests -p "$TEST_NAME"









