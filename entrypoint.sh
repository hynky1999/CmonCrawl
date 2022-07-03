#!/bin/bash
# Currently we only support running tests


run_test(){
    case $1 in 
        *_tests.py)
        TEST_NAME=$1
        ;;
        all)
        TEST_NAME="*tests*.py"
        ;;
    *)
        echo "No tests specified"
        exit 1
        ;;
    esac
    echo "Runing tests:"
    conda run --no-capture-output -n myenv python -m unittest discover -s unit_tests -p "$TEST_NAME"
    echo "Finished running tests"
    exit 0
}


run_extract(){
    exit "$(python3 -m Aggregator.aggregate "$@")"
}

case "$1" in
    test)
        shift;
        run_test "$@"
    ;;
    run)
        shift;
        run_extract "$@"
    ;;
    *)
    echo "Usage: $0 {test {tests} | run {args} }"
    exit 1
    ;;
esac












