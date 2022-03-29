#!/usr/bin/env bash

OLD_IFS="$IFS"
IFS=" "
EXECUTION_MODE=(${1})
PYTHON_EXECUTION_MODE=(${2})

IDFS="$OLD_IFS"

all_tests=()

for mode in "${EXECUTION_MODE[@]}"; do
    if [[ ${mode} = "stream" ]]; then
        mode="Stream"
    elif [[ ${mode} = "batch" ]]; then
        mode="Batch"
    else
        echo "unknown execution mode ${mode}, only supported execution modes are ('stream', 'batch')"
        exit 1
    fi

    for python_mode in "${PYTHON_EXECUTION_MODE[@]}"; do
        if [[ ${python_mode} = "java" ]]; then
            python_mode="Java"
        elif [[ ${python_mode} = "process" ]]; then
            python_mode="ProcessMode"
        elif [[ ${python_mode} = "thread" ]]; then
            python_mode="ThreadMode"
        else
            echo "unknown python execution mode ${python_mode}, only supported python execution modes are ('java', 'process', 'thread')"
            exit 1
        fi
        all_tests+=("PyFlink${mode}Table${python_mode}Tests")
    done
done

CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"
PYFLINK_BENCH_MARK_DR=$(dirname $(dirname "$(dirname "$CURRENT_DIR")"))

export PYTHONPATH=${PYFLINK_BENCH_MARK_DR}:${PYTHONPATH}

# get all table tests
methods_string=$(python -c "import inspect;\
from python.flink.table.test_flink_table_base import TableTests;\
print(','.join([name for name, _ in inspect.getmembers(TableTests, predicate=inspect.isfunction)]))")

methods=(${methods_string//,/ })

for test in ${all_tests[@]}; do
    echo ${test}
    for method in ${methods[@]}; do
        python -c "from python.flink.table.test_flink_table_base import $test;t=$test();t.${method}()";
        sleep 3
    done
done
