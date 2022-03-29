#!/usr/bin/env bash

function contains_element() {
    arr=($1)
    if echo "${arr[@]}" | grep -w "$2" &>/dev/null; then
        echo true
    else
        echo false
    fi
}

function check_argument_valid() {
    local arguments=$1
    local valid_arguments=$2
    for argument in ${arguments[*]} ; do
        if [[ `contains_element "${valid_arguments[*]}" "${argument}"` = false ]]; then
            echo "unknown argument '${argument}', valid arguments are ""'${valid_arguments[*]}'"
            exit 1
        fi
    done
}

EXECUTION_MODE=("stream" "batch")

EXECUTION_ENVIRONMENT=("table")

PYTHON_EXECUTION_MODE=("java" "process" "thread")

# parse_opts
USAGE="
usage: $0 [options]
-h          print this help message and exit
-m [stream,batch,all]
            execution mode, default: all
-s [table,all]
            execution environments which split by comma(,), default: all
-i [java,thread,process,all]
            python execution mode tests which split by comma(,), default: all
            note:
                java     -> java udf tests
                process  -> python udf tests run in process execution mode
                thread   -> python udf tests run in thread execution mode
Examples:
  ./benchmark.sh -m stream           =>  specify stream execution mode.
  ./benchmark.sh -s table            =>  specify Table related tests.
  ./benchmark.sh -i all              =>  include all related tests.
  ./benchmark.sh -i thread,process   =>  include thread and process related tests.
  ./benchmark.sh                     =>  exec all tests.
"
while getopts "hm:s:i:" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        m)
            INPUT_EXECUTION_MODE=($(echo $OPTARG | tr ',' ' ' ))

            if [[ `contains_element "${INPUT_EXECUTION_MODE[*]}" "all"` = true ]]; then
                INPUT_EXECUTION_MODE=(${EXECUTION_MODE[*]})
            fi

            check_argument_valid "${INPUT_EXECUTION_MODE[*]}" "${EXECUTION_MODE[*]}"

            EXECUTION_MODE=(${INPUT_EXECUTION_MODE[*]})
            ;;
        s)
            INPUT_EXECUTION_ENVIRONMENT=($(echo $OPTARG | tr ',' ' ' ))

            if [[ `contains_element "${INPUT_EXECUTION_ENVIRONMENT[*]}" "all"` = true ]]; then
                INPUT_EXECUTION_ENVIRONMENT=(${EXECUTION_ENVIRONMENT[*]})
            fi

            check_argument_valid "${INPUT_EXECUTION_ENVIRONMENT[*]}" "${EXECUTION_ENVIRONMENT[*]}"

            EXECUTION_ENVIRONMENT=(${INPUT_EXECUTION_ENVIRONMENT[*]})
            ;;
        i)
            INPUT_PYTHON_EXECUTION_MODE=($(echo $OPTARG | tr ',' ' ' ))

            if [[ `contains_element "${INPUT_PYTHON_EXECUTION_MODE[*]}" "all"` = true ]]; then
                INPUT_PYTHON_EXECUTION_MODE=(${PYTHON_EXECUTION_MODE[*]})
            fi

            check_argument_valid "${INPUT_PYTHON_EXECUTION_MODE[*]}" "${PYTHON_EXECUTION_MODE[*]}"

            PYTHON_EXECUTION_MODE=(${INPUT_PYTHON_EXECUTION_MODE[*]})
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done

CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"

for env in "${EXECUTION_ENVIRONMENT[*]}"; do
    if [[ ${env} == "table" ]]; then
        TABLE_SCRIPT="$CURRENT_DIR/flink/table/benchmark_table.sh"
        bash ${TABLE_SCRIPT} "${EXECUTION_MODE[*]}" "${PYTHON_EXECUTION_MODE[*]}"
    fi
done
