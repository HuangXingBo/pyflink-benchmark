CURRENT_DIR="$(cd "$( dirname "$0" )" && pwd)"
PYFLINK_BENCH_MARK_DR=$(dirname $(dirname "$(dirname "$CURRENT_DIR")"))

export PYTHONPATH=${PYFLINK_BENCH_MARK_DR}:${PYTHONPATH}

# get all table tests
methods_string=$(python -c "import inspect;\
from python.flink.table.test_flink_table_base import TableTests;\
print(','.join([name for name, _ in inspect.getmembers(TableTests, predicate=inspect.isfunction)]))")

methods=(${methods_string//,/ })

tests=("PyFlinkStreamTableJavaTests" "PyFlinkStreamTableThreadModeTests" "PyFlinkStreamTableProcessModeTests")

for test in ${tests[@]}; do
    echo ${test}
    for method in ${methods[@]}; do
        python -c "from python.flink.table.test_flink_table_base import $test;t=$test();t.${method}()";
        sleep 3
    done
done