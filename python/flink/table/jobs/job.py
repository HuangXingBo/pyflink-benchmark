import argparse
import os
import time
from enum import Enum

from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf


class QPS(Enum):
    QPS_10_W = 100000
    QPS_20_W = 200000
    QPS_30_W = 300000
    QPS_40_W = 400000
    QPS_50_W = 500000
    QPS_100_W = 1000000
    QPS_200_W = 2000000
    QPS_250_W = 2500000
    QPS_300_W = 3000000
    QPS_400_W = 4000000
    QPS_500_W = 5000000
    QPS_1000_W = 10000000


def create_source_sink(t_env, num_rows: int, field_size: int,
                       qps: QPS = QPS.QPS_10_W):
    t_env.execute_sql(f"""
        CREATE TABLE source_table (
            id STRING,
            d int not null
        ) WITH (
          'connector' = 'SimpleDataGen',
          'rows-per-second' = '{qps.value}',
          'field-length' = '{field_size}',
          'number-of-rows' = '{num_rows}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE sink_table (
            id STRING
        ) WITH (
          'connector' = 'blackhole'
        )
    """)


@udf(result_type=DataTypes.STRING(), func_type="general")
def upper(s):
    return s.upper()


def main(args):
    if args.mode == 'stream':
        t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    elif args.mode == 'batch':
        t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    else:
        raise RuntimeError('You need to specify `---mode` to `stream` or `batch`')

    if args.exec_mode == 'process':
        t_env.get_config().get_configuration().set_string(
            "python.execution-mode", "process")
    elif args.exec_mode == 'thread':
        t_env.get_config().get_configuration().set_string(
            "python.execution-mode", "thread")
    elif not args.exec_mode == 'java':
        raise RuntimeError('You need to specify `---exec_mode` to `process`, `thread` or `java`')

    current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    java_target_dir = os.path.abspath(current_dir + "/../../../../java/target")
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file://{0}/benchmark-0.1.jar".format(java_target_dir))
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")
    t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)

    if args.exec_mode == 'java':
        t_env.create_java_temporary_system_function(
            "upper_op", "com.pyflink.benchmark.function.StringUpper")
    else:
        t_env.create_temporary_function("upper_op", upper)

    num_rows = 500000
    create_source_sink(t_env, num_rows, 100, QPS.QPS_500_W)
    table_result = t_env.from_path('source_table') \
        .select("upper_op(id)") \
        .execute_insert('sink_table')
    beg_time = time.time()
    table_result.wait()
    print("{0} in {1} computes 1k bytes string upper QPS is {2} ".format(
        args.exec_mode,
        args.mode,
        num_rows / (time.time() - beg_time)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", default="", help="stream or batch")
    parser.add_argument("--exec_mode", default="",
                        help="java, process or thread")

    main(parser.parse_known_args()[0])
