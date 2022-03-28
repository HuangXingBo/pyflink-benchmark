from abc import ABC

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import TableEnvironment, EnvironmentSettings

from python.test_case_utils import BenchmarkTestBase


class PyFlinkTests(BenchmarkTestBase, ABC):
    def __init__(self):
        self.batch_env = StreamExecutionEnvironment.get_execution_environment()
        self.batch_env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.batch_table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

        self.stream_env = StreamExecutionEnvironment.get_execution_environment()
        self.stream_env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.stream_table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
