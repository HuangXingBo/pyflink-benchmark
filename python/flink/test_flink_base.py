from abc import ABC

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, CheckpointingMode
from pyflink.table import TableEnvironment, EnvironmentSettings, StreamTableEnvironment

from python.test_case_utils import BenchmarkTestBase


class PyFlinkTests(BenchmarkTestBase, ABC):
    def __init__(self):
        self.batch_env = StreamExecutionEnvironment.get_execution_environment()
        self.batch_env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.batch_table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

        self.stream_env = StreamExecutionEnvironment.get_execution_environment()
        self.stream_env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.stream_env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
        self.stream_table_env = StreamTableEnvironment.create(self.stream_env)
