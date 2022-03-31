from abc import ABC

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, CheckpointingMode
from pyflink.table import StreamTableEnvironment

from python.test_case_utils import BenchmarkTestBase


class PyFlinkTests(BenchmarkTestBase, ABC):
    def __init__(self):
        self.batch_env = StreamExecutionEnvironment.get_execution_environment()
        self.batch_env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.batch_env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
        self.batch_env.disable_operator_chaining()
        self.batch_table_env = StreamTableEnvironment.create(self.batch_env)

        self.stream_env = StreamExecutionEnvironment.get_execution_environment()
        self.stream_env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.stream_env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
        self.stream_env.disable_operator_chaining()
        self.stream_table_env = StreamTableEnvironment.create(self.stream_env)
