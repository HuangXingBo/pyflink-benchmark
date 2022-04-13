import os
import time
from abc import abstractmethod, ABC
from enum import Enum

from python.flink.table.functions.udf_analyzer import loads_functions
from python.flink.test_flink_base import PyFlinkTests


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


class TableTests(object):

    def test_compute_100_bytes_json_op(self):
        self.compute_json_op(20000000, 100)

    def test_compute_1000bytes_json_op(self):
        self.compute_json_op(5000000, 1 << 10)

    def test_compute_1000_0bytes_json_op(self):
        self.compute_json_op(500000, 10 * (1 << 10))

    def test_compute_1000_00bytes_json_op(self):
        self.compute_json_op(300000, 100 * (1 << 10))

    def test_compute_100_bytes_string_upper(self):
        self.compute_string_upper(20000000, 100)

    def test_compute_1000bytes_string_upper(self):
        self.compute_string_upper(5000000, 1 << 10)

    def test_compute_1000_0bytes_string_upper(self):
        self.compute_string_upper(500000, 10 * (1 << 10))

    def test_compute_1000_00bytes_string_upper(self):
        self.compute_string_upper(300000, 100 * (1 << 10))

    # TODO: add more tests


class PyFlinkTableTests(PyFlinkTests, ABC):
    def __init__(self):
        super(PyFlinkTableTests, self).__init__()
        self.t_env = self.get_underlying_table_env()
        self.initialize()
        self.config_env()

    def create_source_sink(self, num_rows: int, field_size: int,
                           qps: QPS = QPS.QPS_10_W):
        self.t_env.execute_sql(f"""
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

        self.t_env.execute_sql(f"""
            CREATE TABLE sink_table (
                id STRING
            ) WITH (
              'connector' = 'blackhole'
            )
        """)

    def initialize(self):
        def register_function(functions):
            for name, f in functions:
                self.t_env.create_temporary_system_function("python_" + name, f)

        current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
        java_target_dir = os.path.abspath(current_dir + "/../../../java/target")
        self.t_env.get_config().get_configuration().set_string(
            "pipeline.jars",
            "file://{0}/benchmark-0.1.jar".format(java_target_dir))
        self.t_env.get_config().get_configuration().set_string("parallelism.default", "1")

        udfs, udtfs, udafs, udtafs = loads_functions()

        if udfs:
            register_function(udfs)

        if udtfs:
            register_function(udtfs)

        if udafs:
            register_function(udafs)

        if udtafs:
            register_function(udtafs)

    def compute_json_op(self, num_rows: int, field_size: int):
        self.create_source_sink(num_rows, field_size, QPS.QPS_500_W)
        table_result = self.t_env.from_path('source_table') \
            .select("{}(id)".format(self.get_function_name("json"))) \
            .execute_insert('sink_table')
        beg_time = time.time()
        table_result.wait()
        print("{0} computes {1} bytes json operate QPS is {2} ".format(
            self,
            field_size,
            num_rows / (time.time() - beg_time)))

    def compute_string_upper(self, num_rows: int, field_size: int):
        self.create_source_sink(num_rows, field_size, QPS.QPS_500_W)
        table_result = self.t_env.from_path('source_table') \
            .select("{}(id)".format(self.get_function_name("upper"))) \
            .execute_insert('sink_table')
        beg_time = time.time()
        table_result.wait()
        print("{0} computes {1} bytes string upper QPS is {2} ".format(
            self,
            field_size,
            num_rows / (time.time() - beg_time)))

    @abstractmethod
    def get_underlying_table_env(self):
        pass

    @abstractmethod
    def config_env(self):
        pass

    @abstractmethod
    def get_function_name(self, name: str) -> str:
        pass


class PyFlinkTablePythonTests(PyFlinkTableTests, ABC):
    mapping = {
        'upper': 'python_upper',
        'json': 'python_json_value_lower'
    }

    def get_function_name(self, name: str) -> str:
        return PyFlinkTablePythonTests.mapping[name]


class PyFlinkTableJavaTests(PyFlinkTableTests, ABC):
    mapping = {
        'upper': 'java_StringUpper',
        'json': 'java_JsonValueLower'
    }

    def config_env(self):
        # TODO: auto scan java udf in jar.
        self.t_env.create_java_temporary_system_function(
            "java_StringUpper", "com.pyflink.benchmark.function.StringUpper")
        self.t_env.create_java_temporary_system_function(
            "java_JsonValueLower", "com.pyflink.benchmark.function.JsonValueLower")
        self.t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)

    def get_function_name(self, name: str) -> str:
        return PyFlinkTableJavaTests.mapping[name]


class PyFlinkTableProcessModeTests(PyFlinkTablePythonTests, ABC):
    def config_env(self):
        self.t_env.get_config().get_configuration().set_string(
            "python.execution-mode", "process")


class PyFlinkTableThreadModeTests(PyFlinkTablePythonTests, ABC):
    def config_env(self):
        self.t_env.get_config().get_configuration().set_string(
            "python.execution-mode", "thread")


class PyFlinkStreamTableProcessModeTests(TableTests, PyFlinkTableProcessModeTests):
    def get_underlying_table_env(self):
        return self.stream_table_env

    def __str__(self):
        return "Stream Python UDF in Process Mode"


class PyFlinkBatchTableProcessModeTests(TableTests, PyFlinkTableProcessModeTests):
    def get_underlying_table_env(self):
        return self.batch_table_env

    def __str__(self):
        return "Batch Python UDF in Process Mode"


class PyFlinkStreamTableThreadModeTests(TableTests, PyFlinkTableThreadModeTests):
    def get_underlying_table_env(self):
        return self.stream_table_env

    def __str__(self):
        return "Stream Python UDF in Thread Mode"


class PyFlinkBatchTableThreadModeTests(TableTests, PyFlinkTableThreadModeTests):
    def get_underlying_table_env(self):
        return self.batch_table_env

    def __str__(self):
        return "Batch Python UDF in Thread Mode"


class PyFlinkStreamTableJavaTests(TableTests, PyFlinkTableJavaTests):
    def get_underlying_table_env(self):
        return self.stream_table_env

    def __str__(self):
        return "Stream Java UDF"


class PyFlinkBatchTableJavaTests(TableTests, PyFlinkTableJavaTests):
    def get_underlying_table_env(self):
        return self.batch_table_env

    def __str__(self):
        return "Batch Java UDF"
