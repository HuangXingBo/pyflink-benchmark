import pkgutil
import importlib

from pyflink.table.udf import UserDefinedScalarFunctionWrapper, UserDefinedTableFunctionWrapper, \
    UserDefinedAggregateFunctionWrapper

from python.flink.table import functions


def loads_scalar_functions(module):
    return [(name, obj) for name, obj in module.__dict__.items()
            if isinstance(obj, UserDefinedScalarFunctionWrapper)]


def loads_table_functions(module):
    return [(name, obj) for name, obj in module.__dict__.items()
            if isinstance(obj, UserDefinedTableFunctionWrapper)]


def loads_aggregate_functions(module):
    return [(name, obj) for name, obj in module.__dict__.items()
            if isinstance(obj, UserDefinedAggregateFunctionWrapper)
            and not (hasattr(obj, '_is_table_aggregate') and obj._is_table_aggregate)]


def loads_table_aggregate_functions(module):
    return [(name, obj) for name, obj in module.__dict__.items()
            if isinstance(obj, UserDefinedAggregateFunctionWrapper)
            and hasattr(obj, '_is_table_aggregate') and obj._is_table_aggregate]


def loads_functions():
    package_name = "python.flink.table.functions"
    modules = [importlib.import_module('.'.join([package_name, file_name]))
               for _, file_name, _ in pkgutil.walk_packages(functions.__path__)]

    udfs = sum([loads_scalar_functions(module) for module in modules], [])
    udtfs = sum([loads_table_functions(module) for module in modules], [])
    udafs = sum([loads_aggregate_functions(module) for module in modules], [])
    udtafs = sum([loads_table_aggregate_functions(module) for module in modules], [])

    return udfs, udtfs, udafs, udtafs


if __name__ == '__main__':
    print(loads_functions())
