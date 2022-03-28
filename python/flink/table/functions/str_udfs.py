from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING(), func_type="general")
def upper(s):
    return s.upper()


@udf(result_type=DataTypes.STRING(), func_type="general")
def json_value_lower(s: str):
    import json
    a = json.loads(s)
    a['a'] = a['a'].lower()
    return json.dumps(a)
