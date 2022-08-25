import pickle
import datetime

from pyflink.common import Row
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings, AggregateFunction
from pyflink.table.udf import udaf


class ECODSync(AggregateFunction):
    def create_accumulator(self):
        # Row(sum, count)
        return Row([], [], [])

    def get_value(self, accumulator):
        import numpy as np
        if accumulator[0]:
            last_id = max(accumulator[0])
            last_ts = max(accumulator[1])
            x_train = np.vstack(accumulator[2]).astype(np.float64)
            from pyod.models.ecod import ECOD
            model = ECOD(n_jobs=1, contamination=0.0001).fit(x_train)
            return Row(id=last_id,
                       model_byte=pickle.dumps(model),
                       ts=last_ts)

    def accumulate(self, accumulator, *args):
        for i in range(len(args)):
            accumulator[i].append(args[i])

    def retract(self, accumulator, *args):
        for i in range(len(args)):
            accumulator[i].remove(args[i])

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            for i in range(len(acc)):
                accumulator[i].extend(acc[i])

    def get_result_type(self):
        return DataTypes.ROW(row_fields=[
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("model_byte", DataTypes.BYTES())
        ])


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "2")
    t_env.get_config().set(
        "python.fn-execution.bundle.size", "1")
    t = t_env.from_elements([(1, datetime.datetime(1999, 9, 10, 5, 20, 10), 1.0),
                             (3, datetime.datetime(1999, 9, 10, 5, 20, 10), 2.0), ],
                            ['a', 'b', 'c'])

    my_ecod_sync = udaf(ECODSync(),
                        accumulator_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                                        DataTypes.FIELD("b",
                                                                        DataTypes.TIMESTAMP(3)),
                                                        DataTypes.FIELD("c", DataTypes.FLOAT())]))
    result = t.select(my_ecod_sync(t.a, t.b, t.c))
    print(result.to_pandas())


if __name__ == '__main__':
    main()
