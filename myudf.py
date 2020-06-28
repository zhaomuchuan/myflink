from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.udf import udf
import numpy as np

from sklearn.linear_model import LinearRegression as LR
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import train_test_split


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)


@udf(input_types=[DataTypes.DECIMAL(38,12,nullable=True)], result_type=DataTypes.DECIMAL(38,12,nullable=True))
def myadd(i):
    return i*i*2


# add = udf(myadd, [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

t_env.register_function("add", myadd)

t_env.connect(FileSystem().path('/tmp/input')) \
    .with_format(OldCsv()
                 .field('b', DataTypes.DECIMAL(38,12,nullable=True))) \
    .with_schema(Schema()
                 .field('b', DataTypes.DECIMAL(38,12,nullable=True))) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field('sum', DataTypes.DECIMAL(38,12,nullable=True))) \
    .with_schema(Schema()
                 .field('sum', DataTypes.DECIMAL(38,12,nullable=True))) \
    .create_temporary_table('mySink')

t_env.from_path('mySource')\
    .select("add(b)") \
    .insert_into('mySink')

t_env.execute("tutorial_job")