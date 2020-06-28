import os, time

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, FileSystem
from pyflink.table.window import Tumble
from pyflink.table.udf import udf


if __name__ == '__main__':

    # stream setting
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)

    # csv path for csv sink
    result_file = "/tmp/tumble_time_window_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)

    # udf
    @udf(input_types=[DataTypes.DECIMAL(38, 12, nullable=True)], result_type=DataTypes.DECIMAL(38, 12, nullable=True))
    def myadd(i):
        return i * i * 2
    st_env.register_function("add", myadd)

    # way kafka
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("universal")
            .topic("user")
            # .start_from_earliest()
            # .start_from_earliest()
            .start_from_specific_offset(0,496)
            .property("zookeeper.connect", "6.86.2.170:2181")
            .property("bootstrap.servers", "6.86.2.170:9092")
    ) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .json_schema(
            "{"
            "  type: 'object',"
            "  properties: {"
            "    a: {"
            "      type: 'string'"
            "    },"
            "    b: {"
            "      type: 'number'"
            "    },"
            "    c: {"
            "      type: 'string'"
            "    },"
            "    time: {"
            "      type: 'string',"
            "      format: 'date-time'"
            "    }"
            "  }"
            "}"
        )
    ) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("rowtime", DataTypes.TIMESTAMP())
            .rowtime(
            Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(60000))
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.DECIMAL(38,12,nullable=True))
            .field("c", DataTypes.STRING())
    ) \
        .in_append_mode() \
        .register_table_source("source")

    # way sink csv
    # st_env.register_table_sink("result_tab",
    #                            CsvTableSink(["a", "count"],
    #                                         [DataTypes.STRING(),
    #                                          DataTypes.DECIMAL(38, 12, nullable=True)],
    #                                         result_file))

    # way of mysql
    mysql_sink_ddl = """
    CREATE TABLE temp_statistic (
     item VARCHAR,
     cal DECIMAL
    ) WITH (
     'connector.type' = 'jdbc',
     'connector.url' = 'jdbc:mysql://6.86.2.170:3306/Flink',
     'connector.table' = 'temp_statistic',
     'connector.username' = 'root',
     'connector.password' = '801105',
     'connector.write.flush.interval' = '1s'
    )
    """

    st_env.sql_update(mysql_sink_ddl)

    # way of kafka sink
    kafka_sink_ddl = """
    CREATE TABLE kafka_sink (
     item VARCHAR,
     cal DECIMAL
    ) WITH (
     'connector.type' = 'kafka',
     'connector.version' = 'universal',
     'connector.topic' = 'access_log',
     'connector.properties.zookeeper.connect' = '6.86.2.170:2181',
     'connector.properties.bootstrap.servers' = '6.86.2.170:9092',
     'format.type' = 'json'
    )
    """

    st_env.sql_update(kafka_sink_ddl)

    # calculate total
    # st_env.scan("source").group_by("a").select("a, sum(b)").insert_into("temp_statistic")

    # cal window
    st_env.scan("source").window(Tumble.over("10.minutes").on("rowtime").alias("w")) \
        .select("a, sum(b)").insert_into("kafka_sink")

    # cal with udf
    # st_env.scan("source").select("a, add(b)").insert_into("kafka_sink")

    # let's go
    st_env.execute("tumble time window streaming")