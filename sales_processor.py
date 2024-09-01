import os
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

#that's from claude

def main():
    # flink_connector_kafka_name = 'flink-sql-connector-kafka-3.2.0-1.19.jar'
    # kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), flink_connector_kafka_name)

    # if os.path.isfile(kafka_jar):
    #     print(f"File exists: {kafka_jar}")
    # else:
    #     print(f"File doesnt exist: {kafka_jar}")
    #     return

    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings
    )

    flink_connector_kafka_name = 'flink-sql-connector-kafka-3.2.0-1.19.jar'
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), flink_connector_kafka_name)
    kafka_jar = kafka_jar.replace(' ', '%20') # spaces - for -  %20
    #kafka_jar = 'C:/Users/HamkorLab_32/Desktop/py projects/BLOG-DEMO new/flink-sql-connector-kafka-3.2.0-1.19.jar'
    jar_url = f"file:///{kafka_jar}"
    tbl_env.get_config().set("pipeline.jars", jar_url)
    tbl_env.get_config().set("pipeline.classpaths", jar_url)

    # Create Kafka Source Table with DDL
    src_ddl = """
        CREATE TABLE sales_usd (
            seller_id STRING,
            amount_usd DOUBLE,
            sale_ts BIGINT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-usd',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sales-usd',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    # 
    # earliest-offset    

    tbl_env.execute_sql(src_ddl)

    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    sql = """
        SELECT
          seller_id,
          TUMBLE_END(proctime, INTERVAL '5' SECONDS) AS window_end,
          SUM(amount_usd) * 0.85 AS window_sales
        FROM sales_usd
        GROUP BY
          TUMBLE(proctime, INTERVAL '5' SECONDS),
          seller_id
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    # Create Kafka Sink Table
    sink_ddl = """
        CREATE TABLE sales_euros (
            seller_id STRING,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # revenue_tbl.execute_insert('sales_euros').wait()
    # result = tbl_env.sql_query("SELECT * FROM sales_euros")
    # result.execute().print()
    # env.execute('windowed-sales-euros')
    
    print("\nРезультаты обработки:")
    revenue_tbl.execute().print()
    revenue_tbl.execute_insert('sales_euros').wait()
    env.execute("Windowed Sales")

if __name__ == '__main__':
    main()