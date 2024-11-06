import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


def create_keyspace(session) -> None:

    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS spark_streams_cassandra
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session) -> None:
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams_cassandra.created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs) -> None:
    print("Inserting data starting...")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("post_code")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("reg_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute("""
        INSERT INTO spark_streams_cassandra.created_users(
        id, first_name, last_name, gender, address, 
        post_code, email, username, dob, registered_date, phone, picture
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
        postcode, email, username, dob, registered_date, phone, picture))

        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
        .appName("SparkDataStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "localhost") \
        .getOrCreate()


        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "creating_user_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():

    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
    StructField("id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("post_code", StringType(), False),
    StructField("email", StringType(), False),
    StructField("username", StringType(), False),
    StructField("registered_date", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("picture", StringType(), False)
    ])

    df_spark = spark_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

    return df_spark


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()
    if session is not None:
        create_keyspace(session)
        create_table(session)
        logging.info("Streaming is being started...")

        streaming_query = (
            selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_streams_cassandra")
            .option("table", "created_users")
            .start()
        )

        streaming_query.awaitTermination()