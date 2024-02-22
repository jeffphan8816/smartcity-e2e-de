from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

from config import configuration


def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration['AWS_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration['AWS_SECRET_KEY']) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Adjust LOG level to minimize the amount of information printed to the console
    spark.sparkContext.setLogLevel("WARN")

    # vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("fuelType", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("deviceId", StringType(), True),
    ])

    # gps schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("deviceId", StringType(), True),
    ])

    # traffic camera schema
    camera_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
    ])

    # weather schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    # emergency schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("incidentType", StringType(), True),
        StructField("description", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("status", StringType(), True),
        StructField("incidentId", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    def streamWriter(df: DataFrame, checkpointFolder, output):
        return df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output) \
            .option("checkpointLocation", checkpointFolder) \
            .start()

    # Read data from Kafka Topics
    vehicle_df = read_kafka_topic("vehicle_topic", vehicle_schema).alias("vehicle")
    gps_df = read_kafka_topic("gps_topic", gps_schema).alias("gps")
    camera_df = read_kafka_topic("traffic_topic", camera_schema).alias("camera")
    weather_df = read_kafka_topic("weather_topic", weather_schema).alias("weather")
    emergency_df = read_kafka_topic("emergency_topic", emergency_schema).alias("emergency")

    # Join the dataframes
    query1 = streamWriter(vehicle_df, "s3a://phatphan2004spark-streaming-data/checkpoints/vehicle_data",
                          "s3a://phatphan2004spark-streaming-data/data/vehicle_data")
    query2 = streamWriter(gps_df, "s3a://phatphan2004spark-streaming-data/checkpoints/gps_data",
                          "s3a://phatphan2004spark-streaming-data/data/gps_data")
    query3 = streamWriter(camera_df, "s3a://phatphan2004spark-streaming-data/checkpoints/camera_data",
                          "s3a://phatphan2004spark-streaming-data/data/camera_data")
    query4 = streamWriter(weather_df, "s3a://phatphan2004spark-streaming-data/checkpoints/weather_data",
                          "s3a://phatphan2004spark-streaming-data/data/weather_data")
    query5 = streamWriter(emergency_df, "s3a://phatphan2004spark-streaming-data/checkpoints/emergency_data",
                          "s3a://phatphan2004spark-streaming-data/data/emergency_data")
    query5.awaitTermination()


if __name__ == "__main__":
    main()
