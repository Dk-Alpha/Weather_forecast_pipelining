# weather_stream_to_viz.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataTransformer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema based on 7Timer API structure
schema = StructType([
    StructField("timepoint", IntegerType()),
    StructField("cloudcover", IntegerType()),
    StructField("lifted_index", IntegerType()),
    StructField("prec_type", StringType()),
    StructField("prec_amount", IntegerType()),
    StructField("temp2m", IntegerType()),
    StructField("rh2m", StringType()),
    StructField("wind10m", StructType([
        StructField("direction", StringType()),
        StructField("speed", IntegerType())
    ])),
    StructField("weather", StringType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ]))
])

# Read streaming data from Kafka topic "weather_raw"
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert value (bytes) to string and parse JSON
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*")

# Add init and forecast time
json_df = json_df.withColumn("init_time", current_timestamp())
# Divide by 1 to make sure it's numeric
json_df = json_df.withColumn("forecast_time", col("init_time") + expr("INTERVAL 1 HOUR") * col("timepoint"))

# Extract required fields for visualization
viz_df = json_df.select(
    col("forecast_time"),
    col("temp2m").alias("temperature"),
    col("prec_type").alias("precipitation"),
    col("cloudcover"),
    col("lifted_index"),
    col("weather"),
    col("wind10m.direction").alias("wind_dir"),
    col("wind10m.speed").alias("wind_speed"),
    col("location.lat"),
    col("location.lon"),
    col("rh2m").alias("humidity")
)

# Repack into JSON format for Kafka sink
viz_output = viz_df.withColumn("value", to_json(struct(*viz_df.columns))) \
                   .selectExpr("CAST(value AS STRING)")

# Write the processed data to Kafka topic "weather_viz"
query = viz_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "weather_viz") \
    .option("checkpointLocation", "checkpoints/weather_viz") \
    .outputMode("append") \
    .start()

query.awaitTermination()