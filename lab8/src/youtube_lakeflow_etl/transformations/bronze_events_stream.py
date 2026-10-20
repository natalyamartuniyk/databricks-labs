from pyspark import pipelines as dp 
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

EH_NAMESPACE = spark.conf.get("eventhub.namespace")
EH_NAME = spark.conf.get("eventhub.name")
EH_SECRET_SCOPE = spark.conf.get("eventhub.secret_scope")
EH_SECRET_KEY = spark.conf.get("eventhub.secret_key")
EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("watch_duration_sec", IntegerType(), True),
    StructField("event_timestamp", StringType(), True)
])

def _eventhub_sasl_config():
    conn_str = dbutils.secrets.get(scope=EH_SECRET_SCOPE, key=EH_SECRET_KEY)
    return (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )

@dp.table(
    name = "youtube_events_stream_bronze",
    comment="Raw YouTube watch events ingested from Event Hub via the Kafka-compatible endpoint",
    table_properties = {
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    }
)

def youtube_events_stream_bronze():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
        .option("subscribe", EH_NAME)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", _eventhub_sasl_config())
        .option("kafka.group.id", "$Default")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
            col("offset").alias("_eventhub_offset"),
            col("partition").alias("_eventhub_partition"),
            col("timestamp").alias("_eventhub_enqueued_at"),
        )
        .select("data.*", "_eventhub_offset", "_eventhub_partition", "_eventhub_enqueued_at")
        .withColumn("_ingested_at", current_timestamp())
    )