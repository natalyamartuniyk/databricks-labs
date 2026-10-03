from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

RAW_VOLUME_PATH = spark.conf.get("youtube.raw_volume_path")
SCHEMA_LOCATION = f"{RAW_VOLUME_PATH}/_schemas/videos_files"

@dp.table(
    name = "youtube_videos_files_bronze",
    comment = "Raw YouTube trending video data from the raw_data volume, ingested incrementally via Auto Loader",
    table_properties={
    "pipelines.reset.allowed": "false",
    "delta.appendOnly": "true",
},
)
def youtube_videos_files_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", "*.csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .load(RAW_VOLUME_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
            
    )