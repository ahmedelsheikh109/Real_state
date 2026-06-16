from pyspark.sql.functions import current_timestamp, input_file_name

catalog = "workspace"
schema = "default"
volume = "raw_real_estate"

source_path = f"/Volumes/{catalog}/{schema}/{volume}/"

schema_path = f"/Volumes/{catalog}/{schema}/{volume}_checkpoints/schema/"

checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}_checkpoints/bronze_cp/"

bronze_table = f"{catalog}.{schema}.real_estate_bronze"

print(f"Reading new files from: {source_path}")
print(f"Writing to Bronze Table: {bronze_table}")

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)

        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("source_file_path", input_file_name())

        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
)

print("Incremental Load Finished Successfully!")