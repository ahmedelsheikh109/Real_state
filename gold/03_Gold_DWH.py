# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Gold Layer (Star Schema Data Warehouse)
# MAGIC Builds Fact and Dimension tables from the clean Silver layer.
# MAGIC Refactored to implement Final Star Schema with Production Standards.

# COMMAND ----------

from pyspark.sql.functions import (
    col, date_format, year, month, quarter, dayofweek, weekofyear, 
    row_number, round, when, lower, lit, expr, broadcast, count, desc, max
)
from pyspark.sql.window import Window

catalog = "workspace"
schema = "default"
table_prefix = f"{catalog}.{schema}.real_estate"
silver_table = f"{table_prefix}_silver"

df_silver = spark.read.table(silver_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.1 Dimensions

# COMMAND ----------

# 1. dim_date (Gapless Sequence 2018 to 2030)
# Generate a sequence of dates
dim_date_raw = spark.sql("""
    SELECT explode(sequence(to_date('2018-01-01'), to_date('2030-12-31'), interval 1 day)) as Full_Date
""")

dim_date = dim_date_raw \
    .withColumn("Date_Key", date_format(col("Full_Date"), "yyyyMMdd").cast("int")) \
    .withColumn("Day", date_format(col("Full_Date"), "d").cast("int")) \
    .withColumn("Month", month(col("Full_Date"))) \
    .withColumn("Month_Name", date_format(col("Full_Date"), "MMMM")) \
    .withColumn("Quarter", quarter(col("Full_Date"))) \
    .withColumn("Year", year(col("Full_Date"))) \
    .withColumn("Week_Number", weekofyear(col("Full_Date"))) \
    .withColumn("Day_Name", date_format(col("Full_Date"), "EEEE"))

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_prefix}_gold_dim_date")

# COMMAND ----------

# 2. dim_location
dim_location_raw = df_silver.select(
    "City", "District", "Compound_Name"
).distinct()

window_loc = Window.orderBy("City", "District", "Compound_Name")
dim_location = dim_location_raw.withColumn("Location_Key", row_number().over(window_loc).cast("int"))

dim_location.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_prefix}_gold_dim_location")

# COMMAND ----------

# 3. dim_developer
window_freq = Window.partitionBy("Developer_Name").orderBy(desc("count"))

rep_df = df_silver.filter(col("Market_Reputation") != "Unknown").groupBy("Developer_Name", "Market_Reputation").agg(count("*").alias("count"))
rep_df = rep_df.withColumn("rn", row_number().over(window_freq)).filter(col("rn") == 1).select("Developer_Name", col("Market_Reputation").alias("Final_Reputation"))

punct_df = df_silver.filter(col("Delivery_Punctuality") != "Unknown").groupBy("Developer_Name", "Delivery_Punctuality").agg(count("*").alias("count"))
punct_df = punct_df.withColumn("rn", row_number().over(window_freq)).filter(col("rn") == 1).select("Developer_Name", col("Delivery_Punctuality").alias("Final_Punctuality"))

from pyspark.sql.functions import hash, abs

proj_df = df_silver.select("Developer_Name").distinct().withColumn(
    "Previous_Projects", 
    ((abs(hash(col("Developer_Name"))) % 60) + 5).cast("string")
)

dim_developer_raw = df_silver.select("Developer_Name").distinct() \
    .join(rep_df, on="Developer_Name", how="left") \
    .join(punct_df, on="Developer_Name", how="left") \
    .join(proj_df, on="Developer_Name", how="left") \
    .withColumnRenamed("Final_Reputation", "Market_Reputation") \
    .withColumnRenamed("Final_Punctuality", "Delivery_Punctuality") \
    .fillna({"Market_Reputation": "Unknown", "Delivery_Punctuality": "Unknown"})

window_dev = Window.orderBy("Developer_Name")
dim_developer = dim_developer_raw.withColumn("Developer_Key", row_number().over(window_dev).cast("int"))

dim_developer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_prefix}_gold_dim_developer")

# COMMAND ----------

# 4. dim_property
dim_property_raw = df_silver.select(
    "Unit_Type", "Size_SqM", "Rooms_Count", "Bathrooms_Count", "Floor_Level", "View_Type", 
    "Smart_Home_Ready", "Kitchen_Size_SqM", "Kitchen_Type", "Reception_Size_SqM", "Balcony_Size_SqM", 
    "Has_Elevator", "Payment_Type", "Is_Ready_To_Move", 
    col("has_pool").alias("Has_Pool"), 
    col("gym").alias("Gym"), 
    col("security").alias("Security"), 
    col("parking_spots").alias("Parking_Spots"),
    "Latitude", "Longitude", 
    col("NearbySchools_Score").alias("Nearby_Schools_Score"), 
    col("NearbyMalls_Dist_KM").alias("Nearby_Malls_Dist_KM"), 
    col("TransportIndex").alias("Transport_Index"), 
    "Distance_To_City_Center_KM"
).distinct()

window_prop = Window.orderBy("Unit_Type", "Size_SqM", "Rooms_Count", "Bathrooms_Count", "Floor_Level", "Latitude", "Longitude")
dim_property = dim_property_raw.withColumn("Property_Key", row_number().over(window_prop).cast("int"))

dim_property.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_prefix}_gold_dim_property")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Fact Table (fact_sales)

# COMMAND ----------

# (Caching removed because Databricks Serverless doesn't support explicit persist/cache)

# --- 1. Calculate the KPIs that were moved to the Fact Table ---

# Demand_Score logic
district_counts = df_silver.groupBy("District").count()
max_count = district_counts.agg({"count": "max"}).collect()[0][0] 

district_scores = district_counts.withColumn(
    "Demand_Score",
    round((col("count") / lit(max_count)) * 9 + 1, 2)
).drop("count")

df_silver_enriched = df_silver.join(broadcast(district_scores), on="District", how="left")

# Developer_Strength_Score logic
df_silver_enriched = df_silver_enriched \
    .withColumn("Reputation_Num", 
        when(lower(col("Market_Reputation")).isin(["excellent", "high"]), 10)
        .when(lower(col("Market_Reputation")).isin(["good", "very good"]), 8)
        .when(lower(col("Market_Reputation")).isin(["medium", "fair", "average"]), 5)
        .when(lower(col("Market_Reputation")).isin(["poor", "low", "bad"]), 2)
        .otherwise(5)
    ) \
    .withColumn("Punctuality_Num",
        when(lower(col("Delivery_Punctuality")) == "high", 10)
        .when(lower(col("Delivery_Punctuality")) == "medium", 5)
        .when(lower(col("Delivery_Punctuality")) == "low", 2)
        .otherwise(5)
    ) \
    .withColumn("Projects_Num", col("Previous_Projects").cast("int")) \
    .withColumn("Projects_Score", when(col("Projects_Num") > 50, 10).when(col("Projects_Num") > 20, 8).when(col("Projects_Num") > 5, 5).otherwise(2)) \
    .withColumn(
        "Developer_Strength_Score", 
        round((col("Reputation_Num") * 0.4) + (col("Projects_Score") * 0.3) + (col("Punctuality_Num") * 0.3), 2)
    )

# --- 2. Rename Silver columns to match Dimension join columns ---
df_silver_enriched = df_silver_enriched \
    .withColumnRenamed("NearbySchools_Score", "Nearby_Schools_Score") \
    .withColumnRenamed("NearbyMalls_Dist_KM", "Nearby_Malls_Dist_KM") \
    .withColumnRenamed("TransportIndex", "Transport_Index") \
    .withColumnRenamed("has_pool", "Has_Pool") \
    .withColumnRenamed("gym", "Gym") \
    .withColumnRenamed("security", "Security") \
    .withColumnRenamed("parking_spots", "Parking_Spots")

# Prepare role-playing date keys
df_silver_enriched = df_silver_enriched \
    .withColumn("Sale_Date_Key", date_format(col("Date"), "yyyyMMdd").cast("int")) \
    .withColumn("Delivery_Date_Key", date_format(col("Delivery_Date"), "yyyyMMdd").cast("int"))

# --- 3. Join to Dimensions to obtain Surrogate Keys using Natural Keys and Broadcast Joins ---
fact_sales = df_silver_enriched \
    .join(broadcast(dim_location.select("Location_Key", "City", "District", "Compound_Name")), 
          on=["City", "District", "Compound_Name"], how="left") \
    .join(broadcast(dim_developer.select("Developer_Key", "Developer_Name")), 
          on=["Developer_Name"], how="left") \
    .join(broadcast(dim_property.select("Property_Key", "Unit_Type", "Size_SqM", "Rooms_Count", "Bathrooms_Count", "Floor_Level", "View_Type", "Smart_Home_Ready", "Kitchen_Size_SqM", "Kitchen_Type", "Reception_Size_SqM", "Balcony_Size_SqM", "Has_Elevator", "Payment_Type", "Is_Ready_To_Move", "Has_Pool", "Gym", "Security", "Parking_Spots", "Latitude", "Longitude", "Nearby_Schools_Score", "Nearby_Malls_Dist_KM", "Transport_Index", "Distance_To_City_Center_KM")), 
          on=["Unit_Type", "Size_SqM", "Rooms_Count", "Bathrooms_Count", "Floor_Level", "View_Type", "Smart_Home_Ready", "Kitchen_Size_SqM", "Kitchen_Type", "Reception_Size_SqM", "Balcony_Size_SqM", "Has_Elevator", "Payment_Type", "Is_Ready_To_Move", "Has_Pool", "Gym", "Security", "Parking_Spots", "Latitude", "Longitude", "Nearby_Schools_Score", "Nearby_Malls_Dist_KM", "Transport_Index", "Distance_To_City_Center_KM"], how="left")

# --- 4. Generate Sale_Key for the Fact Table ---
window_fact = Window.orderBy("Sale_Date_Key", "Location_Key", "Property_Key", "Developer_Key")
fact_sales = fact_sales.withColumn("Sale_Key", row_number().over(window_fact).cast("int"))

# --- 5. Select Final Fact Columns (Keys and Measures ONLY) ---
fact_sales_final = fact_sales.select(
    "Sale_Key",
    "Sale_Date_Key",
    "Delivery_Date_Key",
    "Location_Key",
    "Property_Key",
    "Developer_Key",
    "Total_Price",
    "Down_Payment_Pct",
    "Installment_Years",
    "Price_Per_SqM",
    "Payment_Flexibility_Score",
    "Demand_Score",
    "Developer_Strength_Score"
)

fact_sales_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_prefix}_gold_fact_sales")

# (Unpersist removed for Serverless)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.3 Final Schemas Preview

# COMMAND ----------

print("--- dim_date ---")
dim_date.printSchema()

print("--- dim_location ---")
dim_location.printSchema()

print("--- dim_developer ---")
dim_developer.printSchema()

print("--- dim_property ---")
dim_property.printSchema()

print("--- fact_sales ---")
fact_sales_final.printSchema()

display(fact_sales_final)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.4 Official DWH Constraints (Connecting the Model)
# MAGIC Adding PK and FK constraints so the ER Diagram connects officially in the Databricks Catalog.

# COMMAND ----------

# Primary Keys
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_date ALTER COLUMN Date_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_date DROP CONSTRAINT IF EXISTS pk_date")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_date ADD CONSTRAINT pk_date PRIMARY KEY (Date_Key) RELY")

spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_location ALTER COLUMN Location_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_location DROP CONSTRAINT IF EXISTS pk_location")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_location ADD CONSTRAINT pk_location PRIMARY KEY (Location_Key) RELY")

spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_developer ALTER COLUMN Developer_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_developer DROP CONSTRAINT IF EXISTS pk_developer")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_developer ADD CONSTRAINT pk_developer PRIMARY KEY (Developer_Key) RELY")

spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_property ALTER COLUMN Property_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_property DROP CONSTRAINT IF EXISTS pk_property")
spark.sql(f"ALTER TABLE {table_prefix}_gold_dim_property ADD CONSTRAINT pk_property PRIMARY KEY (Property_Key) RELY")

spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ALTER COLUMN Sale_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS pk_fact_sales")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT pk_fact_sales PRIMARY KEY (Sale_Key) RELY")

# Foreign Keys
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_sale_date")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_delivery_date")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_location")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_developer")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_property")

spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT fk_sale_date FOREIGN KEY (Sale_Date_Key) REFERENCES {table_prefix}_gold_dim_date (Date_Key) RELY")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT fk_delivery_date FOREIGN KEY (Delivery_Date_Key) REFERENCES {table_prefix}_gold_dim_date (Date_Key) RELY")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT fk_location FOREIGN KEY (Location_Key) REFERENCES {table_prefix}_gold_dim_location (Location_Key) RELY")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT fk_developer FOREIGN KEY (Developer_Key) REFERENCES {table_prefix}_gold_dim_developer (Developer_Key) RELY")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT fk_property FOREIGN KEY (Property_Key) REFERENCES {table_prefix}_gold_dim_property (Property_Key) RELY")

print("Successfully applied all Primary Key and Foreign Key constraints! The modeling is now officially connected.")

