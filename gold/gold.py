from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid
from datetime import datetime

# ==========================================
#(Configurations)
# ==========================================
catalog = "workspace"
schema = "default"
table_prefix = f"{catalog}.{schema}.real_estate"

silver_table = f"{table_prefix}_silver"
control_table = f"{catalog}.{schema}.gold_processing_control"

#Data Model (Star Schema)
dim_developer_table = f"{table_prefix}_dim_developer"
dim_location_table = f"{table_prefix}_dim_location"
dim_profile_table = f"{table_prefix}_dim_profile"
fact_table = f"{table_prefix}_fact_listings"

gold_run_id = str(uuid.uuid4())
PRIMARY_KEY = "Property_ID" 

# ==========================================
#(Incremental Fetch)
# ==========================================
def get_last_processed_ts():
    if spark.catalog.tableExists(control_table):
        try:
            return spark.table(control_table).filter(col("entity_name") == "real_estate").select(max("last_silver_update_ts")).collect()[0][0]
        except:
            return None
    return None

last_processed_ts = get_last_processed_ts()
df_silver = spark.read.table(silver_table)


incremental_col = "silver_run_id" 

if last_processed_ts:
    df_new = df_silver.filter(col(incremental_col) > lit(last_processed_ts))
else:
    df_new = df_silver

new_records_count = df_new.count()
print(f"Found {new_records_count} new records to process in Gold.")

if new_records_count == 0:
    print("No new data to process. Exiting.")
    dbutils.notebook.exit("Success")


current_gold_ts = current_timestamp()
df_new = df_new.withColumn("gold_update_ts", current_gold_ts)

# ==========================================
# [Dim_Developer] using SCD Type 2 
# ==========================================
print("Processing Dim_Developer (SCD2)...")
df_dev_delta = df_new.select(
    "Developer_Name", 
    "Market_Reputation", 
    "Delivery_Punctuality",
    "gold_update_ts"
).distinct()

if not spark.catalog.tableExists(dim_developer_table):
    spark.sql(f"""
        CREATE TABLE {dim_developer_table} (
            developer_sk STRING,
            Developer_Name STRING,
            Market_Reputation STRING,
            Delivery_Punctuality STRING,
            valid_from_ts TIMESTAMP,
            valid_to_ts TIMESTAMP,
            is_current BOOLEAN
        ) USING DELTA
    """)

df_dev_delta.createOrReplaceTempView("dev_delta_view")


spark.sql(f"""
    MERGE INTO {dim_developer_table} t
    USING dev_delta_view s
    ON t.Developer_Name = s.Developer_Name AND t.is_current = true
    WHEN MATCHED AND (
        not(t.Market_Reputation <=> s.Market_Reputation) OR
        not(t.Delivery_Punctuality <=> s.Delivery_Punctuality)
    ) THEN
      UPDATE SET
        t.valid_to_ts = s.gold_update_ts,
        t.is_current = false
""")


spark.sql(f"""
    INSERT INTO {dim_developer_table}
    SELECT 
        md5(concat(s.Developer_Name, s.gold_update_ts)) as developer_sk,
        s.Developer_Name,
        s.Market_Reputation,
        s.Delivery_Punctuality,
        s.gold_update_ts as valid_from_ts,
        cast(null as timestamp) as valid_to_ts,
        true as is_current
    FROM dev_delta_view s
    LEFT JOIN {dim_developer_table} t
    ON s.Developer_Name = t.Developer_Name AND t.is_current = true
    WHERE t.Developer_Name IS NULL OR (
        not(t.Market_Reputation <=> s.Market_Reputation) OR
        not(t.Delivery_Punctuality <=> s.Delivery_Punctuality)
    )
""")

# ==========================================
#[Dim_Location] using (SCD Type 1 ) 
# ==========================================
print("Processing Dim_Location...")
df_loc_delta = df_new.select(
    "City", "District", 
    "Distance_To_City_Center_KM", "Transport_Index_Num", "Nearby_Schools_Score_Num"
).distinct().withColumn("location_sk", md5(concat_ws("||", col("City"), col("District"))))

if spark.catalog.tableExists(dim_location_table):
    (DeltaTable.forName(spark, dim_location_table).alias("t")
        .merge(df_loc_delta.alias("s"), "t.location_sk = s.location_sk")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    df_loc_delta.write.format("delta").saveAsTable(dim_location_table)

# ==========================================
# [Dim_Property_Profile] using (SCD Type 0)
# ==========================================
print("Processing Dim_Profile...")
df_profile_delta = df_new.select(
    "Unit_Type", "View_Type", "Kitchen_Type", 
    "Has_Elevator", "Smart_Home_Ready", "has_pool", "gym", "security", "parking_spots"
).distinct()

df_profile_delta = df_profile_delta.withColumn(
    "profile_sk", 
    md5(concat_ws("||", *[col(c) for c in df_profile_delta.columns]))
)

if spark.catalog.tableExists(dim_profile_table):
    (DeltaTable.forName(spark, dim_profile_table).alias("t")
        .merge(df_profile_delta.alias("s"), "t.profile_sk = s.profile_sk")
        .whenNotMatchedInsertAll()
        .execute())
else:
    df_profile_delta.write.format("delta").saveAsTable(dim_profile_table)

# ==========================================
# [Fact_Real_Estate_Listings] 
# ==========================================
print("Processing Fact Table...")
dim_dev_current = spark.read.table(dim_developer_table).filter(col("is_current") == True)
dim_loc = spark.read.table(dim_location_table)
dim_prof = spark.read.table(dim_profile_table)

fact_prep = (
    df_new.alias("sil")
    .join(dim_dev_current.alias("dev"), col("sil.Developer_Name") == col("dev.Developer_Name"), "left")
    .join(dim_loc.alias("loc"), (col("sil.City") == col("loc.City")) & (col("sil.District") == col("loc.District")), "left")
    .withColumn("temp_profile_sk", md5(concat_ws("||", col("sil.Unit_Type"), col("sil.View_Type"), col("sil.Kitchen_Type"), col("sil.Has_Elevator"), col("sil.Smart_Home_Ready"), col("sil.has_pool"), col("sil.gym"), col("sil.security"), col("sil.parking_spots"))))
)

fact_delta = fact_prep.select(
    col("sil." + PRIMARY_KEY),
    col("dev.developer_sk"),
    col("loc.location_sk"),
    col("temp_profile_sk").alias("profile_sk"),
    
    col("sil.Date").alias("Listing_Date"),
    col("sil.Delivery_Date"),
    
    col("sil.Total_Price"),
    col("sil.Size_SqM"),
    col("sil.Price_Per_SqM"),
    col("sil.Rooms_Count"),
    col("sil.Kitchen_Size_SqM"),
    col("sil.Reception_Size_SqM"),
    col("sil.Balcony_Size_SqM"),
    col("sil.Down_Payment_Pct"),
    col("sil.Installment_Years"),
    col("sil.Payment_Flexibility_Score"),
    
    col("sil.Is_Ready_To_Move"),
    col("sil.Payment_Type"),
    
    col("sil.gold_update_ts").alias("last_updated_ts")
)

if spark.catalog.tableExists(fact_table):
    (DeltaTable.forName(spark, fact_table).alias("t")
        .merge(fact_delta.alias("s"), f"t.{PRIMARY_KEY} = s.{PRIMARY_KEY}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    fact_delta.write.format("delta").saveAsTable(fact_table)

fact_count = fact_delta.count()
print(f"Upserted {fact_count} records into Fact Table.")

# ==========================================
# (Control Table Update) 
# ==========================================
print("Updating Control Table...")
max_silver_incremental_value = df_new.select(max(incremental_col)).collect()[0][0]

control_data = [(
    "gold",                                
    "real_estate",                         
    max_silver_incremental_value,          
    fact_count,                    
    "SUCCESS",                             
    gold_run_id,                           
    datetime.now()                         
)]


control_schema = """
    layer STRING,
    entity_name STRING,
    last_silver_update_ts STRING, 
    rows_merged BIGINT,
    run_status STRING,
    gold_run_id STRING,
    updated_at TIMESTAMP
"""

df_control = spark.createDataFrame(control_data, schema=control_schema)

if spark.catalog.tableExists(control_table):
    (DeltaTable.forName(spark, control_table).alias("t")
        .merge(df_control.alias("s"), "t.layer = s.layer AND t.entity_name = s.entity_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    df_control.write.format("delta").saveAsTable(control_table)

print(f"Gold Pipeline Finished Successfully! High-water mark set to: {max_silver_incremental_value}")