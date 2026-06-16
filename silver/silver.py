from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

# ==========================================
#(Configurations)
# ==========================================
catalog = "workspace"
schema = "default"
table_prefix = f"{catalog}.{schema}.real_estate"

bronze_table = f"{table_prefix}_bronze"
silver_table = f"{table_prefix}_silver"
quarantine_table = f"{table_prefix}_quarantine"
control_table = f"{catalog}.{schema}.processing_control"

silver_run_id = str(uuid.uuid4())

#  var to preserve (pk)
PRIMARY_KEY = "Property_ID" 

# ==========================================
#(State Management)
# ==========================================
def get_last_processed_ts():
    if spark.catalog.tableExists(control_table):
        try:
            return spark.table(control_table).filter(col("entity_name") == "real_estate").select(max("last_bronze_ingested_at")).collect()[0][0]
        except:
            return None
    return None

last_processed_ts = get_last_processed_ts()

# ==========================================
#(Incremental Fetch)
# ==========================================
df_bronze = spark.read.table(bronze_table)

if last_processed_ts:
    df_new = df_bronze.filter(col("bronze_ingested_at") > lit(last_processed_ts))
else:
    df_new = df_bronze

new_records_count = df_new.count()
print(f"Found {new_records_count} new records to process.")

if new_records_count > 0:
    # ==========================================
    #(Deduplication) 
    # ==========================================
    dedup_window = Window.partitionBy(PRIMARY_KEY).orderBy(col("bronze_ingested_at").desc())

    df_dedup = (
        df_new
        .withColumn("row_rank", row_number().over(dedup_window))
        .filter(col("row_rank") == 1)
        .drop("row_rank")
    )

    df_2 = df_dedup 

    # ==========================================
    #(Business Logic)
    # ==========================================
    
    # collapse multiple spaces, trim, capitalize, and fill NULLs with "Unknown"
    for text_col in ["City", "District", "Compound_Name", "Unit_Type", "View_Type"]:
        df_2 = df_2.withColumn(
            text_col, 
            coalesce(initcap(trim(regexp_replace(col(text_col), r'\s+', ' '))), lit("Unknown"))
        )

    # Delivery_Date Parsing
    df_2 = df_2.withColumn("Delivery_Date", trim(regexp_replace(col("Delivery_Date"), "\\s+", "")))
    df_2 = df_2.withColumn(
        "Delivery_Date",
        coalesce(
            try_to_date(substring("Delivery_Date",1,10), "yyyy-MM-dd"),
            try_to_date("Delivery_Date", "MM/dd/yyyy"),
            try_to_date("Delivery_Date", "M/d/yyyy"), 
            try_to_date("Delivery_Date", "dd-MM-yyyy"),
            try_to_date("Delivery_Date", "d-M-yyyy"), 
            try_to_date("Delivery_Date", "dd-MMM-yyyy"),
            try_to_date("Delivery_Date", "MMM-dd-yyyy"),
            try_to_date("Delivery_Date", "MMM dd, yyyy")
        )
    )

    # Date Parsing
    df_2 = df_2.withColumn("Date", trim(regexp_replace(col("Date"), "\\s+", "")))
    df_2 = df_2.withColumn(
        "Date",
        coalesce(
            try_to_date(substring("Date",1,10), "yyyy-MM-dd"), try_to_date("Date", "MM/dd/yyyy"),
            try_to_date("Date", "M/d/yyyy"), try_to_date("Date", "dd-MM-yyyy"),
            try_to_date("Date", "d-M-yyyy"), try_to_date("Date", "dd-MMM-yyyy"),
            try_to_date("Date", "MMM-dd-yyyy"), try_to_date("Date", "MMM dd, yyyy")
        )
    )
    
    # Date Fallback
    ingest_col = "ingestion_time" if "ingestion_time" in df_2.columns else "bronze_ingested_at"
    df_2 = df_2.withColumn("Date", coalesce(col("Date"), to_date(col(ingest_col))))

    df_2 = df_2.withColumn("Delivery_Date", when((year(col("Delivery_Date")) >= 2010) & (year(col("Delivery_Date")) <= 2035), col("Delivery_Date")).otherwise(None))
    df_2 = df_2.withColumn("Date", when((year(col("Date")) >= 2018) & (year(col("Date")) <= 2027), col("Date")).otherwise(None))

    # Numeric formatting & Size
    df_2 = df_2.withColumn("Total_Price", regexp_replace(col("Total_Price"), "[^0-9]", "").cast("int"))
    df_2 = df_2.withColumn("Total_Price", when(col("Total_Price") > 0, col("Total_Price")).otherwise(None))
    df_2 = df_2.withColumn("Size_SqM", when(col("Size_SqM").cast("double") > 0, col("Size_SqM").cast("double")).otherwise(None))

    # derived col of [Price_Per_SqM]
    df_2 = df_2.withColumn(
        "Price_Per_SqM", 
        when(col("Size_SqM").isNotNull() & col("Total_Price").isNotNull(), col("Total_Price").cast("double") / col("Size_SqM").cast("double"))
        .otherwise(None)
    )

    df_2 = df_2.withColumn("year", year(col("Delivery_Date")))

    # Down Payment boundaries
    df_2 = df_2.withColumn("Down_Payment_Pct", col("Down_Payment_Pct").cast("double"))
    df_2 = df_2.withColumn("Down_Payment_Pct", when(col("Down_Payment_Pct") < 0, 0.0).when(col("Down_Payment_Pct") > 100, 100.0).otherwise(col("Down_Payment_Pct")))

    # Installments boundaries
    df_2 = df_2.withColumn("Installment_Years", col("Installment_Years").cast("int"))
    df_2 = df_2.withColumn("Installment_Years", when(col("Installment_Years") < 0, 0).when(col("Installment_Years") > 30, 30).otherwise(col("Installment_Years")))

    df_2 = df_2.withColumn("Kitchen_Type", initcap(trim(regexp_replace(col("Kitchen_Type"), r'\s+', ' '))))
    df_2 = df_2.withColumn("Has_Elevator", col("Has_Elevator").cast("boolean"))
    
    # Casts
    df_2 = df_2.withColumn("Distance_To_City_Center_KM", col("Distance_To_City_Center_KM").cast("double"))
    df_2 = df_2.withColumn("Kitchen_Size_SqM", col("Kitchen_Size_SqM").cast("double"))
    df_2 = df_2.withColumn("Reception_Size_SqM", col("Reception_Size_SqM").cast("double"))
    df_2 = df_2.withColumn("Balcony_Size_SqM", col("Balcony_Size_SqM").cast("double"))

    # JSON Parsing
    schema_json = StructType([
        StructField("has_pool", BooleanType(), True),
        StructField("gym", BooleanType(), True),
        StructField("security", StringType(), True),
        StructField("parking_spots", IntegerType(), True)
    ])

    df_2 = df_2.withColumn("parsed", from_json(col("Amenities_JSON"), schema_json))
    df_2 = df_2.select("*", col("parsed.*")).drop("parsed", "Amenities_JSON")

    # Developer Mapping
    developer = col("Developer_Name")
    developer = regexp_replace(developer, '"', "")
    developer = regexp_replace(developer, r"\n", " ")
    developer = regexp_replace(developer, ",", "")
    developer = regexp_replace(developer, r"\s+", " ")
    developer = trim(developer)

    df_2 = df_2.withColumn("Developer_Name", coalesce(developer, lit("Unknown Developer")))

    developer_mapping = {
        "هايد بار ك للتطوير": "هايد بارك للتطوير",
        "ذا لان د ووركس": "ذا لاند ووركس",
        "كابيتا ل جروب": "كابيتال جروب",
        "سمو ل لتطوير": "سمو للتطوير",
        "المقاول ون العرب": "المقاولون العرب",
        "بالم هيل ز للتعمير": "بالم هيلز للتعمير",
        "جلوبال ري ال إيستيت": "جلوبال ريال إيستيت",
        "إيمتك ا لعقارية": "إيمتك العقارية",
        "الأهل ي صبور": "الأهلي صبور",
        "مصر الجدي دة للإسكان": "مصر الجديدة للإسكان",
        "بروبرت ي سكوب": "بروبرتي سكوب",
        "أرض الني ل للتطوير": "أرض النيل للتطوير",
        "مجموع ة طلب": "مجموعة طلب",
        "سو ديك": "سوديك",
        "نايل دف يلوبمنتس": "نايل دفيلوبمنتس",
        "مجموعة رؤ ية القابضة": "مجموعة رؤية القابضة",
        "رؤية ل لتطوير": "رؤية للتطوير"
    }
    df_2 = df_2.replace(developer_mapping, subset=["Developer_Name"])

    # Market Reputation Validation
    valid_reputations = ["low", "medium", "high", "excellent", "poor"]
    df_2 = df_2.withColumn(
        "Market_Reputation",
        when(
            col("Market_Reputation").isNull() | ~lower(col("Market_Reputation")).isin(valid_reputations),
            "Unknown"
        ).otherwise(initcap(col("Market_Reputation")))
    )

    # Delivery Punctuality Validation
    valid_punctuality=["low","medium","high"]
    df_2 = df_2.withColumn(
        "Delivery_Punctuality",
        when(
            col("Delivery_Punctuality").isNull() | 
            ~lower(col("Delivery_Punctuality")).isin(valid_punctuality), 
            "Unknown"
        ).otherwise(initcap(col("Delivery_Punctuality")))
    )

    # Fix string 'nan'
    for c in [f.name for f in df_2.schema.fields if isinstance(f.dataType, StringType)]:
        df_2 = df_2.withColumn(
            c, 
            when(lower(trim(col(c))) == "nan", "Unknown").otherwise(col(c))
        )

    # Rooms count
    df_2 = df_2.withColumn("Rooms_Count", when(col("Rooms_Count") == 0, 1).otherwise(col("Rooms_Count")))

    # Decimals casting
    df_2 = df_2.withColumn("Transport_Index_Num", col("Transport_Index").cast(DecimalType(10, 2)))
    df_2 = df_2.withColumn("Nearby_Schools_Score_Num", col("Nearby_Schools_Score").cast(DecimalType(10, 2)))
    df_2 = df_2.withColumn("NearbyMalls_Dist_KM", col("Nearby_Malls_Dist_KM").cast(DecimalType(10, 2)))

    # Window Imputations
    city_window = Window.partitionBy("City")
    df_2 = df_2.withColumn(
        "Transport_Index_Num",
        coalesce(col("Transport_Index_Num"), mean(col("Transport_Index_Num")).over(city_window))
    )
    df_2 = df_2.withColumn(
        "Nearby_Schools_Score_Num",
        coalesce(col("Nearby_Schools_Score_Num"), mean(col("Nearby_Schools_Score_Num")).over(city_window))
    )

    df_2 = df_2.drop("Transport_Index", "Nearby_Malls_Dist_KM", "Nearby_Schools_Score")
    
    fill_values = {
        "Smart_Home_Ready": 0,
        "security": "None",
        "has_pool": False,
        "gym": False,
        "parking_spots": 0
    }
    df_2 = df_2.fillna(fill_values)

    df_2 = df_2.withColumn(
        "Payment_Type",
        when(col("Installment_Years") > 0, "تقسيط").otherwise("كاش")
    )

    dev_window = Window.partitionBy("Developer_Name")
    df_2 = df_2.withColumn(
        "Down_Payment_Pct",
        when(col("Payment_Type") == "كاش", 100.0) 
        .when(col("Down_Payment_Pct").isNull(), mean(col("Down_Payment_Pct")).over(dev_window))
        .otherwise(col("Down_Payment_Pct"))
    )

    df_2 = df_2.withColumn("Payment_Flexibility_Score", round(col("Installment_Years") / (col("Down_Payment_Pct") + lit(1)), 2))
    df_2 = df_2.withColumn("Is_Ready_To_Move", when(col("Delivery_Date") <= current_date(), True).otherwise(False))

    # ==========================================
    #(Data Quality & Quarantine)
    # ==========================================
    df_validated = df_2.withColumn(
        "is_valid",
        when(
            col("Total_Price").isNull() | 
            col("Size_SqM").isNull() | 
            (col("City") == "Unknown"), 
            False
        ).otherwise(True)
    ).withColumn(
        "quarantine_reason",
        when(col("Total_Price").isNull(), "Missing or Invalid Price")
        .when(col("Size_SqM").isNull(), "Missing or Invalid Size")
        .when(col("City") == "Unknown", "Missing City")
        .otherwise("Valid")
    )

    df_good = (
        df_validated.filter(col("is_valid") == True)
        .drop("is_valid", "quarantine_reason")
        .withColumn("silver_run_id", lit(silver_run_id))
    )
    
    df_bad = (
        df_validated.filter(col("is_valid") == False)
        .drop("is_valid")
        .withColumn("quarantine_ts", current_timestamp())
    )

    # ==========================================
    #(Quarantine)
    # ==========================================
    if df_bad.count() > 0:
        df_bad.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)

    # ==========================================
    #(Upsert via MERGE)
    # ==========================================
    if spark.catalog.tableExists(silver_table):
        delta_table = DeltaTable.forName(spark, silver_table)
        (
            delta_table.alias("target")
            .merge(
                df_good.alias("source"), 
                f"target.{PRIMARY_KEY} = source.{PRIMARY_KEY}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_good.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    # ==========================================
    #(Update State)
    # ==========================================
    max_bronze_ts = df_new.select(max("bronze_ingested_at")).collect()[0][0]
    
    # Save logic to control_table goes here
    
    print(f"Success! Processed {df_good.count()} valid records. Sent {df_bad.count()} records to Quarantine.")
else:
    print("No new records found in Bronze.")