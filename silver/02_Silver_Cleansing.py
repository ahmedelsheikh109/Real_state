# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Silver Layer (Data Transformations & Cleaning)
# MAGIC Standardizes formats, extracts JSON, imputes nulls with medians, and cleans messy strings.

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap, regexp_replace, coalesce, try_to_date, year, from_json, when, percentile_approx, lit, round, current_date, rand, floor, array, size, expr, to_date, lower
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DecimalType
from pyspark.sql.window import Window

catalog = "workspace"
schema = "default"
table_prefix = f"{catalog}.{schema}.real_estate"
bronze_table = f"{table_prefix}_bronze"
silver_table = f"{table_prefix}_silver"

df = spark.read.table(bronze_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.1 Standardizing Text & Casting Metrics

# COMMAND ----------

# Initialize df_2 from df
df_2 = df

# Robust text cleaning: collapse multiple spaces, trim, capitalize, and fill NULLs with "Unknown"
for text_col in ["City", "District", "Compound_Name", "Unit_Type", "View_Type"]:
    df_2 = df_2.withColumn(
        text_col, 
        coalesce(initcap(trim(regexp_replace(col(text_col), r'\s+', ' '))), lit("Unknown"))
    )

# delivery date column transformation 
df_2 = df_2.withColumn(
 "Delivery_Date",
 trim(regexp_replace(col("Delivery_Date"), "\\s+", ""))
)
df_2 = df_2.withColumn(
 "Delivery_Date",
 coalesce(
 try_to_date("Delivery_Date", "yyyy-MM-dd"),
 try_to_date("Delivery_Date", "MM/dd/yyyy"),
 try_to_date("Delivery_Date", "M/d/yyyy"), # IMPORTANT (fixes single digit cases)
 try_to_date("Delivery_Date", "dd-MM-yyyy"),
 try_to_date("Delivery_Date", "d-M-yyyy"), # IMPORTANT
 try_to_date("Delivery_Date", "dd-MMM-yyyy"),
 try_to_date("Delivery_Date", "MMM-dd-yyyy"),
 try_to_date("Delivery_Date", "MMM dd, yyyy")
 )
)

# Apply identical logic to Date column to prevent parsing errors downstream
df_2 = df_2.withColumn("Date", trim(regexp_replace(col("Date"), "\\s+", "")))
df_2 = df_2.withColumn(
 "Date",
 coalesce(
 try_to_date("Date", "yyyy-MM-dd"), try_to_date("Date", "MM/dd/yyyy"),
 try_to_date("Date", "M/d/yyyy"), try_to_date("Date", "dd-MM-yyyy"),
 try_to_date("Date", "d-M-yyyy"), try_to_date("Date", "dd-MMM-yyyy"),
 try_to_date("Date", "MMM-dd-yyyy"), try_to_date("Date", "MMM dd, yyyy")
 )
)

# Date Sanity Check: Ensure dates match our dataset logic to filter out typos
# Delivery Date can be in the future (e.g. Off-plan properties delivering in 2035) or past
df_2 = df_2.withColumn("Delivery_Date", when((year(col("Delivery_Date")) >= 2010) & (year(col("Delivery_Date")) <= 2035), col("Delivery_Date")).otherwise(None))
# Transaction Date should strictly match our actual data years (2018 to 2027) with a tiny buffer
df_2 = df_2.withColumn("Date", when((year(col("Date")) >= 2018) & (year(col("Date")) <= 2027), col("Date")).otherwise(None))

# total price -> int and cap at > 0
df_2 = df_2.withColumn(
 "Total_Price",
 regexp_replace(col("Total_Price"), "[^0-9]", "").cast("int")
)
df_2 = df_2.withColumn("Total_Price", when(col("Total_Price") > 0, col("Total_Price")).otherwise(None))

# size sqM -> cap at > 0
df_2 = df_2.withColumn("Size_SqM", when(col("Size_SqM").cast("double") > 0, col("Size_SqM").cast("double")).otherwise(None))

# Derived metric (Safe division to avoid DivByZero)
df_2 = df_2.withColumn(
 "Price_Per_SqM", 
 when(col("Size_SqM").isNotNull() & col("Total_Price").isNotNull(), col("Total_Price").cast("double") / col("Size_SqM").cast("double"))
 .otherwise(None)
)

# --- Conditional Random Assignment (Rule-Based Bucket Routing) ---
# Defining arrays for major governorates
cairo_luxury = array(lit("التجمع الخامس"), lit("الزمالك"), lit("مصر الجديدة"))
cairo_medium = array(lit("مدينة نصر"), lit("الشروق"), lit("المستقبل"))
cairo_economic = array(lit("شبرا"), lit("المرج"))

giza_luxury = array(lit("الشيخ زايد"), lit("المهندسين"))
giza_medium = array(lit("6 أكتوبر"), lit("الدقي"))
giza_economic = array(lit("فيصل"), lit("الهرم"))

# Smart routing function using size() to automatically adapt to array length
def get_random_district(district_array):
    return district_array.getItem(floor(rand(seed=42) * size(district_array)).cast("int"))

# Apply intelligent routing when the original district is just the broad Governorate name or Unknown
df_2 = df_2.withColumn(
    "District",
    when((col("City") == "القاهرة") & (col("District").isin("القاهرة", "Unknown")),
        when(col("Price_Per_SqM") >= 40000, get_random_district(cairo_luxury))
        .when(col("Price_Per_SqM") >= 20000, get_random_district(cairo_medium))
        .otherwise(get_random_district(cairo_economic))
    )
    .when((col("City") == "الجيزة") & (col("District").isin("الجيزة", "Unknown")),
        when(col("Price_Per_SqM") >= 35000, get_random_district(giza_luxury))
        .when(col("Price_Per_SqM") >= 18000, get_random_district(giza_medium))
        .otherwise(get_random_district(giza_economic))
    )
    .otherwise(col("District"))
)

# extracting year from delivery date for filling the total price with median per year per city
df_2 = df_2.withColumn(
 "year",
 year(col("Delivery_Date"))
)

# changing down payment pct type to double and enforcing [0, 100] bounds
df_2 = df_2.withColumn("Down_Payment_Pct", col("Down_Payment_Pct").cast("double"))
df_2 = df_2.withColumn("Down_Payment_Pct", when(col("Down_Payment_Pct") < 0, 0.0).when(col("Down_Payment_Pct") > 100, 100.0).otherwise(col("Down_Payment_Pct")))

# installment years -> int and enforcing [0, 30] bounds
df_2 = df_2.withColumn("Installment_Years", col("Installment_Years").cast("int"))
df_2 = df_2.withColumn("Installment_Years", when(col("Installment_Years") < 0, 0).when(col("Installment_Years") > 30, 30).otherwise(col("Installment_Years")))

# --- Safe Transformations for New Columns (No Null Dropping/Filling) ---
df_2 = df_2.withColumn("Kitchen_Type", initcap(trim(regexp_replace(col("Kitchen_Type"), r'\s+', ' '))))
df_2 = df_2.withColumn("Has_Elevator", col("Has_Elevator").cast("boolean"))
df_2 = df_2.withColumn("Distance_To_City_Center_KM", col("Distance_To_City_Center_KM").cast("double"))
df_2 = df_2.withColumn("Kitchen_Size_SqM", col("Kitchen_Size_SqM").cast("double"))
df_2 = df_2.withColumn("Reception_Size_SqM", col("Reception_Size_SqM").cast("double"))
df_2 = df_2.withColumn("Balcony_Size_SqM", col("Balcony_Size_SqM").cast("double"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.2 JSON Extraction & Developer Name Regex

# COMMAND ----------

# json column parsing
schema_json = StructType([
 StructField("has_pool", BooleanType(), True),
 StructField("gym", BooleanType(), True),
 StructField("security", StringType(), True),
 StructField("parking_spots", IntegerType(), True)
])

df_2 = df_2.withColumn(
 "parsed",
 from_json(col("Amenities_JSON"), schema_json)
)
df_2 = df_2.select(
 "*",
 col("parsed.*")
).drop("parsed")

# dropping json column
fin_df = df_2.drop("Amenities_JSON")

# Advanced regex cleaning for Developer Name & NULL imputation
fin_df = fin_df.withColumn(
    "Developer_Name",
    coalesce(
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            col("Developer_Name"),
                            '"',
                            ''
                        ),
                        r'\n',
                        ' '
                    ),
                    ',',
                    ''
                ),
                r'\s+',
                ' '
            )
        ),
        lit("Unknown Developer")
    )
)

# 2. The Standardization Dictionary (Corruption Fixes)
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

# 3. Apply the mapping to standardize the values
fin_df = fin_df.replace(developer_mapping, subset=["Developer_Name"])

# --- User Requested Transformations for Reputation & Punctuality ---
# We use initcap to fix casing ('hIGH' -> 'High', 'LOW' -> 'Low').
# Instead of using .filter() to delete the row, we safely map 'Null', '???', and 'M' to 'Unknown'
fin_df = fin_df.withColumn(
    "Market_Reputation",
    when(
        col("Market_Reputation").isNull() | 
        (lower(col("Market_Reputation")) == "null") | 
        (col("Market_Reputation") == "???"),
        "Unknown"
    ).otherwise(initcap(col("Market_Reputation")))
)

fin_df = fin_df.withColumn(
    "Delivery_Punctuality",
    when(
        col("Delivery_Punctuality").isNull() | 
        (lower(col("Delivery_Punctuality")) == "null") | 
        (col("Delivery_Punctuality") == "M"),
        "Unknown"
    ).otherwise(initcap(col("Delivery_Punctuality")))
)

# --- Global Text Cleanup for the literal string "Nan" ---
# Replaces the literal string "Nan" with "Unknown" across all string columns to fix Power BI showing 'Nan'
for c in [f.name for f in fin_df.schema.fields if isinstance(f.dataType, StringType)]:
    fin_df = fin_df.withColumn(c, when(col(c) == "Nan", "Unknown").otherwise(col(c)))

# --- Data Quality: Realistic Minimums for Rooms ---
# The user noted that a Rooms_Count of 0 is unrealistic and unreliable (e.g. for Studios).
# We ensure every valid property has at least 1 room.
fin_df = fin_df.withColumn("Rooms_Count", when(col("Rooms_Count") == 0, 1).otherwise(col("Rooms_Count")))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.3 Hierarchical Window Imputation & Rounding

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

# --- Window Definitions for Hierarchical Fallbacks ---
dev_window = Window.partitionBy("Developer_Name")
district_window = Window.partitionBy("City", "District")
city_window = Window.partitionBy("City")

# --- 1. Dates & Time (Advanced Median Imputation) ---
fin_df = fin_df.withColumn(
    "Date",
    coalesce(
        to_date(col("Date")),
        to_date(expr("percentile_approx(unix_timestamp(Date), 0.5)").over(district_window).cast("timestamp")),
        to_date(expr("percentile_approx(unix_timestamp(Date), 0.5)").over(city_window).cast("timestamp")),
        to_date(col("ingestion_time"))
    )
)

# Recompute the year after imputation
fin_df = fin_df.withColumn("year", year(col("Date")))

# --- 2. Previous Projects (Developer Hierarchy) ---
fin_df = fin_df.withColumn(
    "Previous_Projects",
    coalesce(
        col("Previous_Projects"),
        expr("percentile_approx(Previous_Projects, 0.5)").over(dev_window),
        lit(0) # Safest global fallback for unknown developers
    )
)

# --- 3 & 4. Transport Index & Nearby Schools (Geospatial Hierarchy) ---
# Cast to numeric first so percentile_approx can calculate the median
fin_df = fin_df.withColumn("Transport_Index_Num", col("Transport_Index").cast(DecimalType(10, 2)))
fin_df = fin_df.withColumn("Nearby_Schools_Score_Num", col("Nearby_Schools_Score").cast(DecimalType(10, 2)))
fin_df = fin_df.withColumn("NearbyMalls_Dist_KM", col("Nearby_Malls_Dist_KM").cast(DecimalType(10, 2)))

fin_df = fin_df.withColumn(
    "TransportIndex",
    coalesce(
        col("Transport_Index_Num"),
        expr("percentile_approx(Transport_Index_Num, 0.5)").over(district_window),
        expr("percentile_approx(Transport_Index_Num, 0.5)").over(city_window)
    )
)

fin_df = fin_df.withColumn(
    "NearbySchools_Score",
    coalesce(
        col("Nearby_Schools_Score_Num"),
        expr("percentile_approx(Nearby_Schools_Score_Num, 0.5)").over(district_window),
        expr("percentile_approx(Nearby_Schools_Score_Num, 0.5)").over(city_window)
    )
)

# COMMAND ----------

# drop old string columns and temporary numeric columns
fin_df = fin_df.drop("Transport_Index", "Nearby_Malls_Dist_KM", "Nearby_Schools_Score", "Transport_Index_Num", "Nearby_Schools_Score_Num")

# filling nulls
fin_df = fin_df.fillna(0, subset=["Smart_Home_Ready"])
fin_df = fin_df.fillna("None", subset=["security"])
fin_df = fin_df.fillna(False, subset=["has_pool"])
fin_df = fin_df.fillna(False, subset=["gym"])
fin_df = fin_df.fillna(0, subset=["parking_spots"])

# --- Add Payment_Type Logic ---
# إذا كانت سنوات التقسيط = 0 ونسبة المقدم 100 (أو لا يوجد مقدم لأن الدفع فوري)، العقد كاش. وإلا تقسيط.
fin_df = fin_df.withColumn(
    "Payment_Type",
    when((col("Installment_Years") == 0) & ((col("Down_Payment_Pct") == 100) | col("Down_Payment_Pct").isNull()), "كاش")
    .when(col("Installment_Years") > 0, "تقسيط")
    .otherwise("كاش") # Default fallback if 0 years but Down_Payment is weirdly missing
)

# calculate median per developer for non-cash rows
median_per_developer = fin_df.filter(
 col("Down_Payment_Pct").isNotNull()
).groupBy("Developer_Name").agg(
 percentile_approx("Down_Payment_Pct", 0.5).alias("median_down_payment")
)

# join median back to main df
fin_df = fin_df.join(median_per_developer, on="Developer_Name", how="left")

# fill nulls with median per developer
fin_df = fin_df.withColumn(
 "Down_Payment_Pct",
 when(col("Payment_Type") == "كاش", 100) # enforce 100% for cash
 .when(col("Down_Payment_Pct").isNull(), col("median_down_payment"))
 .otherwise(col("Down_Payment_Pct"))
).drop("median_down_payment")

# --- Calculate Investor KPI: Payment Flexibility Score ---
fin_df = fin_df.withColumn(
    "Payment_Flexibility_Score", 
    round(col("Installment_Years") / (col("Down_Payment_Pct") + lit(1)), 2)
)

# --- Calculate Investor KPI: Is Ready To Move ---
fin_df = fin_df.withColumn(
    "Is_Ready_To_Move",
    when(col("Delivery_Date") <= current_date(), True).otherwise(False)
)

# COMMAND ----------

# Save final Silver Table
fin_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
display(spark.read.table(silver_table))
