from pyspark.sql.functions import col, year, month, dayofmonth, quarter, dayofweek, date_format, expr

#range from (2010 to 2035)
start_date = "2010-01-01"
end_date = "2035-12-31"


df_date = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as Date")

# (Date Dimension)
df_dim_date = (
    df_date
   
    .withColumn("Date_ID", col("Date")) 
    
  
    .withColumn("Year", year(col("Date")))
    .withColumn("Quarter", quarter(col("Date")))
    .withColumn("Month", month(col("Date")))
    .withColumn("Month_Name", date_format(col("Date"), "MMMM"))
    
    
    .withColumn("Day_Of_Month", dayofmonth(col("Date")))
    .withColumn("Day_Of_Week", dayofweek(col("Date"))) 
    .withColumn("Day_Name", date_format(col("Date"), "EEEE"))
    

    .withColumn(
        "Is_Weekend", 
        expr("CASE WHEN dayofweek(Date) IN (6, 7) THEN True ELSE False END")
    )
)


table_name = "workspace.default.real_estate_dim_date"

df_dim_date.write.format("delta").mode("overwrite").saveAsTable(table_name)

