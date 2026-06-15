# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Gold Layer Constraints (DWH Modeling)
# MAGIC This notebook officially connects the Data Warehouse tables in Databricks by defining 
# MAGIC Primary Key (PK) and Foreign Key (FK) constraints. This ensures the ER diagram 
# MAGIC is correctly connected in the Databricks Catalog.

# COMMAND ----------

catalog = "workspace"
schema = "default"
table_prefix = f"{catalog}.{schema}.real_estate"

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Define Primary Keys for Dimensions

# COMMAND ----------

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

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Define Primary Key for Fact Table

# COMMAND ----------

spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ALTER COLUMN Sale_Key SET NOT NULL")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS pk_fact_sales")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales ADD CONSTRAINT pk_fact_sales PRIMARY KEY (Sale_Key) RELY")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Define Foreign Keys mapping Fact to Dimensions

# COMMAND ----------

# Drop existing FKs if rerunning
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_sale_date")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_delivery_date")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_location")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_developer")
spark.sql(f"ALTER TABLE {table_prefix}_gold_fact_sales DROP CONSTRAINT IF EXISTS fk_property")

# Add Foreign Keys
spark.sql(f"""
ALTER TABLE {table_prefix}_gold_fact_sales 
ADD CONSTRAINT fk_sale_date FOREIGN KEY (Sale_Date_Key) REFERENCES {table_prefix}_gold_dim_date (Date_Key) RELY
""")

spark.sql(f"""
ALTER TABLE {table_prefix}_gold_fact_sales 
ADD CONSTRAINT fk_delivery_date FOREIGN KEY (Delivery_Date_Key) REFERENCES {table_prefix}_gold_dim_date (Date_Key) RELY
""")

spark.sql(f"""
ALTER TABLE {table_prefix}_gold_fact_sales 
ADD CONSTRAINT fk_location FOREIGN KEY (Location_Key) REFERENCES {table_prefix}_gold_dim_location (Location_Key) RELY
""")

spark.sql(f"""
ALTER TABLE {table_prefix}_gold_fact_sales 
ADD CONSTRAINT fk_developer FOREIGN KEY (Developer_Key) REFERENCES {table_prefix}_gold_dim_developer (Developer_Key) RELY
""")

spark.sql(f"""
ALTER TABLE {table_prefix}_gold_fact_sales 
ADD CONSTRAINT fk_property FOREIGN KEY (Property_Key) REFERENCES {table_prefix}_gold_dim_property (Property_Key) RELY
""")

print("Successfully applied all Primary Key and Foreign Key constraints! The modeling is now officially connected.")
