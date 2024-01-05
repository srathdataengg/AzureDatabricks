# Databricks notebook source
container_name ="gold"
account_name="dataenggingestion"
mount_point ="/mnt/gold"

# COMMAND ----------

tenant_id=dbutils.secrets.get(scope="databricks-secrets-data",key="tenant-id")
application_id=dbutils.secrets.get(scope="databricks-secrets-data",key="application-id")
secret=dbutils.secrets.get(scope="databricks-secrets-data",key="secret")

# COMMAND ----------

# MAGIC %md
# MAGIC ORDERS
# MAGIC Save a table called ORDERS in the silver layer, it should contain the following columns:
# MAGIC
# MAGIC ORDER_ID, type INTEGER
# MAGIC ORDER_TIMESTAMP, type TIMESTAMP
# MAGIC CUSTOMER_ID, type INTEGER
# MAGIC STORE_NAME, type STRING
# MAGIC The resulting table should only contain records where the ORDER_STATUS is 'COMPLETE'.
# MAGIC
# MAGIC The file should be saved in PARQUET format.
# MAGIC
# MAGIC Hint 1: You will need to initially read in the ORDER_DATETIME column as a string and then convert it to a timestamp using to_timestamp.
# MAGIC
# MAGIC Hint 2: You will need to merge the orders and stores tables from the bronze layer.
# MAGIC
# MAGIC Supporting Resources
# MAGIC to_timestamp: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp
# MAGIC
# MAGIC datetime patterns: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# MAGIC

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

countries_path = '/mnt/bronze/countries.csv'
from pyspark.sql.types import IntegerType,StringType,StructField,StructType,DoubleType
countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ]
                    )
country_df = spark.read.csv(path = countries_path,schema=countries_schema)
display(country_df)

# COMMAND ----------

country_df=country_df.drop("sub_region_id","intermediate_region_id","organization_region_id")
country_df.display()

# COMMAND ----------

country_df.write.parquet('/mnt/silver/countries')

# COMMAND ----------

regions_path = '/mnt/bronze/country_regions.csv'
 
regions_schema = StructType([
                    StructField("Id", StringType(), False),
                    StructField("NAME", StringType(), False)
                    ]
                    )
regions = spark.read.csv(path=regions_path,schema=regions_schema)

regions.show()

# COMMAND ----------

regions = regions.withColumnRenamed("NAME","REGION_NAME")

# COMMAND ----------

regions.write.parquet('/mnt/silver/regions')

# COMMAND ----------

country_silver = '/mnt/silver/countries'
COUNTRY = spark.read.parquet('/mnt/silver/countries')
COUNTRY.display()

# COMMAND ----------

REGIONS = spark.read.parquet('/mnt/silver/regions')
REGIONS.display()


# COMMAND ----------

from pyspark.sql.functions import col
country = COUNTRY.join(REGIONS,COUNTRY.REGION_ID==REGIONS.Id,'left').drop('Id','REGION_ID','ISO_ALPHA2','COUNTRY_CODE')

# COMMAND ----------

COUNTRY.display()

# COMMAND ----------

country.write.parquet('/mnt/gold/country_data')

# COMMAND ----------


