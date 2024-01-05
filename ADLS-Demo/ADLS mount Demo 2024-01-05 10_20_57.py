# Databricks notebook source
#spark.conf.set(
#   "fs.azure.account.key.dataenggingestion.dfs.core.windows.net",
#   "dR/zPzem3oH1ZPhHvxoRJ6mKeDHWl34VCfDT8EfS/qVunAL+1AD50oYUWcKNn18rhoqD3HcGfiRG+ASt3yU58w==")

# COMMAND ----------

countries_df = spark.read.csv("abfss://bronze@dataenggingestion.dfs.core.windows.net/countries.csv",header=True,inferSchema=True)
countries_df.printSchema()
countries_df.display()

# COMMAND ----------

regions_df = spark.read.csv("abfss://bronze@dataenggingestion.dfs.core.windows.net/country_regions.csv",header=True,inferSchema=True)
regions_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Connect ADLS Gen2 container using SAS Token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dataenggingestion.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dataenggingestion.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dataenggingestion.dfs.core.windows.net","sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-05T14:42:27Z&st=2024-01-05T06:42:27Z&spr=https&sig=GeIw7vSYAe4H0KH24YUdXtkErW1geiDJMn76xvNQqJM%3D")

# COMMAND ----------

regions_df = spark.read.csv("abfss://bronze@dataenggingestion.dfs.core.windows.net/country_regions.csv",header=True,inferSchema=True)
regions_df.display()

# COMMAND ----------


