# Databricks notebook source
print("hello welcome to hive-managed table-external table")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS COUNTRIES;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE COUNTRIES;

# COMMAND ----------

# MAGIC %md
# MAGIC MANAGED TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC use countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries_txt;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries_txt;

# COMMAND ----------

countries = spark.read.csv(path='dbfs:/mnt/bronze/countries.csv',header=True)
countries.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

countries.write.saveAsTable('countries.countries_mt');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table countries.countries_mt;

# COMMAND ----------

# MAGIC %md
# MAGIC EXTERNAL TABLES

# COMMAND ----------

# MAGIC %python
# MAGIC countries = spark.read.csv(path='dbfs:/mnt/bronze/countries.csv',header=True)
# MAGIC countries.show()

# COMMAND ----------

# MAGIC %python
# MAGIC countries.write.option('path','FileStore/external/countries').saveAsTable('countries.countries_ext_python')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED countries.countries_ext_python;

# COMMAND ----------


