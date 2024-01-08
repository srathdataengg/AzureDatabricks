# Databricks notebook source
container_name ="employees"
account_name="dataenggingestion"
mount_point ="/mnt/employees"

# COMMAND ----------

tenant_id=dbutils.secrets.get(scope="databricks-secrets-data",key="tenant-id")
application_id=dbutils.secrets.get(scope="databricks-secrets-data",key="application-id")
secret=dbutils.secrets.get(scope="databricks-secrets-data",key="secret")

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

# MAGIC %md
# MAGIC Adding parquet files to the silver layer
# MAGIC Columns must be not be a nullable except for MANAGER_ID
# MAGIC

# COMMAND ----------

# Reading in the employees csv file from the bronze layer.
# Reading in the hire date as string to avoid formatting issues.
employee_path ='dbfs:/mnt/employees/bronze/employees.csv'

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

employee_schema = StructType([
                    StructField("EMPLOYEE_ID", IntegerType(), False),
                    StructField("FIRST_NAME", StringType(), False),
                    StructField("LAST_NAME", StringType(), False),
                    StructField("EMAIL", StringType(), False),
                    StructField("PHONE_NUMBER", StringType(), False),
                    StructField("HIRE_DATE", StringType(), False),
                    StructField("JOB_ID", StringType(), False),
                    StructField("SALARY", IntegerType(), False),
                    StructField("MANAGER_ID", IntegerType(), True),
                    StructField("DEPARTMENT_ID", IntegerType(), False)
                    ]
                    )
employee = spark.read.csv(path=employee_path,schema=employee_schema)

employee.show()

# COMMAND ----------

# Dropping unnecessary columns - EMAIL,PHONE_NUMBER
from pyspark.sql.functions import col
employee = employee.drop(col("EMAIL"),col("PHONE_NUMBER"))
display(employee)

# COMMAND ----------

# changing HIRE_DATE column from string to DATE column
from pyspark.sql.functions import to_date

employee = employee.select(
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    to_date(employee["HIRE_DATE"], "MM/dd/yyyy").alias('HIRE_DATE'),
    "JOB_ID",
    "SALARY",
    "MANAGER_ID",
    "DEPARTMENT_ID"
)

# COMMAND ----------

employee.show(truncate=True)


# COMMAND ----------

# Writing data in parquet format to silver layer.
employee.write.parquet("/mnt/employees/silver/employees",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC DEPARTMENTS
# MAGIC

# COMMAND ----------

# Reading the department files from the bronze layer

from pyspark.sql.types import IntegerType,StringType
department_path = 'dbfs:/mnt/employees/bronze/departments.csv'

department_schema = StructType([StructField("department_id",IntegerType(),True),
                                    StructField("department_name",StringType(),True),
                                    StructField("manager_id",IntegerType(),True),
                                    StructField("location_id",IntegerType(),True)])

department = spark.read.csv(path=department_path,schema=department_schema)

display(department)


# COMMAND ----------

# Dropping unnecessary columns - manger_id,location_id

department = department.drop("manager_id","location_id")
display(department)



# COMMAND ----------

# writing department data into silver layer as a parquet file
department.write.parquet("/mnt/employees/silver/department",mode="overwrite")


# COMMAND ----------

# MAGIC %md 
# MAGIC COUNTRIES

# COMMAND ----------

# Reading the department files from the bronze layer

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
countries_path = 'dbfs:/mnt/employees/bronze/countries.csv'
country_schema = StructType([
    StructField("country_id",IntegerType(),True),
    StructField("country_name",StringType(),True)])
country = spark.read.csv("dbfs:/mnt/employees/bronze/countries.csv",header="True")
display(country)

# COMMAND ----------

# Writing dataframes country to silver layer
country = country.write.parquet("/mnt/employees/silver/country",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC Adding Parquet files to gold folder
# MAGIC

# COMMAND ----------

# Reading data from Employee Silver Parquet folder
employee = spark.read.parquet("/mnt/employees/silver/employees")
employee.show()

# COMMAND ----------

# Reading data from Department 
department = spark.read.parquet("/mnt/employees/silver/department")
department.show()

# COMMAND ----------

# Create a new column full_name using with_column

from pyspark.sql.functions import concat_ws

employee = employee.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name"))).drop(col("first_name"),col("last_name"))

employee = employee.select("employee_id","full_name","hire_date","job_id","salary","manager_id","department_id")

display(employee)

# COMMAND ----------

# Joining the employee and department tables on department_id and drop irrelevant columns
employee = employee.join(department,employee.department_id == department.department_id,'left')
display(employee)

# COMMAND ----------

employee = employee.select("employee_id","full_name","hire_date","salary","department_name")
employee.show()

# COMMAND ----------

employee.write.parquet("dbfs:/mnt/employees/gold/employee")

# COMMAND ----------

# MAGIC %md
# MAGIC  Create the employee database and load the employee gold layer parquet file

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS EMPLOYEE;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE employee.employees 
# MAGIC (
# MAGIC   EMPLOYEE_ID int,
# MAGIC   FULL_NAME string,
# MAGIC   HIRE_DATE date, 
# MAGIC   JOB_ID string,
# MAGIC   SALARY int,
# MAGIC   DEPARTMENT_NAME string
# MAGIC   )
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/employees/gold/employee';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employee.employees;

# COMMAND ----------


