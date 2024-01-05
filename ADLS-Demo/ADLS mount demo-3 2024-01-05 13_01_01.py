# Databricks notebook source
container_name ="bronze"
account_name="dataenggingestion"
mount_point ="/mnt/bronze"

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
  
