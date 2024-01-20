# Databricks notebook source
container_name = "demo"
storage_account = "datalakeanalyticssa"
source = "abfss://{}@{}.dfs.core.windows.net".format(container_name, storage_account)
mount_name = "/mnt/demo"
configs = {'fs.azure.account.key.datalakeanalyticssa.blob.core.windows.net':
  'vvgU8S3damw5O0lGkkhlzvGkZ4o6WTUtYKT91nGWgv9OotDISwdHOlzRY0Vn2RYJAfzmHcvANdR8+AStT0D5Bg=='
}
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/",
  mount_point = mount_name,
  extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("/mnt/demo")

# COMMAND ----------

display(spark.read.csv("/mnt/demo/wine_quality__1_.csv"))

# COMMAND ----------


