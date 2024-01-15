# Databricks notebook source
container_name = "demo"
storage_account = "datalakeanalyticssa"
source = "abfss://{}@{}.dfs.core.windows.net".format(container_name, storage_account)
mount_point = "/mnt/data"

# COMMAND ----------

dbutils.fs.ls(source)

# COMMAND ----------


