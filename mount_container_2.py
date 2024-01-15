# Databricks notebook source
container_name = "demo"
storage_account = "datalakeanalyticssa"
source_ls = "abfss://{}@{}.dfs.core.windows.net".format(container_name, storage_account)
source_mount = "wasbs://{}@{}.blob.core.windows.net/".format(container_name, storage_account),
mount_point = "/mnt/data"

# COMMAND ----------

dbutils.fs.ls(source_ls)

# COMMAND ----------


