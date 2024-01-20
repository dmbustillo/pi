# Databricks notebook source
container_name = "demo"
storage_account = "datalakeanalyticssa"
source = "abfss://{}@{}.dfs.core.windows.net".format(container_name, storage_account)
mount_point = "/mnt/data"

# COMMAND ----------

dbutils.fs.ls(source)

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")


# COMMAND ----------

dbutils.fs.ls("/FileStore")

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/wine_quality__1_.csv")


# COMMAND ----------

# displaying the dataframe df
display(df)


# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/wine_quality__1_.csv", "abfss://{}@{}.dfs.core.windows.net/wine_quality__1_.csv".format(container_name, storage_account))

# COMMAND ----------

dbutils.fs.ls("abfss://{}@{}.dfs.core.windows.net/wine_quality__1_.csv".format(container_name, storage_account))

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://root@dbstoragehyj74x4ihlu6.dfs.core.windows.net",
  mount_point = "/mnt/kp-adls",
)

# COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "<application-id>",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/<directory-id>/oauth2/token")
// Optionally, you can add <directory-name> to the source URI of your mount point.

container_name = "demo"
storage_account = "datalakeanalyticssa"
source = "abfss://{}@{}.dfs.core.windows.net".format(container_name, storage_account)

dbutils.fs.mount(
  source = f"abfss://{container_name}>@{storage_account}.dfs.core.windows.net/",
  mountPoint = "/mnt/<mount-name>",
  extraConfigs = configs)

# COMMAND ----------

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

sc.getConf().getAll()

# COMMAND ----------

# MAGIC %scala
# MAGIC val storageAccount = "datalakeanalyticssa"
# MAGIC val container = "demo"
# MAGIC val sasKey = "?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupyx&se=2024-01-20T23:20:01Z&st=2024-01-20T15:20:01Z&spr=https&sig=jKkWQW7mvCpGpJGQSQQRCz7EskOt1DN6yRlkGXxfDCk%3D"
# MAGIC  
# MAGIC val mountPoint = s"/mnt/testing"
# MAGIC  
# MAGIC  
# MAGIC try {
# MAGIC   dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
# MAGIC } catch {
# MAGIC   case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
# MAGIC }
# MAGIC  
# MAGIC  
# MAGIC val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
# MAGIC val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC try {
# MAGIC   dbutils.fs.mount(
# MAGIC     source = sourceString,
# MAGIC     mountPoint = mountPoint,
# MAGIC     extraConfigs = Map(confKey -> sasKey)
# MAGIC   )
# MAGIC }
# MAGIC catch {
# MAGIC   case e: Exception =>
# MAGIC     println(s"*** ERROR: Unable to mount $mountPoint. Run previous cells to unmount first")
# MAGIC }
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

dbutils.fs.unmount("/mnt/testing")

# COMMAND ----------

# MAGIC %fs ls /mnt/testing/

# COMMAND ----------

df = spark.read.csv("/mnt/testing/wine_quality__1_.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------


