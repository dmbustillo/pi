# Databricks notebook source
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


