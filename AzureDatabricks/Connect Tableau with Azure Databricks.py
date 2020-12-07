


# Here is the Python script to mount Azure Blob Storage - 

dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})


# <conf-key> - 

# <conf-key> can be either fs.azure.account.key.<storage-account-name>.blob.core.windows.net or fs.azure.sas.<container-name>.<storage-account-name>.blob.core.windows.net


csv_file = "/mnt/<azureblob-mountname>/<file-name.csv>"

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(csv_file)

deltapath = "/mnt/<azureblob-mountname>/<subfolder"


df.write.format("delta").mode("append").option("path", deltapath).saveAsTable("default.mytableblob")  
