# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC ## Download gene expression profiles and clinical data using [GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)
# MAGIC 
# MAGIC The Genomic Data Commons data portal's interface is first used to create a manifest file. This contains the UUID(s) of the gene expression profiles we would like to download.
# MAGIC 
# MAGIC <img src="https://rishi-public-resources.s3-eu-west-1.amazonaws.com/images/gdc.png" width=1200/>

# COMMAND ----------

# DBTITLE 1,prepare environment
import os

#get current user
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

# MAGIC %md
# MAGIC To download expression data, you first create a manifest file on [GDC Portal](https://portal.gdc.cancer.gov/) and put the file on [dbfs](https://docs.databricks.com/data/databricks-file-system.html#databricks-file-system-dbfs)

# COMMAND ----------

script_path='/dbfs/home/{}/tmptools'.format(user)
output_path='/dbfs/home/{}/data/genomics/rna/expression'.format(user)
manifest_path='/dbfs/home/rishi.ghose@databricks.com/data/genomics/rna/gdc_manifest_20200224_192255.txt' #change this path to where you put the manifest file
dbutils.fs.mkdirs(script_path.replace('/dbfs/','dbfs:/'))
dbutils.fs.mkdirs(output_path.replace('/dbfs/','dbfs:/'))

# COMMAND ----------

# MAGIC %fs head dbfs:/home/rishi.ghose@databricks.com/data/genomics/rna/gdc_manifest_20200224_192255.txt

# COMMAND ----------

# DBTITLE 1,Read in the manifest file
schema = "id string, filename string, md5 string, size int, state string"

manifest = spark.read\
  .option("header", True)\
  .option("schema", schema)\
  .option("delimiter", "\t")\
  .csv(manifest_path.replace('/dbfs/','dbfs:/'))

#get count of profiles
print("The are a total of {} profiles in the manifest".format(manifest.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC The next step is to define functions that download the [GDC Client](https://docs.gdc.cancer.gov/Data_Transfer_Tool/Users_Guide/Getting_Started/) and download profiles to [dbfs](https://docs.databricks.com/data/databricks-file-system.html#databricks-file-system-dbfs).

# COMMAND ----------

#for download GDC client
def get_gdc_client(script_path):
  
  command_str = """
  cd {};
  wget https://gdc.cancer.gov/files/public/file/gdc-client_v1.6.0_Ubuntu_x64-py3.7.zip;
  unzip gdc-client_v1.6.0_Ubuntu_x64-py3.7.zip;
  unzip gdc-client_v1.6.0_Ubuntu_x64.zip
  """
  command=command_str.format(script_path)
  print(command)
  os.system(command)

# COMMAND ----------

#to dowload a profile based on its id
def download_data(script_path, fileid, output_path):
  
  command_str="""
  cd {};
  {}/gdc-client download {}
  """
  
  command = command_str.format(output_path, script_path, fileid)
  print(command)
  os.system(command)

# COMMAND ----------

#download the GDC-client in to DBFS
get_gdc_client(script_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are over 9000 files in the manifest that need to be downloaded, these are roughly 25 MB each. For 9000 files that's over 200GB of data, which could take some time. To make this more efficient we could utilise the scaling capabilities of Spark to paralleise this process.

# COMMAND ----------

#set this to a factor of the cores in your cluster
parallelism = 1728

#loop through partitioned manifest and download files in parallel
manifest.select("id").repartition(parallelism)\
  .foreach(lambda x: download_data(script_path,x[0], output_path))

# COMMAND ----------

#Let's examine the output path
display(
  dbutils.fs.ls(output_path.replace("/dbfs","dbfs:"))
)

# COMMAND ----------

len(dbutils.fs.ls(output_path.replace("/dbfs","dbfs:")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next steps are to ingest these profiles and explore the data

# COMMAND ----------

def cleanup():
  dbutils.fs.rm(output_path.replace('/dbfs/','dbfs:/'), True)
  dbutils.fs.rm(script_path.replace('/dbfs/','dbfs:/'), True)
  dbutils.fs.mkdirs(output_path.replace('/dbfs/','dbfs:/'))
  dbutils.fs.mkdirs(script_path.replace('/dbfs/','dbfs:/'))
  return

#cleanup()