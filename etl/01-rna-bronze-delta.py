# Databricks notebook source
# MAGIC %md
# MAGIC # RNA Expression Data Ingest and Analysis
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 0px;">
# MAGIC   <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/rna_seq_ref_arch.png" width="1000">
# MAGIC </div>
# MAGIC 
# MAGIC In this notebook we ingest data from [TCGA](https://portal.gdc.cancer.gov/) that has been downloaded using `./gdc-client` [data transfer tools](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)
# MAGIC and write to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Configuration

# COMMAND ----------

# DBTITLE 0,Configuration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

import numpy as np
import pandas as pd
import os

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

#get current user
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

# MAGIC %md 
# MAGIC Specify paths and enviroment variables

# COMMAND ----------

# DBTITLE 0,Specify Paths
raw_input_path = "dbfs:/home/{}/data/genomics/rna".format(user)
expression_path = raw_input_path+'/expression/'
sample_sheet_path = raw_input_path+'/gdc_sample_sheet.2020-02-20.tsv'
metadata_path = raw_input_path+'/metadata.cart.2020-02-26.json'
clinical_path = raw_input_path+'/clinical.tsv'

#path that will store the Bronze layer
delta_bronze_path = "dbfs:/home/{}/delta/genomics/rna/bronze".format(user)

# COMMAND ----------

os.environ["EXPRESSION"] = expression_path.replace('dbfs:','/dbfs')
os.environ["SAMPLESHEET"]=sample_sheet_path.replace('dbfs:','/dbfs')
os.environ["METADATA"]=metadata_path.replace('dbfs:','/dbfs')
os.environ["CLINICAL"]=clinical_path.replace('dbfs:','/dbfs')

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Data Ingest

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.1 Read Expression Data Counts

# COMMAND ----------

#let's explore the directory with the expression data
display(
  dbutils.fs.ls(expression_path)
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/home/rishi.ghose@databricks.com/data/genomics/rna/expression/000aa330-a5f6-4a2c-b4d5-8df4304b6fa8/

# COMMAND ----------

# MAGIC %md 
# MAGIC We can explore what one of the reads looks like head and tail, to understand how to extract data

# COMMAND ----------

# MAGIC %sh
# MAGIC zcat /dbfs/home/rishi.ghose@databricks.com/data/genomics/rna/expression/000aa330-a5f6-4a2c-b4d5-8df4304b6fa8/2d6fc33e-c553-427e-9e1c-8008f694b0ce.htseq.counts.gz | head

# COMMAND ----------

# MAGIC %sh
# MAGIC zcat /dbfs/home/rishi.ghose@databricks.com/data/genomics/rna/expression/000aa330-a5f6-4a2c-b4d5-8df4304b6fa8/2d6fc33e-c553-427e-9e1c-8008f694b0ce.htseq.counts.gz | tail

# COMMAND ----------

# MAGIC %md
# MAGIC Each file represents the RNA sequence read counts by gene id. We're interested in pulling this information across and excluding the commentary at the bottom of the file.

# COMMAND ----------

expression_schema = "gene_id string, counts int"

expressionDF = (
  spark.read.csv(expression_path+"*", sep="\t", schema=expression_schema, comment="_")
  .withColumn('_file', substring_index(input_file_name(),'/',-2)) # extracting file_id from full path
  .withColumn('id',substring_index(col('_file'), '/', 1))
  .drop('_file')
  .cache()
)

expressionDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.2 Read Metadata

# COMMAND ----------

# MAGIC %sh 
# MAGIC head $SAMPLESHEET

# COMMAND ----------

#Read in Sample Data
sampleDF = (
  spark.read.csv(sample_sheet_path,sep='\t',header=True)
  .selectExpr("`File ID` AS file_id",
              "`Project ID` AS project_id",
              "`Case ID` AS case_id",
              "`Sample Type` AS sample_type",
              "`Sample ID` AS sample_id")
)

#Read in Clinical Data
clinicalDF = spark.read.csv(clinical_path,sep='\t',header=True)

print("sample sheet count : {}\nclinical data count : {}".format(sampleDF.count(), clinicalDF.count()))

# COMMAND ----------

display(sampleDF.limit(5))

# COMMAND ----------

display(clinicalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly you can load metadata stored in `json` format into spark dataframes

# COMMAND ----------

metadataDF = spark.read.json(metadata_path, multiLine=True)

display(metadataDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Write to Delta Bronze Layer

# COMMAND ----------

df_names = {
  "expression": expressionDF,
  "gdc_sample_sheet": sampleDF,
  "clinical": clinicalDF,
  "metadata": metadataDF
}
  
# Dataframes to be written in to Bronze 
out_str="<h2>Bronze Dataframes</h2><br>"
for df_name, df in df_names.items():
  out_str+='<b>{}</b> with <i style="color:Green;">{}</i> columns <br>'.format(df_name, len(df.columns))

displayHTML(out_str)

# COMMAND ----------

#create a bronze writer
def rna_bronze_writer(df, df_name):
  bronze_table = (
    df.write.format("delta")\
    .option("userMetadata", "creating-{}-deltatable".format(df_name))
    .mode("overwrite")
    .save(os.path.join(delta_bronze_path, df_name))
  ) 
  return bronze_table

for df_name, df in df_names.items():
  rna_bronze_writer(df, df_name)

# COMMAND ----------

display(dbutils.fs.ls(delta_bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Quick Data Exploration
# MAGIC * Explore how many samples there are by projects
# MAGIC * Explore size of the raw BAM files based on the metadata

# COMMAND ----------

display(
  spark.sql("""
  SELECT project_id, count(*) TotalSamples FROM delta.`{}` 
  GROUP BY project_id
  ORDER BY TotalSamples DESC
  """.format(os.path.join(delta_bronze_path, "clinical")))
)

# COMMAND ----------

metadataDF = spark.read.format("delta").load(os.path.join(delta_bronze_path, "metadata"))

display(
  metadataDF
  .select(explode('analysis.input_files').alias('input_files'))
  .select('input_files.data_format','input_files.platform','input_files.file_size')
)

# COMMAND ----------

