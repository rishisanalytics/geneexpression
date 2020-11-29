# Databricks notebook source
# MAGIC %md
# MAGIC # RNA Expression Data Ingest and Analysis
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 0px;">
# MAGIC   <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/rna_seq_ref_arch.png" width="1000">
# MAGIC </div>
# MAGIC 
# MAGIC In this notebook we read data from Delta Bronze Table and after QC write to Delta Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os

import numpy as np
import pandas as pd


#get current user
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

#Define Delta Path
delta_bronze_path = "dbfs:/home/{}/delta/genomics/rna/bronze".format(user)
delta_silver_path = "dbfs:/home/{}/delta/genomics/rna/silver".format(user)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Load Bronze Delta Tables

# COMMAND ----------

def rna_bronze_reader(bronzeTable):
  df = spark.read.format("delta").load(os.path.join(delta_bronze_path, bronzeTable))
  return df

# COMMAND ----------

expressionDF = rna_bronze_reader("expression")
clinicalDF = rna_bronze_reader("clinical")
samplesheetDF = rna_bronze_reader("gdc_sample_sheet")
metadataDF = rna_bronze_reader("metadata")

# COMMAND ----------

display(expressionDF.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Clean and Reshape Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.1 Create Expression table (sample per row) and add normalised counts

# COMMAND ----------

w = Window.partitionBy("id")

df_expression_silver = (
  expressionDF.repartition('id')
  .withColumn('_sd',stddev('counts').over(w))
  .withColumn('_avg',mean('counts').over(w))
  .withColumn('counts_stand',(col('counts')-col('_avg'))/col('_sd'))
  .drop('_sd','_avg')
  .groupBy('id').agg(collect_list('counts_stand').alias('norm_counts_arr'),collect_list('gene_id').alias('gene_id'),collect_list('counts').alias('counts_arr'))
  .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.2 Clean up metadata

# COMMAND ----------

df_meta_selected= (
  metadataDF
  .select('file_id',explode('associated_entities').alias('entities'))
  .select('file_id','entities.case_id')
  .drop_duplicates()
)

df_meta_selected.count()

# COMMAND ----------

clincial_cols=['case_id0',
 'project_id',
 'ethnicity',
 'gender',
 'race',
 'vital_status',
 'year_of_birth',
 'age_at_diagnosis',
 'ajcc_clinical_m',
 'ajcc_clinical_n',
 'ajcc_clinical_stage',
 'ajcc_clinical_t',
 'ajcc_pathologic_m',
 'ajcc_pathologic_n',
 'ajcc_pathologic_stage',
 'ajcc_pathologic_t',
 'anaplasia_present',
 'anaplasia_present_type',
 'ann_arbor_pathologic_stage',
 'case_id40',
 'case_submitter_id',
 'category',
 'classification',
 'entity_id',
 'entity_type',
 'classification_of_tumor',
 'icd_10_code',
 'masaoka_stage',
 'morphology',
 'percent_tumor_invasion',
 'primary_diagnosis',
 'synchronous_malignancy',
 'tissue_or_organ_of_origin',
 'tumor_focality',
 'tumor_grade',
 'tumor_stage'
]

df_clinical_selected = clinicalDF.select(clincial_cols).drop_duplicates()
df_clinical_selected.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.3 Merge metadata and clinical table

# COMMAND ----------

df_clinical_meta = (
  df_clinical_selected
  .join(df_meta_selected,on=df_clinical_selected.case_id0==df_meta_selected.case_id)
  .drop('case_id',)
  .join(samplesheetDF,on=['file_id','project_id'])
)

df_clinical_meta.count()

# COMMAND ----------

display(df_clinical_meta.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Explore the expression patterns

# COMMAND ----------

display(
  df_expression_silver.selectExpr('id','counts_arr[0]','counts_arr[10]','counts_arr[20]','counts_arr[30]')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Write to Silver Delta Tables

# COMMAND ----------

df_names = {
  "expression": df_expression_silver,
  "clinical-meta": df_clinical_meta
}

#create a silver table writer
def rna_bronze_writer(df, df_name):
  bronze_table = (
    df.write.format("delta")\
    .option("userMetadata", "creating-{}-deltatable".format(df_name))
    .mode("overwrite")
    .save(os.path.join(delta_silver_path, df_name))
  ) 
  return bronze_table

#write to delta
for df_name, df in df_names.items():
  rna_bronze_writer(df, df_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.1 Optimize Tables
# MAGIC Optimise compacts tables in to large chunks to avoid a small file problem. It also collects statistics on the data to enable improved query performance

# COMMAND ----------

