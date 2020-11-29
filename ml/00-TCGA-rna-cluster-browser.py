# Databricks notebook source
# MAGIC %md
# MAGIC # RNA Expression Data Ingest and Analysis
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 0px;">
# MAGIC   <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/rna_seq_ref_arch.png" width="1000">
# MAGIC </div>
# MAGIC 
# MAGIC In this notebook we will browse the expression data and create a gold table

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

#Define the Delta paths
delta_silver_path = 'dbfs:/home/{}/delta/genomics/rna/silver'.format(user)
delta_gold_path = 'dbfs:/home/{}/delta/genomics/rna/gold'.format(user)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Read data from silver tables

# COMMAND ----------

df_expression = spark.read.format("delta").load(os.path.join(delta_silver_path, "expression"))
df_clinical_meta = spark.read.format("delta").load(os.path.join(delta_silver_path, "clinical-meta"))

print("expressions with {} rows\nclinical metadata with {} rows".format(df_expression.count(), df_clinical_meta.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's explore the clinical data associated with expressions

# COMMAND ----------

display(
  df_clinical_meta.limit(10)
)

# COMMAND ----------

display(
  df_expression.select('id',size('counts_arr'),array_max('counts_arr'),array_max('norm_counts_arr'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Get data ready for clustering and visualisation

# COMMAND ----------

selected_clinical_cols=[
 'file_id',
 'project_id',
 'ethnicity',
 'gender',
 'race',
 'category',
 'classification',
 'classification_of_tumor',
 'icd_10_code',
 'masaoka_stage',
 'primary_diagnosis',
 'tissue_or_organ_of_origin',
 'tumor_grade',
 'tumor_stage',
]

df_expr_and_clinical=(
  df_expression
  .join(
    df_clinical_meta.select(selected_clinical_cols),on=df_expression.id==df_clinical_meta.file_id)
)

# COMMAND ----------

#create Pandas DataFrames for clustering
expression_pdf = df_expr_and_clinical.select("id", "norm_counts_arr").toPandas()
clinical_pdf = df_expr_and_clinical.select(selected_clinical_cols).toPandas()

n = len(expression_pdf.norm_counts_arr[0])
m = expression_pdf.shape[0]

expression_mat=np.concatenate(expression_pdf.norm_counts_arr.values,axis=0).reshape(m,n)
expression_mat.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Dimensionality Reduction Using UMAP

# COMMAND ----------

dbutils.widgets.text(name='n_neighbors',defaultValue='15')
dbutils.widgets.text(name='min_dist',defaultValue='0.1')
dbutils.widgets.text(name='n_components',defaultValue='2')
dbutils.widgets.dropdown(name='label',defaultValue='project_id',choices=selected_clinical_cols)

# COMMAND ----------

#import interactive visualisation libraries
from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import file_html

import umap
import umap.plot

import mlflow

# COMMAND ----------

params ={'n_neighbors':int(dbutils.widgets.get('n_neighbors')),
        'min_dist':float(dbutils.widgets.get('min_dist')),
        'n_components':int(dbutils.widgets.get('n_components')),
        }
mapper = umap.UMAP(**params).fit(expression_mat)

mlflow.end_run()
for key,value in mapper.get_params().items():
  mlflow.log_param(key,value)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Create an interactive cluster browser

# COMMAND ----------

assert params['n_components']==2
with mlflow.start_run(nested=True) as run:
  selected_lablel=dbutils.widgets.get('label')
  hover_data = pd.DataFrame({'index':np.arange(m), 'label':clinical_pdf[selected_lablel]})
  p = umap.plot.interactive(mapper, labels=clinical_pdf[selected_lablel], hover_data=hover_data, point_size=2)
  html = file_html(p, CDN, "TCGA Expressions")
  dbutils.fs.put('/tmp/umap.html',html,True)
  mlflow.log_artifact('/dbfs/tmp/umap.html')
  mlflow.log_param('label',selected_lablel)
  displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Write tables to Delta gold path

# COMMAND ----------

(
  df_expr_and_clinical
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_gold_path,'expressions-clinical'))
)