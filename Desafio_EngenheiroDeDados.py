#!/usr/bin/env python
# coding: utf-8

# In[106]:


#IMPORTANDO OS MÓDULOS
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
spark = SparkSession.builder.master("local[*]").getOrCreate()


# In[107]:


#LENDO O DATASET DE JULHO

dfJul = spark.read    .format("com.databricks.spark.csv")    .option("header", "false")     .option("delimiter", " ")     .option("inferSchema", "true")     .load("C:/Users/Diego/NASA_access_log_Jul95")


# In[108]:


#LENDO O DATASET DE AGOSTO

dfAug = spark.read    .format("com.databricks.spark.csv")    .option("header", "false")     .option("delimiter", " ")     .option("inferSchema", "true")     .load("C:/Users/Diego/NASA_access_log_Aug95")


# In[109]:


#JUNTANDO OS DATASETS

df = dfJul.union(dfAug)

df.show(7)


# In[110]:


#SEPARAÇÃO E TRATAMENTO DOS DADOS

split_col = f.split(df['_c3'], ':')
df = df.withColumn('_c3', split_col.getItem(0))

df = df.withColumn('_c3', df['_c3'].substr(2, 11))

df = df.selectExpr('_c0 as host', '_c3 as date', '_c6 as code', '_c7 as size')

df.show(7)


# In[111]:


#CONTAGEM DOS HOST'S UNICOS = 137979

df.select('host').distinct().count()


# In[112]:


#TOTAL DE ERROS 404 = 20871

df.where(df['code'] == "404").count()


# In[113]:


#5 URL QUE MAIS CAUSARAM ERROS
#|hoohoo.ncsa.uiuc.edu
#|piweba3y.prodigy.com
#|jbiagioni.npt.nuw...
#|piweba1y.prodigy.com
#|www-d4.proxy.aol.com

df.where(df['code'] == "404").groupBy("host").count().orderBy("count", ascending=False).show(5)
df.where(df['code'] == "404").groupBy("date").count().orderBy("date", ascending=True).show()


# In[114]:


#TOTAL DE BYTES RETORNADOS = 

total = df.groupBy().agg(f.sum('size'))
total.withColumn('sum(size)', total['sum(size)'].cast(DecimalType(18, 0))).show()

dfJul.count()

dfAug.count()


# In[ ]:




