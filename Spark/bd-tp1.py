import urllib
import ssl
import csv
import datetime as dt
# import matplotlib.pyplot as pp
import leather
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql import Row, Column
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlfn
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from pyspark.ml.clustering import KMeans

from collections import Counter
# from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.mllib.linalg import Vectors
# from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

from pyspark.sql.types import *
import folium 
import functools
from datetime import datetime
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from decimal import *

getcontext().prec = 4
def returnDic(array):
    i = 0
    dic = {}
    for a in array:
        if (not (dic.has_key(a[0]))):
            dic[a[0]] = i
            i += 1
    return dic

def plotDF(data, name, type="df"):
    plotData = []
    if (type != "df"):
        for d in data:
            plotData.append((d["id"], float(Decimal(d["MISSED"])/Decimal(d["TOTAL"]))*100))
        chart = leather.Chart(name)
        chart.add_columns(plotData, fill_color='#265b6a')
        chart.to_svg(name.replace(" ","_") + ".svg",900)
    else:
        df = data.collect()
        if (name != "Obras por UF e estagio"):
            for d in df:
                plotData.append((d[0], d[1]))
            chart = leather.Chart(name)
            chart.add_columns(plotData, fill_color='#265b6a')
            chart.to_svg(name.replace(" ","_") + ".svg",900)
        else:
            for d in df:
                plotData.append((d[2], d[0] + " - " + d[1]))
            chart = leather.Chart(name)
            chart.add_bars(plotData, fill_color='#265b6a')
            chart.to_svg(name.replace(" ","_") + ".svg",900,1600)

#----------------------
SparkContext.setSystemProperty('spark.executor.memory', '2g')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '5g')
SparkContext.setSystemProperty('spark.driver.memory', '5g')

sc = SparkContext("local", "App Name")

print "----\n\n\n"
spark = SQLContext(sc)

print "Lendo arquivos\n"
data = [spark.read.format("com.databricks.spark.csv").option("header", "true").load("Documentos/data_pac/aeroporto.csv", inferSchema=True),\
        spark.read.format("com.databricks.spark.csv").option("header", "true").load("Documentos/data_pac/infraturistica.csv", inferSchema=True),\
        spark.read.format("com.databricks.spark.csv").option("header", "true").load("Documentos/data_pac/cidadeshistoricas.csv", inferSchema=True)]

print "Juntando arquivos\n"
newData = data[0].select("FID","empreendimento","unidade_federativa","orgao_responsavel","tipo","estagio","geometria")
x = 1
for x in range(1,len(data)):
    newData = newData.union(data[x].select("FID","empreendimento","unidade_federativa","orgao_responsavel","tipo","estagio","geometria"))



print "Traduzindo Features de string para inteiro\n"
estados = newData.select("unidade_federativa").distinct().where("unidade_federativa != ''").collect()
estagios = newData.select("estagio").distinct().where("estagio != ''").collect()
tipos = newData.select("tipo").distinct().where("tipo != ''").collect()
orgao_responsavel = newData.select("orgao_responsavel").distinct().where("orgao_responsavel != ''").collect()

dicUF = {}
dicEstagio = {}
dicTipo = {}
dicResp = {}

dicUF = returnDic(estados)
dicEstagio = returnDic(estagios)
dicTipo = returnDic(tipos)
dicResp = returnDic(orgao_responsavel)

imp_data = newData.select("unidade_federativa","estagio","tipo","orgao_responsavel","FID").collect()
header=("uf_code","stages","type","responsable","FID")
info = []
for impD in imp_data:
    info.append((dicUF[impD[0]],dicEstagio[impD[1]],dicTipo[impD[2]],dicResp[impD[3]],impD[4]))


df = spark.createDataFrame(info, header)

completeData = (newData.join(df, df["FID"] == newData["FID"], "leftouter").drop(df["FID"]))


# print "Vou mostrar na tela: \n"

# # newData.show(1)
# # print estados
# print "Mostrei!! \n"
resultados = []

# Machine Learning
print "Comecando ML\n"
beforeML = datetime.now()

features = ['type', 'responsable', 'stages']
assembler = VectorAssembler(inputCols=features, outputCol="features")
dataFinal = assembler.transform(completeData)

dt = DecisionTreeClassifier(labelCol='uf_code', featuresCol='features', maxDepth=5)
(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
model = dt.fit(treinamento)
predictions = model.transform(teste)
print model.toDebugString
total = predictions.count()
missed = predictions.where("uf_code != prediction").count()

resultados.append({"id": "UF_CODE", "TOTAL": total, "MISSED": missed})
#------

features = ['uf_code', 'responsable', 'stages']
assembler = VectorAssembler(inputCols=features, outputCol="features")
dataFinal = assembler.transform(completeData)

dt = DecisionTreeClassifier(labelCol='type', featuresCol='features', maxDepth=5)
(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
model = dt.fit(treinamento)
predictions = model.transform(teste)
print model.toDebugString
total = predictions.count()
missed = predictions.where("type != prediction").count()

resultados.append({"id": "TYPE", "TOTAL": total, "MISSED": missed})
#------

features = ['uf_code', 'type', 'stages']
assembler = VectorAssembler(inputCols=features, outputCol="features")
dataFinal = assembler.transform(completeData)

dt = DecisionTreeClassifier(labelCol='responsable', featuresCol='features', maxDepth=5)
(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
model = dt.fit(treinamento)
predictions = model.transform(teste)
print model.toDebugString
total = predictions.count()
missed = predictions.where("responsable != prediction").count()

resultados.append({"id": "RESPONSABLE", "TOTAL": total, "MISSED": missed})
#------

features = ['uf_code', 'type', 'responsable']
assembler = VectorAssembler(inputCols=features, outputCol="features")
dataFinal = assembler.transform(completeData)

dt = DecisionTreeClassifier(labelCol='stages', featuresCol='features', maxDepth=5)
(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
model = dt.fit(treinamento)
predictions = model.transform(teste)
print model.toDebugString
total = predictions.count()
missed = predictions.where("stages != prediction").count()

resultados.append({"id": "STAGE", "TOTAL": total, "MISSED": missed})
afterML = datetime.now()
print "Tempo depois do ML: " + str(afterML) + "TOTAL: " + str(afterML-beforeML) + "\n"
# Fim Machnie Learning
print resultados

resa = completeData.select("estagio").groupBy("estagio").count()
resb = completeData.select("unidade_federativa").groupBy("unidade_federativa").count()
resc = completeData.select("unidade_federativa","estagio").groupBy("unidade_federativa","estagio").count().orderBy("unidade_federativa")


plotDF(resultados,"Porcentagem de Erro","array")
plotDF(resa,"Obras por estagio")
plotDF(resb,"Obras por UF")
plotDF(resc,"Obras por UF e estagio")

print "Fim do programa"