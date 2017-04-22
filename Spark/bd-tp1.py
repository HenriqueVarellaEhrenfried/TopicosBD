import urllib
import ssl
import csv
import datetime as dt
# import matplotlib.pyplot as pp
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql import Row
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

# SparkContext.setLogLevel("ERROR")
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
newData = data[0].select("empreendimento","unidade_federativa","orgao_responsavel","tipo","estagio","geometria")
x = 1
for x in range(1,len(data)):
    newData = newData.union(data[x].select("empreendimento","unidade_federativa","orgao_responsavel","tipo","estagio","geometria"))



print "Traduzindo Features de string para inteiro\n"
estados = newData.select("unidade_federativa").distinct().where("unidade_federativa != ''").collect()
estagios = newData.select("estagio").distinct().where("estagio != ''").collect()
tipos = newData.select("tipo").distinct().where("tipo != ''").collect()
orgao_responsavel = newData.select("orgao_responsavel").distinct().where("orgao_responsavel != ''").collect()

dicUF = {}
dicEstagio = {}
dicTipo = {}
dicResp = {}
def returnDic(array):
    i = 0
    dic = {}
    for a in array:
        if (not (dic.has_key(a[0]))):
            dic[a[0]] = i
            i += 1
    return dic
dicUF = returnDic(estados)
dicEstagio = returnDic(estagios)
dicTipo = returnDic(tipos)
dicResp = returnDic(orgao_responsavel)

newData2 = newData.withColumn("uf_code", dicUF[newData.col("unidade_federativa")]) 
newData2.printSchema()
print "Vou mostrar na tela: \n"

# newData.show(1)
# print estados
print "Mostrei!! \n"




# del a
# del b
# del c
# del estados
# del estagios
# del tipos
# del orgao_responsavel
# del dfs




#print "Comecando ML\n"
#beforeML = datetime.now()
#print str(beforeML) + " Quando comecou o ML\n"


#features = ['uf_code', 'stages', 'type']
#assembler = VectorAssembler(inputCols=features, outputCol="features")
#dataFinal = assembler.transform(df)

#print "Criando vetor de features\n"
# features = ['uf_code', 'stages', 'type']
#print "Criando Assmebler\n"
# assembler = VectorAssembler(inputCols=features, outputCol="features")
#print "Usando assembler para criar novo DF\n"
# dataFinal = assembler.transform(df)
#print "Iniciando o classificador Decision Tree\n"
#dt = DecisionTreeClassifier(labelCol='type', featuresCol='features', maxDepth=5)
# print "Vou mostrar na tela \n"
# df.show(2)
#df.write.csv("completeFile_original.csv")
#print "Definindo porcao de treino e de testes\n"
#(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
#print "Rodando Fit\n"
#model = dt.fit(treinamento)
#print "Predizendo\n"
#predictions = model.transform(teste)
#print "Tudo pronto\n"
#afterML = datetime.now()
#print "Salvando modelo\n"
#model.save("modelo")
#print str(afterML) + " Quando terminou o ML, totalizando: " + str(afterML-beforeML)
print "Fim do programa"