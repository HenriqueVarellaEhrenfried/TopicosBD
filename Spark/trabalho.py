import urllib
import ssl
import csv
import datetime as dt
import matplotlib.pyplot as pp

url_alvaras_ctba="http://multimidia.curitiba.pr.gov.br/dadosabertos/BaseAlvaras/2017-03-01_Alvaras-Base_de_Dados.CSV"
url_156="http://multimidia.curitiba.pr.gov.br/dadosabertos/156/2017-03-01_156_-_Base_de_Dados.csv"
url_unidades_atendimento_ativas="http://multimidia.curitiba.pr.gov.br/dadosabertos/UnidadesAtendimentoCuritiba/2017-02-21_Unidades_Atendimento_Ativas_Curitiba_-_Base_de_Dados.csv"
url_feiras_ctba="http://multimidia.curitiba.pr.gov.br/dadosabertos/feirascuritiba/2016-12-05_Feiras_Curitiba_-_Base_de_Dados.xls"
url_guarda_municipal="http://multimidia.curitiba.pr.gov.br/dadosabertos/Sigesguarda/2017-03-01_sigesguarda_-_Base_de_Dados.csv"
url_eventos_ctba="http://multimidia.curitiba.pr.gov.br/dadosabertos/agendapmc/2017-03-24_Eventos_-_Base_de_Dados.csv"

response1 = urllib.urlretrieve(url_alvaras_ctba, "alvara.csv")
response2 = urllib.urlretrieve(url_156, "156.csv")
response3 = urllib.urlretrieve(url_unidades_atendimento_ativas, "atendimentoAtivo.csv")
response5 = urllib.urlretrieve(url_guarda_municipal, "guarda.csv")
response6 = urllib.urlretrieve(url_eventos_ctba, "eventos.csv")

alvara = spark.read.format("com.databricks.spark.csv")\
    .option('delimiter', ';')\
    .option('charset', 'iso-8859-1')\
    .option('header', "true")\
    .load("alvara.csv")
prefeitura = spark.read.format("com.databricks.spark.csv")\
    .option('delimiter', ';')\
    .option('charset', 'iso-8859-1')\
    .option('header', "true")\
    .load("156.csv")
atendimento = spark.read.format("com.databricks.spark.csv")\
    .option('delimiter', ',')\
    .option('charset', 'ascii')\
    .option('header', "true")\
    .load("atendimentoAtivo.csv")
guarda = spark.read.format("com.databricks.spark.csv")\
    .option('delimiter', ';')\
    .option('charset', 'iso-8859-1')\
    .option('header', "true")\
    .load("guarda.csv")
eventos = spark.read.format("com.databricks.spark.csv")\
    .option('delimiter', ';')\
    .option('charset', 'iso-8859-1')\
    .option('header', "true")\
    .load("eventos.csv")
    
print "Alvara"
alvara.printSchema()
print "-----------------"
print "156"
prefeitura.printSchema()
print "-----------------"
print "Atendimento"
atendimento.printSchema()
print "-----------------"
print "Guarda"
guarda.printSchema()
print "-----------------"
print "Eventos"
eventos.printSchema()
print "-----------------"