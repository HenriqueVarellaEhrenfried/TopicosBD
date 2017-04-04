import urllib
import ssl
import csv
import datetime as dt
import matplotlib.pyplot as pp
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql import Row
from pyspark.sql import SQLContext
import folium


sqlContext = SQLContext(spark)
df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("data_pac/aeroporto.csv")
# df2 = spark.read.json("data_pac/pavimentacao.json")
df2 = spark.read.json("data_pac/pavimentacao.json")


df2.registerTempTable("pavimentacao")
# df2.printSchema()

coord_a = df2.select("features.geometry.coordinates").collect()
id_a = df2.select("features.id").collect()
empreend_a = df2.select("features.properties.empreendimento").collect()
estagio_a = df2.select("features.properties.estagio").collect()
lats = []
longs = []
ids = [] 
empreends = []
estagios = []
i = 0


while(i < len(coord_a[0][0])):
    longs.append(coord_a[0][0][i][0][0])
    lats.append(coord_a[0][0][i][0][1])
    ids.append(id_a[0][0][i])
    empreends.append(empreend_a[0][0][i])
    estagios.append(estagio_a[0][0][i])
    i = i + 1
    
df2.printSchema()

lat = -25.45
lgn = -49.22

mymap = folium.Map(location=[lat,lgn], zoom_start=12)
mymap.save('/home/henrique/Documentos/map.html')

i = 0
while (i < len(lats)):
    folium.Marker([lats[i], longs[i]], popup="EMPREENDIMENTO: " + empreends[i] + " | STATUS: " + estagios[i], icon = folium.Icon(color = 'green')).add_to(mymap)
    i = i + 1
    
mymap.save('/home/henrique/Documentos/map.html')