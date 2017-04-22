import csv
import datetime
import pydotplus
import random
import numpy as np
import matplotlib.pyplot as plt
from sklearn import tree

def splitArray(array, percentage):
    length = len(array)
    newLen1 = int(length*percentage)
    newLen2 = length - newLen1
    newArr = []
    i = 0
    aux = []
    for x in range(0,length):
        if(i == newLen1):
            newArr.append(aux)
            aux = []
        aux.append(array[x])
        i += 1

    newArr.append(aux)
    return (newArr[0],newArr[1])

def readCSV(file):
    turistico = []
    with open(file, 'rb') as csvfile:
        mycsv = csv.reader(csvfile, delimiter=',')
        for row in mycsv:
            turistico.append(row)
    return turistico

turistico = readCSV('dadosTuristicos.csv')

# label = {type: 15}
# features = {responsable: 16, uf_code: 17, stages: 18}
# names_features = {orgao_responsavel: 5, unidade_federativa: 7, estagio: 11}
# names_label = {tipo: 4}

features_labels = []
features = []
labels = []

for x in range(1,len(turistico)):
    labels.append(turistico[x][15])
    features_labels.append([turistico[x][15], turistico[x][16],turistico[x][17],turistico[x][18]])

labels_unique = np.unique(labels).tolist()

for x in range(0,random.randint(1,10)):
    random.shuffle(features_labels)

(treino,teste) = splitArray(features_labels, 0.8)

treino_features = []
treino_label = []
for x in treino:
    treino_features.append(x[1:len(x)])
    treino_label.append(x[0])

teste_features = []
teste_label = []
for x in teste:
    teste_features.append(x[1:len(x)])
    teste_label.append(x[0])


beforeML = datetime.datetime.now() - datetime.timedelta(hours=3)
print "\n\nComecou as: " + str(beforeML) + "\n"
clf = tree.DecisionTreeClassifier()
clf = clf.fit(treino_features,treino_label)
afterML = datetime.datetime.now() - datetime.timedelta(hours=3)
print "Terminou as: " + str(afterML) + " totalizando: " + str(afterML-beforeML) + "\n"

with open("turistico.dot","w") as f:
    f = tree.export_graphviz(clf, out_file=f)

dot_data = tree.export_graphviz(clf, out_file=None)
graph = pydotplus.graph_from_dot_data(dot_data) 
graph.write_pdf("turisticoPDF.pdf") 

a = treino_features + teste_features
b = treino_label + teste_label
predictions = clf.predict(a)

for x in range(0, len(predictions)):
    print "Predicted: " + str(predictions[x]) + " | Class: " + str(b[x]) + " | STATUS: " + "RIGHT\n" if predictions[x]==b[x] else "WRONG\n"



proba = clf.predict_proba(a)
print proba

