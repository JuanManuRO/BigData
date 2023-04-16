import pyspark
import re

# Creacion de entorno de spark
sc = pyspark.SparkContext()

# Carga de datos
nasdaq = sc.textFile("input/NASDAQsample.csv")
comp = sc.textFile("input/companylist.tsv")

# Se extraen los simbolos y sectores de companylist para crear un diccionarios entre estos dos elementos
clean_comp = comp.map(lambda l: (l.split('\t')[0],l.split('\t')[3]))
comp_dic = clean_comp.collectAsMap()

# Se extraen los simbolos traducidos a sectores, el año y la cantidad de operaciones mapeados para tener llaves de (sector, año)
# Se filtran los datos con el fin de quitar aquellas compañias sin sector reportado en companylist
clean = nasdaq.map(lambda l: ((comp_dic.get(l.split(',')[1], "Unknown"), l.split(',')[2][0:4]), int(l.split(',')[7])))
filtered = clean.filter(lambda val: val[0][0] != 'Unknown' and val[0][0] != 'n/a')

# Se aplica una reduccion sumando cantidades sobre las llaves (sector, año) y se genera una llave de año con valor (sector, cantidad)
# Se define una funcion para maximizar segun la segunda entrada del valor y se reduce usando esta funcion.
reduced = filtered.reduceByKey(lambda a,b:a+b).map(lambda e: (e[0][1], (e[0][0], e[1])))
def max_quant(a,b):
    if a[1] > b[1]:
        return (a[0],a[1])
    else:
        return (b[0],b[1])
final = reduced.reduceByKey(lambda a,b: max_quant(a,b))

# Se genera un archivo de texto, se reunen los resultados finales como lista y se escriben los elementos de esta en el archivo de texto
f = open("1_out.txt", "x")
final_list = final.collect()
for res in final_list:
    f.write(res[0] + ", " + str(res[1][0]) + ", " + str(res[1][1]) + "\n")
f.close()