#! /bin/bash

while read p; # Iterar sobre lineas del archivo .txt
do
	wget $p -P input/ #Realizar operacion get sobre lista de links y guardar en directiorio input con nombre de origen
done < $1 #Aplicar sobre .txt ingresado como argumento
