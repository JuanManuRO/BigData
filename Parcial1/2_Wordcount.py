from mrjob.job import MRJob
import re
import os

WORD_REGEX = re.compile(r"\b\w+\b")

class Max20common(MRJob):

    # Mapper: Funcion de mapeo de valores para Mapredux
    #Input: lineas de archivo leido
    #Output: Key - palabra en minusculas 
    #        Value - Entero unitario
    def mapper(self, _, line):
        name = os.path.basename(os.environ['map_input_file']) #Extraer nombre del archivo
        words = WORD_REGEX.findall(line)
        for word in words:
            if len(word) > 5:
                yield (word.lower(), (1,name)) #Retorna dupla de unidad y nombre para conteo e index invertido
    

    # Combiner: Funcion de combinacion de pares para el conteo
    # Input: Key - palabra en minusculas 
    #        Value - Entero unitario
    # Output: Key - parametro nulo
    #         Value - par de suma de valores y palabra
    def combiner(self, key, value):
        self.aux = set() # Filtrar por valores unicos con set
        total = 0
        for i in value:
            self.aux.add(i[1])
            total += i[0]

        self.aux = list(self.aux) # Convertir en lista para ser compatible con json de MRJob
        yield(None, (total, key, self.aux))
    # Funcion combiner se utiliza para realizar un primer filtro de total. Tambien en caso de
    # presentar mas de un libro den el paso de combiner se permite adicionar un filtro. Las
    # pruebas con 15 libros encontraron que se realiza una iteracion por libro asi que puede
    # generarse error al tener dos libros en combiner por la iteracion en reducer


    # Reducer: Funcion de reduccion para emision de top 20 palabras
    #Input: Key - parametro nulo
    #       Value - par de suma de valores y palabra
    #Output: Key - palabra en minusculas 
    #        Value - Valor total de conteo
    def reducer(self, key, values):
        self.revisadas = []
        self.top = [] # Auxiliar de top 20
        self.aux = [x for x in values if True] # Convertir values en lista, como combiner parece iterar por libro no se ha requerido filtros
        self.aux2 = []
        self.comunes = []

        for val in self.aux: # Se procede a realizar una agrupacion de conteo de palabras y libros encontrados
            if val[1] not in self.revisadas:
                self.revisadas.append(val[1])
                self.aux2.append(val)
            else:
                ind = self.revisadas.index(val[1])
                self.aux2[ind][0] += val[0]
                self.aux2[ind][2].append(val[2])
        
        for val in self.aux2: # Filtro de palabras que se encuentren en los 15 libros
            if len(val[2]) == 15:
                self.comunes.append(val)

        i = 0
        for i in range(20): #Iterar hasta encontrar top 20
            maxword = max(self.comunes)
            i += 1
            self.top.append(maxword)
            self.comunes.remove(maxword)

        for k in self.top:
            yield(k[1], k[0]) #Emitir top 20

if __name__ == "__main__":
    Max20common.run()

# Ejecucion
# python3 2_Wordcount.py input/ > 3_out.txt