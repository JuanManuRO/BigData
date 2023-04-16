from mrjob.job import MRJob

class Authorcount(MRJob):

    # Mapper: Funcion de mapeo de valores para Mapredux
    # Input: lineas de archivo leido
    # Output: Key - Nombre de autor
    #         Value - Entero unitario
    def mapper(self, _, line):
        if 'Author:' in line:
            author_break = line.split(' ', 1)
            yield(author_break[1],1)

    # Combiner: Funcion de combinacion para reducir carga en transito
    # Output: Key - Nombre de autor
    #         Value - Entero unitario
    # Input: Key - Nombre de autor
    #        Value - Entero unitario
    def reducer(self, key, value):
        total = sum(value)
        yield(key,total)
    # El uso de combiner es necesario a nivel de bigdata pensando en 
    # una cantidad de libros y nodos mucho mayor para reducir los mas
    # posible el trafico en la red. Sin embargo el impacto en este caso
    # no es tan grande debido a que se reducen grandes cantidades de
    # datos desde el mapper al convertir libros enteros unicamente el
    # nombre del autor
        

    # Reducer: Funcion de reduccion para emision de top 20 palabras
    # Input: Key - Nombre de autor
    #        Value - Valor de conteo
    # Output: Key - Nombre de autor
    #         Value - Valor total de conteo
    def reducer(self, key, value):
        total = sum(value)
        yield(key,total)


if __name__ == "__main__":
    Authorcount.run()