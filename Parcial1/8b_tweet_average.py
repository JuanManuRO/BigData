from mrjob.job import MRJob


class Tweetaverage(MRJob):

    # Mapper: Funcion de mapeo de valores para Mapredux
    #Input: lineas de archivo leido
    #Output: Key - Nulo
    #        Value - Dupla de longitud con valor unitario de conteo
    def mapper(self, _, line):
        fields = line.split(";")
        length = len(fields[3])
        yield(None, (length, 1))


    # Reducer: Funcion de reduccion para emision de conteo de tweets
    #Input: Key - Nulo
    #       Value - Dupla de longitud con valor unitario de conteo
    #Output: Key - "avg"
    #        Value - Valor promedio de longitud de tweets
    def reducer(self, key, value):
        self.aux = [x for x in value if True]
        total = 0
        for i in self.aux:
            total += i[0]*i[1]
        yield('avg', total/len(self.aux))

if __name__ == "__main__":
    Tweetaverage.run()