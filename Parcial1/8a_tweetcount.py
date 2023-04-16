from mrjob.job import MRJob
import time


class Tweetaverage(MRJob):

    # Mapper: Funcion de mapeo de valores para Mapredux
    #Input: lineas de archivo leido
    #Output: Key - Hora
    #        Value - Entero unitario
    def mapper(self, _, line):
        fields = line.split(";")
        time_epoch = int(fields[0])/1000
        hour = time.strftime("%H",time.gmtime(time_epoch))
        yield(hour, 1)


    # Reducer: Funcion de reduccion para emision de conteo de tweets
    #Input: Key - Hora
    #       Value - Valor unitario
    #Output: Key - Hora
    #        Value - Valor total de conteo
    def reducer(self, key, value):
        total = sum(value)
        yield(key, total)

if __name__ == "__main__":
    Tweetaverage.run()