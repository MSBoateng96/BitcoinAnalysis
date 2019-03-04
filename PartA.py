from mrjob.job import MRJob
import time

class bitcoin(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            #transactions.csv has 5 columns
            if len(fields) == 5: #Verify a row has 5 columns
                timestamp = int(fields[2]) 
                month = time.strftime('%M-%Y', time.gmtime(timestamp))

            yield(month, 1)
        except:
            pass
    
    def reducer(self, features, values):
        no_of_transcations = sum(values) 
        yield(features, no_of_transcations)

if __name__ == '__main__':
    #bitcoin.JOBCONF = { 'mapreduce.job.reduces':'1'}
    bitcoin.run()