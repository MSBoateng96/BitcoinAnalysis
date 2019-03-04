from mrjob.job import MRJob

class bitcoin(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if len(fields) == 4:
                hash = fields[0]
                value = fields[1]
                n = fields[2]
                publicKey = fields[3]

            yield(None, (hash,value,n,publicKey))

        except:
            pass

    def reducer(self, _, values):
        filter_key = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
        filtered_values = filter(lambda tup:tup[3] == filter_key, values)
        for x in filtered_values:
            print("{},{},{},{}".format(x[0], x[1], x[2], x[3]))

if __name__ == '__main__':
    #bitcoin.JOBCONF = { 'mapreduce.job.reduces':'1'}
    bitcoin.run()
