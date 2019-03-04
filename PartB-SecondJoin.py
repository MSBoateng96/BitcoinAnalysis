from mrjob.job import MRJob
from mrjob.step import MRStep

class bitcoin(MRJob):

    v_in_table = {}

    def mapper_join_init(self):
        with open("/Users/Michael/Desktop/BigData_coursework/FirstJoinResult.txt") as f:
            for line in f:
                fields = line.split(",")
                tx_hash = fields[1] #vin values
                vout = fields[2] #vin values
                self.v_in_table[tx_hash] = vout

    def mapper_repl_join(self, _, line):
        fields=line.split(",")
        try:
            Hash = fields[0]
            value = fields[1]
            n = fields[2]
            publicKey = fields[3]
            if (Hash in self.v_in_table):
                if (n == self.v_in_table[Hash]):
                    yield(publicKey, value)

        except:
            pass
    
    def reducer_sums(self,row,value):
        sum_values = sum(value)
        yield(row,sum_values)
    
    def mapper_top(self,row,value):
        yield(row,value)

    def reducer(self,key,tuples):
        results = sorted(tuples, reverse=True, key=lambda x:x[1])[:10]
        for x in results:
            yield(x[0], x[1])

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                       mapper=self.mapper_repl_join,
                       reducer=self.reducer_sums),
                MRStep(mapper=self.mapper_top,
                       reducer=self.reducer)]

if __name__ == '__main__':
    #bitcoin.JOBCONF = { 'mapreduce.job.reduces':'1'}
    bitcoin.run()
