from mrjob.job import MRJob
from mrjob.step import MRStep

class bitcoin(MRJob):

    v_out_table = {}

    def mapper_join_init(self):
        with open("voutFiltered.txt") as f:
            for line in f:
                fields = line.split(",")
                #lines_stripped = [1:-1]
                #enteries = lines_stripped.split(",")
                Hash = fields[0]
                n = fields[2]
                self.v_out_table[Hash] = n

    def mapper_repl_join(self, _, line):
        fields=line.split(",")
        try:
            txid = fields[0]
            tx_hash = fields[1]
            vout = fields[2]
            if txid in self.v_out_table:
                yield(None,(txid, tx_hash, vout))
        except:
            pass

    def reducer(self,key,tuples):
        for x in tuples:
            print("{},{},{}".format(x[0], x[1], x[2]))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(reducer=self.reducer)]

if __name__ == '__main__':
    #bitcoin.JOBCONF = { 'mapreduce.job.reduces':'1'}
    bitcoin.run()
