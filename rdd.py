import functools
import itertools
from operator import add
from partition import Partition
# from util import clean_string, parseNeighbors, computeContribs


class RDD(object):
    def __init__(self, rdd_id):
        self.id = rdd_id

    def __partitioned(self, x, num_partitions=1):
        x_len_iter, x = itertools.tee(x, 2)
        len_x = sum(1 for _ in x_len_iter)
        for i in range(num_partitions):
            start = int(i * len_x / num_partitions)
            end = int((i + 1) * len_x / num_partitions)
            if i + 1 == num_partitions:
                end += 1
            yield Partition(self.id, i, itertools.islice(x, end - start))

    def __new_rdd_id(self):
        return self.id + 1

    def flatMap(self, f):
        r = RDD(self.__new_rdd_id())
        partitions = []
        for p in self.partitions:
            for pp in p.get_content():
                partitions.extend(f(pp))
        r.partitions = list(self.__partitioned(partitions, self.get_num_partitions()))
        r.num_partitions = self.num_partitions
        return r

    def map(self, f):
        r = RDD(self.__new_rdd_id())
        r.partitions = []
        for p in self.partitions:
            rp = Partition(r.id, p.id, map(lambda x: f(x), p.get_content()))
            r.partitions.append(rp)
        r.num_partitions = len(r.partitions)
        return r

    def mapValues(self, f):
        r = RDD(self.__new_rdd_id())
        r.partitions = []
        for p in self.partitions:
            rp = Partition(r.id, p.id, map(lambda x: [x[0], f(x[1])], p.get_content()))
            r.partitions.append(rp)
        r.num_partitions = len(r.partitions)
        return r

    def filter(self, f):
        r = RDD(self.__new_rdd_id())
        r.partitions = []
        for p in self.partitions:
            rp = Partition(r.id, p.id, filter(lambda x: f(x), p.get_content()))
            if rp.get_content():
                r.partitions.append(rp)
        r.num_partitions = len(r.partitions)
        return r

    def parallelize(self, x, num_partitions=1):
        self.num_partitions = num_partitions
        self.partitions = list(self.__partitioned(x, self.num_partitions))
        return self

    def textFile(self, file_name, num_partitions=1):
        r = RDD(self.__new_rdd_id())
        r.num_partitions = num_partitions
        r.file_name = file_name
        f = open(file_name)
        r.partitions = list(self.__partitioned(f.readlines(), r.num_partitions))
        f.close()
        r.num_partitions = len(r.partitions)
        return r

    def reduceByKey(self, f):
        return self.groupByKey().mapValues(lambda x: functools.reduce(f, x))

    def groupByKey(self, num_partitions=1):
        r = RDD(self.__new_rdd_id())
        return r.parallelize((
            (k, [v[1] for v in l]) for k, l in itertools.groupby(
            sorted(self.collect()),
            lambda x: x[0],
        )
        ), num_partitions)

    def join(self, other_rdd, num_partitions=None):
        if num_partitions is None:
            num_partitions = self.get_num_partitions()
        d1 = dict(self.collect())
        d2 = dict(other_rdd.collect())
        keys = set(d1.keys()) & set(d2.keys())
        x = ((k, (d1[k], d2[k])) for k in keys)
        r = RDD(self.__new_rdd_id())
        r.partitions = list(self.__partitioned(x, num_partitions))
        return r

    def count(self):
        res = 0
        for p in self.partitions:
            res += len(p.get_content())
        return res

    def collect(self):
        res = []
        for p in self.partitions:
            res.extend(p.get_content())
        return res

    def distinct(self, numPartitions=None):
        r = RDD(self.__new_rdd_id())
        if numPartitions is None:
            r.num_partitions = self.get_num_partitions()
        else:
            r.num_partitions = numPartitions
        return r.parallelize(list(set(self.collect())),
                             r.num_partitions)

    def get_num_partitions(self):
        self.num_partitions = len(self.partitions)
        return self.num_partitions

    def get_partitions(self):
        res = []
        for p in self.partitions:
            res.append(p.get())
        return res

    def get(self):
        return {
            'rdd_id': self.id,
            'partitions': self.get_partitions()
        }


if __name__ == '__main__':
    '''
    r = RDD(1).parallelize([('hello',2),('yes',3),('hello',4),('yes',5),('hello',2)],3).reduceByKey(add)
    print r.get()
    print r.collect()
    print r.count()
    r = RDD(1).parallelize([1,2,2,3,4,4,5],3).distinct()
    print r.collect()

    rdd1 = RDD(1).parallelize([(0, 1), (1, 1)])
    rdd2 = RDD(100).parallelize([(2, 1), (1, 3)])
    print rdd1.join(rdd2).collect()
    '''
    # print("Word Count\n")
    # world_count = RDD(1).textFile('w', 5)
    # world_count = RDD(1).textFile('w', 5).flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (clean_string(word), 1)).reduceByKey(lambda a, b: a + b)
    # print world_count.collect()
    # print world_count.count()

    print("\n\nQuery Log\n")
    query_log = RDD(1).textFile('log', 5)
    parms = eval("'log',5")
    func = getattr(query_log, "textFile")(*parms)
    parms = eval("lambda line: \"error\" in line")
    func1 = func.filter(lambda line: "error" in line)
    parms = ()
    print getattr(func1, "collect")(*parms)
    print query_log.count()

    # print("\n\nPageRank\n")
    # links = RDD(1).textFile('pagerank', 2).map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()
    # ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # for iteration in range(10):
    #     j = links.join(ranks)
    #     contribs = j.flatMap(
    #         lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    #     ad = contribs.reduceByKey(lambda a, b: a + b)
    #     ranks = ad.mapValues(lambda rank: rank * 0.85 + 0.15)
    # for (link, rank) in ranks.collect():
    #     print("%s has rank: %s." % (link, rank))
