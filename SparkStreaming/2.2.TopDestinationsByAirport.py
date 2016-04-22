from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import operator
import os
import sys
import time

#if (contents.length > 0 && !contents[0].equalsIgnoreCase("year") && !contents[18].equalsIgnoreCase("1")) {
#   context.write(new Text(contents[15]), new IntWritable(1));
#   context.write(new Text(contents[16]), new IntWritable(1));
#}

top_airports = []
top_airports_table = "TopAirports"

# From Forum user Alex
has_data = time.time()
no_data = time.time()

def get_airports(content):
    data = content[1].split(',')
    # print "*************" + str(data) + "********"
    if len(data) > 0 and not data[0] == 'year' and not data[18] == '1\n':
        return [(data[15], 1), (data[16], 1)]
    else:
        return []

def init_cassandra():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('tp')

def get_top_airports(rdd):
    global top_airports
    global has_data

    chandle = init_cassandra()

    current_top = rdd.toLocalIterator()
    top_dict = dict(top_airports)
    total = 0

    for c in current_top:
        total += 1
        airport = c[0]

        if airport in top_dict:
            top_dict[airport] += c[1]
        else:
            top_dict[airport] = c[1]

    top_airports = top_dict.items()

    if total > 0:
        has_data = time.time()

    cinsert = chandle.prepare('INSERT INTO {} (Airport, Count) values (?, ?)'.format(top_airports_table))
    for t in top_airports:
        chandle.execute(cinsert, (t[0], t[1]))

    chandle.shutdown()

def updateFunction(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

def print_rdd(rdd):
    global has_data
    print ('=' * 90)
    airports = rdd.takeOrdered(10, key = lambda x: -x[1])
    if len(airports) > 0:
        has_data = time.time()
    for airport in airports:
        print airport
    print ('=' * 90)

def stream_kafka(ssc):
    global has_data

    kstream = KafkaUtils.createDirectStream(ssc, topics = ['2008'], kafkaParams = {"metadata.broker.list": 'ip-172-31-12-78.us-west-1.compute.internal:6667'})
    # kstream = KafkaUtils.createStream(ssc, "localhost:2181", "raw-event-streaming-consumer", {"2008":1})
    # contents = kstream.flatMap(get_airports).reduceByKey(lambda a, b: a+b).foreachRDD(get_top_airports)
    contents = kstream.flatMap(get_airports).updateStateByKey(updateFunction)

    contents.foreachRDD(lambda rdd: print_rdd(rdd))

    ssc.start()
    while True:
        res = ssc.awaitTerminationOrTimeout(5)
        if res:
           break
        else:
            if time.time() - has_data > 30:
                print("No more data to stream")
                ssc.stop(stopSparkContext=True, stopGraceFully=True)
                break

def main():
    conf = SparkConf()
    conf.setAppName("TopAirports")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "0")
    conf.set("spark.dynamicAllocation.enabled", "true")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 1) # Stream every 1 second
    ssc.checkpoint("checkpoint")

    # Clear the cassandra table
    init_cassandra().execute('TRUNCATE {}'.format(top_airports_table))

    stream_kafka(ssc)

if __name__ =="__main__":
    main()
