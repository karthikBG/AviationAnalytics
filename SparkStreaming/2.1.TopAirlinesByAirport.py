from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import signal

# if (contents.length > 0 && !contents[0].equalsIgnoreCase("year") && !contents[18].equalsIgnoreCase("1")) {
#     String origin = contents[15];
#     int delay = (int) (Float.parseFloat(contents[14]));
#     String destinationDelay = contents[8] + "_" + delay;
#     context.write(new Text(origin), new Text(destinationDelay));
# }

top = []
top_airports_table = "TopAirlinesByAirport"


def get_airport_carrier_delay(content):
    data = content[1].split(',')

    try:
        if len(data) > 0 and not data[0] == 'year' and not data[18] == '1\n':
            origin_carrier = data[15] + "_" + data[8]
            destination_delay = float(data[14])
            return [(origin_carrier, (destination_delay, 1))]
    except:
        return []


def init_cassandra():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('tp')


def top_complex_average(rdd):
    global top

    chandle = init_cassandra()

    # iterate locally on driver (master) host
    curr = rdd.toLocalIterator()
    # concat top and curr values
    top_dict = dict(top)
    total = 0
    for el in curr:
        total += 1
        key = el[0].split('-')[0]
        subkey = el[0].split('-')[1]
        if key in top_dict:
            if subkey in top_dict[key]:
                top_dict[key][subkey] = (top_dict[key][subkey][0] + el[1][0], top_dict[key][subkey][1] + el[1][1])
            else:
                top_dict[key][subkey] = el[1]
        else:
            top_dict[key] = {subkey: el[1]}

    top = top_dict

    prepared_stmt = chandle.prepare(
        'INSERT INTO {} (airport_name,airline_name) values (?, ?, ?)'.format(top_airports_table))
    for origin in top:
        carriers = ' '.join(["%s=%0.2f" % (el[0], el[1][0] / el[1][1]) for el in
                             sorted(top[origin].items(), key=lambda el: el[1][0] / el[1][1])][10])
        chandle.execute(prepared_stmt, (origin, carriers))

    chandle.shutdown()


def stop_streaming():
    global ssc
    ssc.stop(stopSparkContext=True, stopGraceFully=True)


def stream_kafka():
    global ssc

    kstream = KafkaUtils.createDirectStream(ssc, topics=['2008'], kafkaParams={
        "metadata.broker.list": 'ip-172-31-12-78.us-west-1.compute.internal:6667'})

    contents = kstream.flatMap(get_airport_carrier_delay).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(top_complex_average)

    ssc.start()
    ssc.awaitTerminationOrTimeout(15000)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)


def main():
    global ssc

    conf = SparkConf()
    conf.setAppName("TopAirports")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "0")
    conf.set('spark.streaming.stopGracefullyOnShutdown', True)

    sc = SparkContext(conf=conf)

    ssc = StreamingContext(sc, 1)  # Stream every 1 second
    ssc.checkpoint("/tmp/checkpoint")

    signal.signal(signal.SIGINT, stop_streaming)

    stream_kafka()


if __name__ == "__main__":
    main()
