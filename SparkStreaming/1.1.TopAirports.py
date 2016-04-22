from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import signal

# if (contents.length > 0 && !contents[0].equalsIgnoreCase("year") && !contents[18].equalsIgnoreCase("1")) {
#   context.write(new Text(contents[15]), new IntWritable(1));
#   context.write(new Text(contents[16]), new IntWritable(1));
# }

ssc = None


def get_airports(content):
    data = content[1].split(',')
    if len(data) > 0 and not data[0] == 'year' and not data[18] == '1\n':
        return [(data[15], 1), (data[16], 1)]
    else:
        return []


def update_count(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def top_airports(rdd):
    print ('=' * 90)
    airports = rdd.takeOrdered(10, key=lambda p: -p[1])
    for airport in airports:
        print airport
    print ('=' * 90)


def stop_streaming():
    global ssc
    ssc.stop(stopSparkContext=True, stopGraceFully=True)


def stream_kafka():
    global ssc

    kstream = KafkaUtils.createDirectStream(ssc, topics=['2008'], kafkaParams={
        "metadata.broker.list": 'ip-172-31-12-78.us-west-1.compute.internal:6667'})
    # kstream = KafkaUtils.createStream(ssc, "localhost:2181", "raw-event-streaming-consumer", {"2008":1})
    contents = kstream.flatMap(get_airports).updateStateByKey(update_count)
    contents.foreachRDD(lambda rdd: top_airports(rdd))

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
