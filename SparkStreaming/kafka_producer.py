from kafka import SimpleProducer, KafkaClient, KafkaProducer
import sys

def try_send():

    producer = KafkaProducer(bootstrap_servers="ip-172-31-12-78.us-west-1.compute.internal:6667")
    # client = KafkaClient("ip-172-31-12-78.us-west-1.compute.internal:6667")
    # producer = SimpleProducer(client, async=True, batch_send_every_n = 100, batch_send_every_t = 60, random_start=False)
    # producer = SimpleProducer(client)
    # connect_str = 'ip-172-31-12-78.us-west-1.compute.internal:6667'
    # producer = KafkaProducer(bootstrap_servers=connect_str,
    #                         max_block_ms=10000,
    #                         value_serializer=str.encode)

    topic = '2008'
    with open('/home/ec2-user/data/2008.csv') as f:
        for line in f:
            producer.send(topic, line)

    producer.flush()
    producer.close()
    # producer.stop(timeout=None)

def main():
    try_send()

if __name__ == '__main__':
    #logging.basicConfig(format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    #level=logging.DEBUG
    #)
    sys.exit(main())
