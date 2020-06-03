# -*- coding: utf-8 -*-
import argparse
import numpy as np
import os
import subprocess

from multiprocessing import Pool
from topic import add_topic
from topic import del_topic

################################################################################
def producer_perf_test(topic, producers, args):
    """To perform producer performance test.
    Args:
        topic (str): Name of topic.
        producers (int): Number of producers.
        args (argparse.Namespace): Arguments.
    """
    cmd_list = [
        os.path.join(args.dirname, 'kafka-producer-perf-test.sh'),
        f'--topic {topic}',
        f'--num-records {args.iterations}',
        f'--record-size {args.data_size}',
         '--throughput -1',
         '--producer-props',
        f'acks={args.acks}',
        f'bootstrap.servers={",".join(args.brokers)}',
    ]
    if args.producer_props:
        cmd_list = cmd_list + args.producer_props
    command = ' '.join(cmd_list)

    result = []
    pool = Pool(processes=producers)
    for _ in range(producers):
        result.append(
            pool.apply_async(
                func=subprocess.run,
                args=(command.split(),),
                kwds={'capture_output': True,},
            )
        )
    pool.close()
    pool.join()

    print('  Producers:')
    records = 0
    rec_per_sec = 0.0
    bytes_per_sec = 0.0
    latencies = []
    for ret in result:
        ret = ret.get()
        if ret.returncode == 0:
            message = ret.stdout.decode('utf-8').split('\n')[-2]
            parts = message.split(',')
            for part in parts:
                part = part.strip()
                if part.endswith('records sent'):
                    #  50000 records sent
                    records += int(part.split(' ')[0])
                elif part.endswith(')'):
                    # 180505.415162 records/sec (17.21 MB/sec)
                    part_split = part.split(' ')
                    rec_per_sec += float(part_split[0])
                    bytes_per_sec += float(part_split[-2].replace('(', ''))
                elif part.endswith('avg latency'):
                    # 1.37 ms avg latency
                    latencies.append(float(part.split(' ')[0]))
            latency = np.mean(np.array(latencies).astype(float))
    print(f'    Records:    {records}')
    print(f'    Throughput: {rec_per_sec:.2f} rec/sec ({bytes_per_sec:.2f} MB/sec)')
    print(f'    Latency:    {latency:.2f} mS')

    if args.csv_filepath:
        with open(args.csv_filepath, 'a') as fp:
            fp.write(f'Producer,{records},{clients},{args.iterations},'
                     f'{rec_per_sec},{bytes_per_sec},{latency}\n')

################################################################################
def consumer_perf_test(topic, consumers, args):
    """To perform consumer performance test.
    Args:
        topic (str): Name of topic.
        consumers (int): Number of consumers.
        args (argparse.Namespace): Arguments.
    """
    command = ' '.join([
        os.path.join(args.dirname, 'kafka-consumer-perf-test.sh'),
        f'--broker-list={",".join(args.brokers)}',
        f'--messages {args.iterations}',
        f'--topic {topic}',
    ])

    result = []
    pool = Pool(processes=consumers)
    for _ in range(consumers):
        result.append(
            pool.apply_async(
                func=subprocess.run,
                args=(command.split(),),
                kwds={'capture_output': True,},
            )
        )
    pool.close()
    pool.join()

    print('  Consumers:')
    records = 0
    rec_per_sec = 0.0
    mega_bytes_per_sec = 0.0
    for ret in result:
        ret = ret.get()
        if ret.returncode == 0:
            # https://stackoverflow.com/questions/50753980/performance-testing-in-kafka
            message = ret.stdout.decode('utf-8').split('\n')[-2]
            parts = message.split(', ')
            records += int(parts[4]) # data.consumed.in.nMsg
            rec_per_sec += float(parts[5]) # nMsg.sec
            mega_bytes_per_sec += float(parts[3]) # MB.sec
    print(f'    Records:    {records}')
    print(f'    Throughput: {rec_per_sec:.2f} rec/sec ({mega_bytes_per_sec:.2f} MB/sec)')

################################################################################
parser = argparse.ArgumentParser(
    description='Kafka Java performance test.', 
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-b', '--brokers', 
    type=str, nargs='+', default=['localhost'],
    help='Kafka borker host[:port]. The default port is 9092.')
parser.add_argument('-d', '--dirname', 
    type=str, default='/home/nexgus/Downloads/kafka_2.12-2.4.1/bin',
    help='Path to Kafka bin directory.')
parser.add_argument('-i', '--iterations', 
    type=int, default=50000, 
    help='Record generation count for each client.')
parser.add_argument('-ds', '--data-size',
    type=int, default=100,
    help='Data size for each record.')
parser.add_argument('-a', '--acks',
    choices=['0', '1', 'all'], default='1',
    help='The number of acknowledgments the producer requires the leader '
         'to have received before considering a request complete. This '
         'controls the durability of records that are sent. The '
         'following settings are common: 0: Producer will not wait for '
         'any acknowledgment from the server. 1: Wait for leader to '
         'write the record to its local log only. all: Wait for the full '
         'set of in-sync replicas to write the record.')
parser.add_argument('-pp', '--producer-props',
    type=str, nargs='+',
    help='Additional producer properties other than acks and bootstrap.servers.')
parser.add_argument('-c', '--clients',
    type=int,
    help='Client count.')
parser.add_argument('-pt', '--partitions',
    type=int,
    help='Partition count.')
parser.add_argument('-m', '--max', 
    type=int, default=2, 
    help='Maximum client count.')
parser.add_argument('-rf', '--replication-factor',
    type=int, default=1,
    help='Replcation factor if the topic does not exist.')
parser.add_argument('-csv', '--csv-filepath',
    type=str,
    help='Path to a CSV file.')
args = parser.parse_args()

if args.clients:
    if args.partitions is None:
        raise ValueError('When clients is set, partitions must be set.')
    if args.clients < 1:
        raise ValueError('Minimum value of clients is 1.')
    if args.partitions < 1:
        raise ValueError('Minimum value of clients is 1.')

brokers = []
for broker in args.brokers:
    if ':' not in broker:
        broker = broker + ':9092'
    brokers.append(broker)
args.brokers = brokers

################################################################################
if args.csv_filepath:
    with open(args.csv_filepath, 'w') as fp:
        fp.write('Type,Records,Clients,Iterations,RecPerSec,MegaBytesPerSec,'
                 'Latency\n')

if args.clients:
    topic = f'topic{args.clients}-{args.partitions}-{args.replication_factor}'
    print('='*10, topic, '='*10)

    add_topic(argparse.Namespace(
        command='add',
        broker=args.brokers[0],
        topic=topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    ))

    producer_perf_test(topic, args.clients, args)
    consumer_perf_test(topic, args.clients, args)

    del_topic(argparse.Namespace(
        command='del',
        broker=args.brokers[0],
        topics=[topic]
    ))

else:
    for clients in range(1, args.max+1):
        topic = f'topic{clients}'
        print('='*10, topic, '='*10)

        producer_perf_test(topic, clients, args)
        consumer_perf_test(topic, clients, args)

        print()
        del_topic(argparse.Namespace(
            command='del',
            broker=args.brokers[0],
            topics=[topic]
        ))
