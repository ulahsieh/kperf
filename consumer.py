#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time

from kafka import KafkaConsumer
from kafka import TopicPartition
from multiprocessing import Pool

################################################################################
VER_MAJOR = 0
VER_MINOR = 2
VER_PATCH = 0

################################################################################
def consume(idx, brokers, topic, partitions):
    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        client_id=f'perf-consumer-{idx+1}',
        group_id='perf-consumer-group',
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
    )

    # What partition this consumer should subscribe?
    if partitions == 1:
        partition = 0
    elif idx < partitions:
        partition = idx
    else:
        partition = idx % partitions

    # Subscribe to the topic
    # We don't use dynamically partition assignment (subscribe()) since we 
    # have to use seed_to_beginning().
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    t0 = time.time()
    rec = 0
    while True:
        try:
            consumer.next_v2()
            rec += 1
        except StopIteration:
            break

    t = time.time() - t0 - 1 # since we waited for 1000mS for timeout
    consumer.close()

    return idx, t, rec

################################################################################
def validate_args(args):
    if args.consumers < 1:
        raise ValueError(f'consumer count must equal to or large than 1.')
    return args

###############################################################################
def main(args):
    args = validate_args(args)

    # Check if the topic in cluster
    c = KafkaConsumer(bootstrap_servers=args.brokers)
    topics = c.topics()
    if args.topic not in topics:
        raise ValueError(f'The topic {args.topic} not in the Kafka cluster.')

    # Get the partition count
    partitions = c.partitions_for_topic(args.topic)
    partitions = len(partitions)
    c.close()

    t0 = time.time()
    result = []
    pool = Pool(processes=args.consumers)
    for idx in range(args.consumers):
        result.append(
            pool.apply_async(
                func=consume,
                args=(
                    idx,
                    args.brokers,
                    args.topic,
                    partitions,
                )
            )
        )
    pool.close()
    pool.join()
    t1 = time.time() - t0

    records = 0
    rec_per_sec = 0.0
    for ret in result:
        idx, t, rec = ret.get()
        throughput_rec = rec / t
        if args.show_each:
            print('-'*50)
            print(f'perf-consumer-{idx+1}:')
            print(f'    Records:    {rec}')
            print(f'    Elapse:     {t:.3f} sec')
            print(f'    Throughput: {throughput_rec:.2f} rec/sec')
        records += rec
        rec_per_sec += throughput_rec

    if args.csv_filepath:
        dirname = os.path.dirname(args.csv_filepath)
        if dirname: os.makedirs(dirname, exist_ok=True)
        if not os.path.exists(args.csv_filepath):
            with open(args.csv_filepath, 'w') as fp:
                fp.write('Type,Topic,Partitions,Clients,Acks,RecPerCli,'
                         'RcvdRec,RecPerSec\n')
        with open(args.csv_filepath, 'a') as fp:
            fp.write(f'producer,{args.topic},{partitions},'
                     f'{args.consumers},,,{records},'
                     f'{rec_per_sec}')

    print('='*50)
    print(f'Consumer:')
    print(f'    Records:    {records}')
    print(f'    Elapse:     {t1:.3f} sec')
    print(f'    Throughput: {rec_per_sec:.2f} rec/sec')

###############################################################################
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Kafka consumer performance test.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-V', '--version', 
        action='version',
        version=f'{VER_MAJOR}.{VER_MINOR}.{VER_PATCH}')
    parser.add_argument('-b', '--brokers',
        type=str, nargs='+', required=True, 
        help='Kafka broker list (bootstrap servers). Each broker is '
             'represented in HOST[:PORT] format. The default port is '
             '9092.')
    parser.add_argument('-t', '--topic',
        type=str, required=True, 
        help='Topic.')
    parser.add_argument('-c', '--consumers',
        type=int, default=1,
        help='Consumer count.')
    parser.add_argument('--show-each',
        action='store_true',
        help='Show metric of each producer.')
    parser.add_argument('-csv', '--csv-filepath',
        type=str,
        help='Path to a CSV file.')
    args = parser.parse_args()

    main(args)
