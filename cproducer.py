#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import os
import string
import time

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from multiprocessing import Pool
from ctopic import add_topic

################################################################################
VER_MAJOR = 0
VER_MINOR = 2
VER_PATCH = 0

################################################################################
def create_topic(brokers, topic, partition_count=1, replica_count=1):
    """Create a topic if it does not exist.

    Args:
        brokers (list): The 'host[:port]' list that the producer should 
            contact to bootstrap initial cluster metadata.
        topic (str): Topic where the message will be published.
        partition_count (int): Specified partition number (default 1).
        replica_count (int): Specified replication factor (default 1).

    Returns:
        partitions (list): A set including partition number.

    """
    c = Consumer({'bootstrap.servers': ','.join(brokers), 'group.id': 'a_consumer'})
    topics = c.list_topics().topics
    c.close()

    if topic in topics.keys():
        partitions = list(topics[topic].partitions.keys())
    else:
        new_topic = NewTopic(
            topic=topic,
            num_partitions=partition_count,
            replication_factor=replica_count,
        )
        admin = AdminClient({'bootstrap.servers': ','.join(brokers),})
        status = admin.create_topics([new_topic])
        while status[topic].done()==False:
            pass
        partitions = [p for p in range(partition_count)]

    return partitions

################################################################################
def produce(idx, brokers, topic, partitions, 
            acks=1, iterations=10, size=100, is_sync=False):
    """Create a producer and generate required messages."""
    random.seed(int(time.time()*1000000))
    source = string.ascii_letters + string.digits

    cfg = {
        'bootstrap.servers': ','.join(brokers),
        'client.id': f'perf-producer-{idx+1}',
        'acks': acks,
    }
    producer = Producer(**cfg)

    retries = 0
    t0 = time.time()
    for iteration in range(iterations):
        payload = ''.join(random.choice(source) for _ in range(size))
        payload = payload.encode('utf-8')

        ok = False
        while True:
            try:
                producer.produce(topic, value=payload)
            except BufferError:
                ok = False
            else:
                ok = True
            if ok: break
            producer.poll(0)
            retries += 1
        if is_sync: 
            producer.poll()

    producer.flush()
    t = time.time() - t0

    return idx, t, retries

################################################################################
def validate_args(args):
    if args.acks and args.acks in ('0', '1'):
        args.acks = int(args.acks)
    if args.partitions < 1:
        raise ValueError(f'patitions must equal to or large than 1.')
    if args.replication_factor < 1:
        raise ValueError(f'replication-factor must equal to or large than 1.')
    if args.producers < 1:
        raise ValueError(f'producer count must equal to or large than 1.')
    if args.batch_size < 1:
        raise ValueError(f'batch size must equal to or large than 1.')
    #if args.linger < 0:
    #    raise ValueError(f'the minimum value of linger must be 0.0 (seconds).')
    return args

################################################################################
def main(args):
    args = validate_args(args)

    partitions = create_topic(
        brokers=args.brokers, 
        topic=args.topic, 
        partition_count=args.partitions, 
        replica_count=args.replication_factor
    )

    t0 = time.time()
    result = []
    pool = Pool(processes=args.producers)
    for idx in range(args.producers):
        result.append(
            pool.apply_async(
                func=produce,
                args=(
                    idx, 
                    args.brokers,
                    args.topic,
                    partitions,
                    args.acks,
                    args.iterations,
                    args.data_size,
                    args.is_sync,
                )
            )
        )
    pool.close()
    pool.join()
    t1 = time.time() - t0

    records = 0
    rec_per_sec  = 0.0
    total_retries = 0
    for ret in result:
        idx, t, retries = ret.get()
        sent = args.iterations
        throughput_rec = sent / t
        throughput_bytes = args.data_size * sent / t
        if args.show_each:
            print('-'*50)
            print(f'perf-producer-{idx+1}:')
            print(f'    Records:    {sent}')
            print(f'    Retries:    {retries}')
            print(f'    Elapse:     {t:.3f} sec')
            print(f'    Throughput: {throughput_rec:.2f} rec/sec')
        records += sent
        total_retries += retries
        rec_per_sec += throughput_rec

    if args.csv_filepath:
        dirname = os.path.dirname(args.csv_filepath)
        if dirname: os.makedirs(dirname, exist_ok=True)
        if not os.path.exists(args.csv_filepath):
            with open(args.csv_filepath, 'w') as fp:
                fp.write('Type,Topic,Partitions,Clients,Acks,RecPerCli,'
                         'RcvdRec,RecPerSec\n')
        with open(args.csv_filepath, 'a') as fp:
            fp.write(f'producer,{args.topic},{args.partitions},'
                     f'{args.producers},{args.acks},{args.iterations},,'
                     f'{rec_per_sec}')

    print('-'*50)
    print('Producer:')
    print(f'    Records:    {records}')
    print(f'    Retries:    {total_retries}')
    print(f'    Elapse:     {t1:.3f} sec')
    print(f'    Throughput: {rec_per_sec:.2f} rec/sec')

################################################################################
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Kafka producer performance test.', 
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
    parser.add_argument('-pt', '--partitions',
        type=int, default=1,
        help='Partition count. If the topic does not exist.')
    parser.add_argument('-rf', '--replication-factor',
        type=int, default=1,
        help='Replcation factor if the topic does not exist.')
    parser.add_argument('-pd', '--producers',
        type=int, default=1,
        help='Producer count.')
    parser.add_argument('-sync', '--is-sync',
        action='store_true',
        help='Synchronous (blocking) sending.')
    parser.add_argument('-a', '--acks',
        choices=['0', '1', 'all'], default='1',
        help='The number of acknowledgments the producer requires the leader '
             'to have received before considering a request complete. This '
             'controls the durability of records that are sent. The '
             'following settings are common: 0: Producer will not wait for '
             'any acknowledgment from the server. 1: Wait for leader to '
             'write the record to its local log only. all: Wait for the full '
             'set of in-sync replicas to write the record.')
    parser.add_argument('-ct', '--compression-type',
        type=str, choices=['gzip', 'snappy', 'lz4', 'zstd'],
        help='Producer count.')
    parser.add_argument('-bs', '--batch-size',
        type=int, default=16384,
        help='Batch size. The producer will attempt to batch records together '
             'into fewer requests whenever multiple records are being sent to '
             'the same partition. This helps performance on both the client '
             'and the server. This configuration controls the default batch '
             'size in bytes.')
    parser.add_argument('-i', '--iterations',
        type=int, default=10,
        help='Iterations per producer.')
    parser.add_argument('-ds', '--data-size',
        type=int, default=100,
        help='Data size for each message.')
    parser.add_argument('--show-each',
        action='store_true',
        help='Show metrics of each producer.')
    parser.add_argument('-csv', '--csv-filepath',
        type=str,
        help='Path to a CSV file.')
    args = parser.parse_args()

    main(args)
