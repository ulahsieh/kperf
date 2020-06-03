#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# sed -i 's/\x0//g' producer.py
import numpy as np
import random
import string
import time

from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.admin import NewTopic
from multiprocessing import Pool

################################################################################
VER_MAJOR = 0
VER_MINOR = 1
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
        partitions (set): A set including partition number.

    """
    consumer = KafkaConsumer(bootstrap_servers=brokers)
    topics = consumer.topics()

    if topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        consumer.close()
    else:
        consumer.close()
        admin = KafkaAdminClient(bootstrap_servers=brokers)
        admin.create_topics([
            NewTopic(
                name=topic, 
                num_partitions=partition_count, 
                replication_factor=replica_count, 
            ),
        ])
        admin.close()
        partitions = set([p for p in range(partition_count)])

    return partitions

################################################################################
def produce(seq, cid, brokers, topic, partitions, 
            acks=1, iterations=10, size=100):
    """Create a producer and generate required messages."""
    random.seed(int(time.time()*1000000))
    source = string.ascii_letters + string.digits

    partitions = list(partitions)

    producer = KafkaProducer(
        bootstrap_servers=brokers,
        client_id=cid,
        acks=acks,
    )

    # Assign start partition.
    if len(partitions) == 1:
        pidx = 0
    elif seq < len(partitions):
        pidx = seq
    else:
        pidx = seq % len(partitions)

    t0_list, t1_list = [], []
    for iteration in range(iterations):
        # Generate payload
        payload = ''.join(random.choice(source) for _ in range(size))
        payload = payload.encode('utf-8')
        timestamp = int(time.time() * 1000)

        t0_list.append(time.time())

        # Here we simulate a RoundRobinPartitioner since kafka-python
        # doesn't support it.
        partition = partitions[pidx]
        pidx += 1
        if pidx >= len(partitions): pidx = 0

        # send() is asynchronous (non-blocking).
        future = producer.send(
            topic=topic, 
            value=payload,
            partition=partition,
            timestamp_ms=timestamp,
        )

        # Wait for it, which means, force synchronous (blocking)
        try:
            future.get()
        except Exception as ex:
            t1_list.append(None)
        else:
            t1_list.append(time.time())

    producer.close()

    return t0_list, t1_list

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
    print('Producing...', end='')
    args = validate_args(args)

    partitions = create_topic(
        args.brokers, 
        args.topic, 
        args.partitions, 
        args.replication_factor
    )

    result = []
    pool = Pool(processes=args.producers)
    for idx in range(args.producers):
        result.append(
            pool.apply_async(
                func=produce,
                args=(
                    idx, 
                    f'nexgus-producer-{idx+1}',
                    args.brokers,
                    args.topic,
                    partitions,
                    args.acks,
                    args.iterations,
                    args.data_size,
                )
            )
        )
    pool.close()
    pool.join()

    rec_per_sec = 0.0
    errors = 0
    t = np.array([])
    for idx, ret in enumerate(result):
        t0_list, t1_list = ret.get()
        if len(t0_list) == 0: continue
        t0_list = np.array(t0_list).astype(float)
        t1_list = np.array(t1_list).astype(float)
        nan_list = np.isnan(t1_list)
        t_list = t1_list - t0_list
        t_list = t_list[~nan_list]
        rec_per_sec += t_list.size / np.sum(t_list)
        errors  += np.count_nonzero(nan_list)
        t = np.concatenate((t, t_list))

    t = t * 1000 # convert to milliseconds
    avg = np.mean(t)
    std = np.std(t)
    pr50 = np.percentile(t, 50)
    pr95 = np.percentile(t, 95)
    pr99 = np.percentile(t, 99)
    pr999 = np.percentile(t, 99.9)

    total_rec = args.iterations * args.producers

    if args.csv_filepath:
        with open(args.csv_filepath, 'a') as fp:
            fp.write(f'producer,{args.data_size},{args.producers},'
                     f'{len(partitions)},{args.iterations},{total_rec},'
                     f'{errors},{rec_per_sec},{avg},{std},{pr50},{pr95},'
                     f'{pr99},{pr999}\n')

    print()
    print(f'TotalRecords: {total_rec}')
    print(f'Errors: {errors}')
    print(f'Throughput: {rec_per_sec:.2f} rec/sec')
    print(f'Mean Latency: {avg:.2f}±{std:.2f} mS')
    print( 'Percentile:')
    print(f'  50%:   {pr50:.2f} mS')
    print(f'  95%:   {pr95:.2f} mS')
    print(f'  99%:   {pr99:.2f} mS')
    print(f'  99.9%: {pr999:.2f} mS')
    print()

###############################################################################
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
    parser.add_argument('-csv', '--csv-filepath',
        type=str,
        help='Path to a CSV file.')
    args = parser.parse_args()

    main(args)
