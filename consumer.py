#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import time

from kafka import KafkaConsumer
from kafka import TopicPartition
from multiprocessing import Pool

################################################################################
VER_MAJOR = 0
VER_MINOR = 1
VER_PATCH = 0

################################################################################
def consume(idx, brokers, topic, partitions):
    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        client_id=f'nexgus-consumer-{idx+1}',
        group_id='nexgus-consumer-group',
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

    t_list = []
    while True:
        t0 = time.time()
        try:
            consumer.next_v2()
        except StopIteration:
            break
        else:
            t = time.time() - t0
            t_list.append(t)
    consumer.close()

    return t_list

################################################################################
def validate_args(args):
    if args.consumers < 1:
        raise ValueError(f'consumer count must equal to or large than 1.')
    return args

###############################################################################
def main(args):
    print('Consuming...', end='')
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
    del c

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

    total_rec = 0
    rec_per_sec = 0.0
    t = np.array([])
    for idx, ret in enumerate(result):
        t_list = ret.get()
        if len(t_list) == 0: continue
        t_list = np.array(t_list).astype(float)
        total_rec += t_list.size
        rec_per_sec += t_list.size / np.sum(t_list)
        t = np.concatenate((t, t_list))
    t = t * 1000 # Convert to milliseconds

    avg = np.mean(t)
    std = np.std(t)
    pr50 = np.percentile(t, 50)
    pr95 = np.percentile(t, 95)
    pr99 = np.percentile(t, 99)
    pr999 = np.percentile(t, 99.9)

    if args.csv_filepath:
        with open(args.csv_filepath, 'a') as fp:
            fp.write(f'consumer,,{args.consumers},'
                     f',{total_rec},,'
                     f'{partitions},{rec_per_sec},{avg},{std},{pr50},{pr95},'
                     f'{pr99},{pr999}\n')

    print()
    print(f'TotalRecords: {total_rec}')
    print(f'Throughput:   {rec_per_sec:.2f} rec/sec')
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
    parser.add_argument('-csv', '--csv-filepath',
        type=str,
        help='Path to a CSV file.')
    args = parser.parse_args()

    main(args)
