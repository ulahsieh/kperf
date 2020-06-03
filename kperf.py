#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse

from consumer import main as cmain
from producer import main as pmain

################################################################################
VER_MAJOR = 0
VER_MINOR = 1
VER_PATCH = 0

################################################################################
parser = argparse.ArgumentParser(
    description='Kafka performance test.', 
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-V', '--version', 
    action='version',
    version=f'{VER_MAJOR}.{VER_MINOR}.{VER_PATCH}')
parser.add_argument('-b', '--brokers', 
    type=str, nargs='+', default='localhost',
    help='Kafka borker host[:port]. The default port is 9092.')
parser.add_argument('-i', '--iterations', 
    type=int, default=50000, 
    help='Record generation count for each producer.')
parser.add_argument('-m', '--max', 
    type=int, default=12, 
    help='Maximum client count.')
parser.add_argument('-a', '--acks',
    choices=['0', '1', 'all'], default='1',
    help='The number of acknowledgments the producer requires the leader '
         'to have received before considering a request complete. This '
         'controls the durability of records that are sent. The '
         'following settings are common: 0: Producer will not wait for '
         'any acknowledgment from the server. 1: Wait for leader to '
         'write the record to its local log only. all: Wait for the full '
         'set of in-sync replicas to write the record.')
parser.add_argument('-ds', '--data-size',
    type=int, default=100,
    help='Data size for each record.')
parser.add_argument('-csv', '--csv-filepath',
    type=str,
    help='Path to a CSV file.')
args = parser.parse_args()

if args.csv_filepath:
    with open(args.csv_filepath, 'w') as fp:
        fp.write('Type,DataSize,ClientCount,Partitions,Iterations,TotalRecord,'
                 'Errors,Throughput,Latency,Deviation,PR50,PR95,PR99,PR99.9\n')

for idx in range(args.max):
    topic = f'topic{idx+1}'
    print(f'========== {topic} ==========')

    pargs = argparse.Namespace(
        brokers=args.brokers,
        topic=topic,
        partitions=idx+1,
        replication_factor=1,
        producers=idx+1,
        acks=args.acks,
        compression_type=None,
        batch_size=16384,
        iterations=args.iterations,
        data_size=args.data_size,
        csv_filepath=args.csv_filepath,
    )
    pmain(pargs)

    cargs = argparse.Namespace(
        brokers=args.brokers,
        topic=topic,
        consumers=idx+1,
        csv_filepath=args.csv_filepath,
    )
    cmain(cargs)
