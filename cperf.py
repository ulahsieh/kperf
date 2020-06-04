#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os

from cconsumer import main as cmain
from cproducer import main as pmain
from ctopic import del_topic

################################################################################
VER_MAJOR = 0
VER_MINOR = 2
VER_PATCH = 0

################################################################################
def main(args):
    if args.csv_filepath:
        dirname = os.path.dirname(args.csv_filepath)
        if dirname: os.makedirs(dirname, exist_ok=True)
        if not os.path.exists(args.csv_filepath):
            with open(args.csv_filepath, 'w') as fp:
                fp.write('Type,Topic,Partitions,Clients,Acks,RecPerCli,'
                         'RcvdRec,RecPerSec\n')

    if args.clients is None:
        for idx in range(args.max):
            topic = f'topic{idx+1}-{idx+1}-{args.replication_factor}'
            del_topic(argparse.Namespace(
                broker=args.brokers[0],
                topics=[topic],
            ))
            print('='*50)
            print(f'Client:    {idx+1}')
            print(f'Topic:     {topic}')
            print(f'Partition: {idx+1}')
            print(f'Replica:   {args.replication_factor}')

            pargs = argparse.Namespace(
                brokers=args.brokers,
                topic=topic,
                partitions=idx+1,
                replication_factor=args.replication_factor,
                producers=idx+1,
                is_sync=False,
                acks=args.acks,
                compression_type=None,
                batch_size=16384,
                iterations=args.iterations,
                data_size=args.data_size,
                show_each=False,
                csv_filepath=args.csv_filepath,
            )
            pmain(pargs)

            cargs = argparse.Namespace(
                brokers=args.brokers,
                topic=topic,
                consumers=idx+1,
                show_each=False,
                csv_filepath=args.csv_filepath,
            )
            cmain(cargs)

            del_topic(argparse.Namespace(
                broker=args.brokers[0],
                topics=[topic],
            ))
            print()

    else:
        topic = (f'topic{args.clients}-{args.partitions}-'
                 f'{args.replication_factor}')
        del_topic(argparse.Namespace(
            broker=args.brokers[0],
            topics=[topic],
        ))
        print('='*50)
        print(f'Client:    {args.clients}')
        print(f'Topic:     {topic}')
        print(f'Partition: {args.partitions}')
        print(f'Replica:   {args.replication_factor}')

        pargs = argparse.Namespace(
            brokers=args.brokers,
            topic=topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            producers=args.clients,
            is_sync=False,
            acks=args.acks,
            compression_type=None,
            batch_size=16384,
            iterations=args.iterations,
            data_size=args.data_size,
            show_each=False,
            csv_filepath=args.csv_filepath,
        )
        pmain(pargs)

        cargs = argparse.Namespace(
            brokers=args.brokers,
            topic=topic,
            consumers=args.clients,
            show_each=False,
            csv_filepath=args.csv_filepath,
        )
        cmain(cargs)

        del_topic(argparse.Namespace(
            broker=args.brokers[0],
            topics=[topic],
        ))
        print()

################################################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Confluent Kafka Python performance test.', 
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
    parser.add_argument('-c', '--clients', 
        type=int,
        help='Client count.')
    parser.add_argument('-pt', '--partitions', 
        type=int,
        help='Partition count.')
    parser.add_argument('-m', '--max', 
        type=int, default=os.cpu_count(), 
        help='Maximum client count.')
    parser.add_argument('-rf', '--replication-factor', 
        type=int, default=1, 
        help='Replication factor.')
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

    if args.clients is not None:
        if args.partitions is None:
            raise ValueError('Must assign partitions while clients is set.')
        if args.clients < 1:
            raise ValueError('clients must equal to or larger than 1.')
        if args.partitions < 1:
            raise ValueError('partitions must equal to or larger than 1.')

    main(args)

