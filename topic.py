#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin import NewTopic
from kafka import TopicPartition

################################################################################
def add_topic(args):
    admin = KafkaAdminClient(bootstrap_servers=[args.broker])
    admin.create_topics([
        NewTopic(
            name=args.topic, 
            num_partitions=args.partitions, 
            replication_factor=args.replication_factor, 
        ),
    ])
    admin.close()

################################################################################
def del_topic(args):
    admin = KafkaAdminClient(bootstrap_servers=[args.broker])
    admin.delete_topics(topics=args.topics)
    admin.close()

################################################################################
def desc_topic(args):
    consumer = KafkaConsumer(bootstrap_servers=[args.broker])
    topics = consumer.topics()
    if args.topic not in topics:
        consumer.close()
        print(f'Topic "{args.topic}" not in cluster.')
    else:
        partitions = consumer.partitions_for_topic(args.topic)
        tp_list = []
        for p in partitions:
            tp = TopicPartition(args.topic, p)
            tp_list.append(tp)
        beginning_offsets = consumer.beginning_offsets(tp_list)
        end_offsets = consumer.end_offsets(tp_list)

        print(f'Topic: {args.topic}')
        print(f'Partition: {partitions}')
        print(f'Beginning Offsets: {list(beginning_offsets.values())}')
        print(f'End Offsets: {list(end_offsets.values())}')

################################################################################
def list_topics(args):
    consumer = KafkaConsumer(bootstrap_servers=[args.broker])
    topics = consumer.topics()
    consumer.close()

    for topic in topics:
        print(topic)

################################################################################
def main(args):
    funcs = {
        'list': list_topics,
        'add':  add_topic,
        'del':  del_topic,
        'desc': desc_topic,
    }
    func = funcs[args.command]
    func(args)

################################################################################
################################################################################
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Kafka topic management.', 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(
        title='Available commands',
        description='Operation be executed.',
        dest='command',
        help='Description',
    )

    cmd_list = subparsers.add_parser('list',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help='List all topics.')
    cmd_add = subparsers.add_parser('add',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help='Add a topic.')
    cmd_del = subparsers.add_parser('del',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help='Delete a topic.')
    cmd_desc = subparsers.add_parser('desc',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help='Describe a topic.')

    cmd_add.add_argument('broker',
        type=str,
        help='Broker.')
    cmd_add.add_argument('topic',
        type=str, 
        help='Topic.')
    cmd_add.add_argument('--partitions',
        type=int, default=1, 
        help='Partition count.')
    cmd_add.add_argument('--replication-factor',
        type=int, default=1, 
        help='Replication factor.')

    cmd_del.add_argument('broker',
        type=str,
        help='Broker.')
    cmd_del.add_argument('topics',
        type=str, nargs='+', 
        help='Topics.')

    cmd_desc.add_argument('broker',
        type=str,
        help='Broker.')
    cmd_desc.add_argument('topic',
        type=str, 
        help='Topic.')

    cmd_list.add_argument('broker',
        type=str,
        help='Broker.')

    args = parser.parse_args()

    main(args)
