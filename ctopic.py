#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

################################################################################
def add_topic(args):
    new_topic = NewTopic(
        topic=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    admin = AdminClient({'bootstrap.servers': f'{args.broker}',})
    status = admin.create_topics([new_topic])
    while status[args.topic].running(): pass

################################################################################
def del_topic(args):
    admin = AdminClient({'bootstrap.servers': f'{args.broker}',})
    status = admin.delete_topics(topics=args.topics)
    while True:
        counter = 0
        for topic in args.topics:
            if status[topic].done(): counter += 1
        if counter == len(args.topics):
            break

################################################################################
def desc_topic(args):
    c = Consumer({
        'bootstrap.servers': f'{args.broker}',
        'group.id': 'confluent-kafka-describe-topic',
    })
    topics = c.list_topics().topics

    if args.topic not in topics.keys():
        print(f'Topic "{args.topic}" not in cluster.')
    else:
        topic_metadata = topics[args.topic]
        partitions, leaders, replicas, isrs = [], [], [], []
        for metadata in topic_metadata.partitions.values():
            partitions.append(str(metadata.id))
            leaders.append(str(metadata.leader))
            replicas.append(str(metadata.replicas))
            isrs.append(str(metadata.isrs))
        partitions = ', '.join(partitions)
        leaders    = ', '.join(leaders)
        replicas   = ', '.join(replicas)
        isrs       = ', '.join(isrs)
        print(f'Topic:     {topic_metadata.topic}')
        print(f'Partition: {partitions}')
        print(f'Leader:    {leaders}')
        print(f'Replica:   {replicas}')
        print(f'ISRs:      {isrs}')

    c.close()

################################################################################
def list_topics(args):
    c = Consumer({
        'bootstrap.servers': f'{args.broker}',
        'group.id': 'confluent-kafka-list-topic',
    })
    metadata = c.list_topics()
    c.close()

    for topic in metadata.topics.keys():
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
    cmd_add.add_argument('-pt', '--partitions',
        type=int, default=1, 
        help='Partition count.')
    cmd_add.add_argument('-rf', '--replication-factor',
        type=int, default=1, 
        help='Replication factor.')

    cmd_del.add_argument('broker',
        type=str,
        help='Broker.')
    cmd_del.add_argument('topics',
        type=str, nargs='+',
        help='Topic.')

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
