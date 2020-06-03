# Kafka Java Performance Test

## Introduction
Since the performance test tools of Kafka is hard to use, I program this tool 
to perform test.

## Prerequisties
1.  Python 3.5+
1.  kafka-python

## Common Conditions for Examples:
-   Kafka bin dirpath: /opt/kafka/bin
-   Iterations (records per client): 50000000
-   Data Size: 100 bytes
-   acks: 1  

## Examples
1.  Broker: kafka.broker.1:9092  
    Partitions: 3  
    Clients: 3  
    Replication factor: 3
    ```bash
    python3 kjperf.py \
        --brokers kafka.broker.1 \
        --dirname /opt/kafka/bin \
        --iterations 50000000 \
        --clients 3 \
        --partitions 3 \
        --replication-factor 3 \
        --csv-filepath ./logfile.csv
    ```

1.  Broker: kafka.broker.2:30303  
    Partitions: 2  
    Clients: 4  
    Replication factor: 3
    ```bash
    python3 kjperf.py \
        -b kafka.broker.2:30303 \
        -d /opt/kafka/bin \
        -i 50000000 \
        -c 4 \
        -p 2 \
        -rf 3 \
        -csv ./logfile.csv
    ```

1.  Broker: 172.31.0.100:9092  
    Partitions: 1-6  
    Clients: 1-6  
    Replication factor: 1  
    ```bash
    python3 kjperf.py \
        -b 172.31.0.100 \
        -d /opt/kafka/bin \
        -i 50000000 \
        --max 6 \
        -csv ./logfile.csv
    ```
