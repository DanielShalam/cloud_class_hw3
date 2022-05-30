'''
This whole script purpose is to create a producer that listen to the events from wiki and produce an event for them to be consumed
Maybe we should split this events according to the data and send it to different consumers
'''

import json
import argparse
import cluster_utils
from kafka import KafkaProducer
from sseclient import SSEClient as EventSource
from kafka.errors import NoBrokersAvailable
import subprocess

# 1. Bot or Human
# 2. User id/Name
# 3. Timestamp
# 4. Page id/Name
# 5. URL
# 6. To be continue


def construct_create_event(event_data):
    # define the structure of the json event that will be published to kafka topic
    event = {"page_id": event_data['page_id'],
             "domain": event_data['meta']['domain'],
             "timestamp": event_data['meta']['dt'],
             "user_name": event_data['performer']['user_text'],
             "user_is_bot": event_data['performer']['user_is_bot']}
    return event


def construct_edit_event(event_data):
    # define the structure of the json event that will be published to kafka topic
    event = {"page_id": event_data['id'],
             "domain": event_data['meta']['domain'],
             "timestamp": event_data['meta']['dt'],
             "user_name": event_data['user'],
             "user_is_bot": event_data['bot']}
    return event


def create_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])',
                        type=str)
    parser.add_argument('--topic_name', default='wikipedia-events', help='Destination topic name', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int,
                        default=50)

    return parser.parse_args()


def main():
    # parse command line arguments
    args = parse_command_line_arguments()

    # # check if the server is already running
    # if not cluster_utils.is_cluster_available():
    #     print("Cluster is not running.\nInitializing Zookeeper and kafka server...")
    #     # if it's not, we will create a new one
    #     # run zookeeper
    #     subprocess.call(['.\\run-zookeeper.sh', 'disown'], shell=True, cwd='..\\bash_files\\')
    #     # run kafka server
    #     subprocess.call(['.\\run-kafka.sh', 'disown'], shell=True, cwd='..\\bash_files\\')

    # print(f"Cluster is {'Running' if cluster_utils.is_cluster_available() else 'Still Not running. Error.'}")

    # init producer
    producer = create_kafka_producer(args.bootstrap_server)

    # create new topic for each task
    topics = ['recentchange', 'page-create']

    # consume websocket
    url = f'https://stream.wikimedia.org/v2/stream/{",".join(topics)}'

    print('Messages are being published to Kafka topic')
    messages_count = 0
    for event in EventSource(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                # filter out events, keep only article edits (mediawiki.recentchange stream)
                event_topic = event_data['meta']['topic'].split('.')[-1]
                # construct valid json event
                if event_topic == topics[0] and event_data['type'] == 'edit':
                    continue
                    event_to_send = construct_edit_event(event_data)
                elif event_topic == topics[1]:
                    event_to_send = construct_create_event(event_data)
                else:
                    continue

                producer.send('dtest', event_to_send)
                messages_count += 1
                print(messages_count)

        if messages_count >= args.events_to_produce:
            print('Producer will be killed as {} events were produced'.format(args.events_to_produce))
            exit(0)


if __name__ == "__main__":
    main()
