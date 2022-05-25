from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata


def is_cluster_available(bootstrap_servers="localhost:9092"):
    """
    :param bootstrap_servers: The ip:port of the server
    :return: True if server is running on the given address
    """
    try:
        cluster = ClusterMetadata(bootstrap_servers=bootstrap_servers)
        # client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        if len(cluster.brokers()) < 1 and len(cluster.topics()) < 1:
            # case: if no topic found
            return True
    except InterruptedError:
        # Kafka is not available
        return False


def create_topics(topic_names: list):
    """ function to automatically create topics given a list of names. """

    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='test'
    )

    topic_list = []
    for topic_name in topic_names:
        topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)