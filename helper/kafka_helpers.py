import yaml
import json

# third party libraries

import hashlib

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


def hashify(word):
    """
    :param word: string
    :return: md5 hash of the given word
    """
    m = hashlib.md5()
    m.update(word.encode('utf-8'))
    gd_hash = m.hexdigest()
    return gd_hash


def create_metadata(data, client_name):
    """
    :param data: string
    :param client_name: string
    :return: metadata dictionary
    """
    # Creating Payload
    metadata = {
        'data': data,
        'client_name': client_name,  # add the client_name to the message
        'metadata': {  # add the metadata
            'create_date': '',  # create date
            'batch_size': '1',  # TODO: How to compute this? - related to the chunking of messages above
            'batch_total': '1',  # TODO: How to compute this? - related to the chunking of messages above
            'batch_entry': '1',  # TODO: How to compute this? - related to the chunking of messages above
        },
    }

    # batch hash
    # TODO: How to compute this?
    metadata['metadata']['batch_hash'] = hashify(str(metadata))

    return metadata


def get_kafka_consumer(kafka_params):
    """
    Gets a kafka consumer object based on the provided params
    """
    
    # set kafka parameters
    topics = kafka_params['topics']['consuming_from']
    group_id = kafka_params['group_ids']['consuming_from']
    auto_offset_reset = kafka_params['auto_offset_reset']
    bootstrap_servers = kafka_params['bootstrap_servers']
    
    # create a kafka consumer
    consumer = KafkaConsumer(
        topics,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        consumer_timeout_ms=5000,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    return consumer


def get_kafka_producer(kafka_params):
    """
    Gets a kafka consumer object based on the provided params
    """

    # set kafka parameters
    bootstrap_servers = kafka_params['bootstrap_servers']

    # create a kafka consumer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    return producer


def send_data(paths, process_name, config):

    # topic to where the message will be published
    PRODUCING_TO_TOPIC = config['kafka_params']['topics']['producing_to']
    if config['debug_params']['INPUT_MESSAGE_PRINT']:
        print('*** Message sent to ' + str(PRODUCING_TO_TOPIC) + '***')

    payload = create_metadata(paths, process_name)

    if config['debug_params']['INPUT_MESSAGE_VERBOSE_PRINT']:
        print('*** ACA Pair Builder output payload ***')
        print(payload)
        print('*** End ACA Pair Builder output payload message ***')

    # produce to topic 3

    producer = get_kafka_producer(config['kafka_params'])
    future = producer.send(PRODUCING_TO_TOPIC, payload)

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        import traceback
        traceback.print_exc()
        print('producer exception...')
        print(e)
        pass


def send_email(email_text, params):
    """
    """
    sender = params['sender']
    to = params['to'] 
    subject = params['subject']
    pwd = params['pwd']
    message = f'Subject: {subject}\n\n{email_text}'

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()   
        server.login(sender, pwd)
        server.sendmail(sender, to, message)
        server.close()
    except Exception as e:
        pass

