# python libraries
import sys
import argparse

from aca_engine.helper.functions import load_yaml
from aca_engine.aca_get_paths import simulate_get_aca_paths
from aca_engine.helper.kafka_helpers import get_kafka_consumer, send_data


process_name = 'GNFG'


def main():
    print('main')
    parser = argparse.ArgumentParser(prog='gnfg_kafka_scheduler', usage='./%(prog)s.py -y yaml_file',
                                     description='Start GNFG Kafka Scheduler')

    parser.add_argument('-y', '--yaml', help='Config file in yaml format',
                        type=str, required=True)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # parsing the args
    args = parser.parse_args()

    # load yaml file
    config = load_yaml(args.yaml)

    # topic from where the messages are being received
    if config['debug_params']['INPUT_MESSAGE_PRINT']:
        print('*** Message received from topic ' + str(config['kafka_params']) + ' ***')

    # set kafka parameters
    consumer = get_kafka_consumer(config['kafka_params'])

    # get the number of partitions for this topic, this will be equal to max jobs
    # partitions = consumer.partitions_for_topic(config['kafka_params']['topics'][CONSUMING_FROM_TOPIC])

    # uncomment only for debugging
    # consumer.poll()
    # consumer.seek_to_beginning()

    # main loop
    buffer_size = config['job_params']['buffer_size']

    while True:
        buffer = 0
        for message in consumer:

            if config['debug_params']['INPUT_MESSAGE_VERBOSE_PRINT']:
                print(f'*** {process_name} message ***')
                print(message)
            
            # Calling the engine
            graph_config = config['graph']
            export_to_graph = config['aca_params']['export_to_graph']
            paths = simulate_get_aca_paths(message.value, graph_config, export_to_graph)

            data = {
                'operation': 'insert',
                'db': 'GNFG_2',
                'rule': 'aca',
                'data': paths,
            }
            send_data(data, process_name, config)

            if buffer == buffer_size:
                break

            buffer += 1


if __name__ == '__main__':
    main()
