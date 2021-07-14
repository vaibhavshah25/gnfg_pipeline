from abc import ABC, abstractmethod
from pprint import pprint
from datetime import datetime

from helper.neo4j_driver import graph_driver, mapping
from helper.functions import write_records_to_file
from helper.kafka_helpers import get_kafka_consumer, send_data


class BaseEngine(ABC):
    def __init__(self, graph_config, **kwargs):
        self.neo4j_connection = graph_driver(**graph_config)
        self.parsed_results = None

        self.VERBOSE = False
        if 'VERBOSE' in kwargs:
            self.VERBOSE = kwargs['VERBOSE']

        if self.VERBOSE:
            pprint(kwargs)

        self.output_file = None
        if 'output_file' in kwargs:
            self.output_file = kwargs['output_file']

        self.kafka_meta_data = None
        if 'kafka_meta_data' in kwargs:
            self.kafka_meta_data = kwargs['kafka_meta_data']

        self.flush_to_console = None
        if 'flush_to_console' in kwargs:
            self.flush_to_console = kwargs['flush_to_console']

        self.return_results = None
        if 'return_results' in kwargs:
            self.return_results = kwargs['return_results']

    @staticmethod
    @abstractmethod
    def get_query():
        pass

    @staticmethod
    @abstractmethod
    def parse_results(raw_results):
        pass

    def handle_output(self):
        if self.output_file:
            self.write_to_file()

        if self.kafka_meta_data:
            self.publish_to_kafka()

        if self.flush_to_console:
            pprint(self.parsed_results)

        if self.return_results:
            return self.parsed_results

    def write_to_file(self):
        write_records_to_file(self.output_file + "aca.csv", self.parsed_results)

    def publish_to_kafka(self):
        data = {
                'operation': 'insert',
                'db': self.kafka_meta_data['db'],
                'rule': self.kafka_meta_data['rule'],
                'data': self.parsed_results,
            }
        send_data(data, self.kafka_meta_data['process_name'], self.kafka_meta_data['kafka_config'])

    def start(self):
        query = self.get_query()
        if query:
            res = self.neo4j_connection.run_query(query)
            self.parsed_results = self.parse_results(res)

        data_to_return = None
        if self.parsed_results:
            data_to_return = self.handle_output()

        return data_to_return
