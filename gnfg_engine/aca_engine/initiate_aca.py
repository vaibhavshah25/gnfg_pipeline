import json
from pprint import pprint
from datetime import datetime

from gnfg_engine.base_engine.initiate_engine import BaseEngine

from helper.neo4j_driver import graph_driver, mapping
from helper.functions import write_records_to_file


class ACAEngine(BaseEngine):

    def __init__(self, graph_config, **kwargs):
        super().__init__(graph_config, **kwargs)
        self.end_label_to_path_mapping = {
            "alert": "paths_to_alerts",
            "ad": "paths_to_artemis_ad",
            "case": "paths_to_cases",
            "counterparty": "paths_to_counterparty",
            "ncfta": "paths_to_ncfta_ids",
            "sar": "paths_to_sars",
            "party": "paths_to_party",
        }

    @staticmethod
    def get_query():
        query = '''
        MATCH (a:Application {APP_ID:'120000003'})
        OPTIONAL MATCH party_path= (a)--()--(p:Party) 
        OPTIONAL MATCH sar_path= (a)-[*1..7]-(s:SAR) 
        OPTIONAL MATCH ad_path= (a)--()--(ad:Ad) 
        OPTIONAL MATCH counterparty_path= (a)-[*1..7]-(cp:CounterParty) 
        OPTIONAL MATCH ncfta_path= (a)--()--(ncfta:NCFTA) 
        WITH collect(DISTINCT party_path) + collect(DISTINCT sar_path) + collect(DISTINCT ad_path) + 
        collect(DISTINCT counterparty_path) + collect(DISTINCT ncfta_path) as paths
        RETURN paths
        '''
        return query

    @staticmethod
    def parse_results(raw_results):
        results = []
        for record in raw_results:
            for path in record['paths']:

                # end_label = next(iter(path.end_node.labels))

                nodes_in_paths = []

                for node in path.nodes:
                    node_label = next(iter(node.labels))
                    node_key = mapping[node_label.lower()]['node_key']
                    node_dict = {
                        "id": node.id,
                        "label": node_label,
                        "prop": node_key,
                        "propid": node[node_key]
                    }
                    nodes_in_paths.append(node_dict)

                results.append([datetime.now()] + nodes_in_paths)

            return results
