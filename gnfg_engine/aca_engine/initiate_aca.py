import json
from pprint import pprint
from datetime import datetime

from helper.neo4j_driver import graph_driver, mapping
from helper.functions import write_records_to_file


end_label_to_path_mapping = {
    "alert": "paths_to_alerts",
    "ad": "paths_to_artemis_ad",
    "case": "paths_to_cases",
    "counterparty": "paths_to_counterparty",
    "ncfta": "paths_to_ncfta_ids",
    "sar": "paths_to_sars",
    "party": "paths_to_party",
}


def get_aca_query():
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


def parse_results_formatted(raw_results):
    results = {}
    for record in raw_results:
        for path in record['paths']:
            appl_id = path.start_node['APP_ID']
            if appl_id not in results:
                results[appl_id] = {
                    "appl_id": appl_id,
                    "appl_start_dt": datetime.now(),
                    "latest_appl_chng_dt": datetime.now(),
                    "run_date" : datetime.now(),
	                "status" : path.start_node['SYSIX_STATUS'],
                    "paths_to_alerts": [],
                    "paths_to_artemis_ad": [],
                    "paths_to_cases": [],
                    "paths_to_counterparty": [],
                    "paths_to_ncfta_ids": [],
                    "paths_to_sars": [],
                    "paths_to_party": [],
                }

            end_label = next(iter(path.end_node.labels))
            end_label_path = end_label_to_path_mapping.get(end_label.lower(), None)

            if end_label_path:
                nodes_in_paths = []
                for node in path.nodes:
                    node_label = next(iter(path.end_node.labels))
                    node_key = mapping[node_label.lower()]['node_key']
                    node_dict = {
                        "id": node.id,
                        "label": node_label,
                        "prop": node_key,
                        "propid": node[node_key]
                    }
                    nodes_in_paths.append(node_dict)

                path_dict = {
                        "path_dt": datetime.now(),
                        "path_length": len(nodes_in_paths),
                        "path": nodes_in_paths
                    }
                results[appl_id][end_label_path].append(path_dict)

        return results


def start(graph_config, output_file, VERBOSE=True):
    neo4j_connection = graph_driver(**graph_config)
    query = get_aca_query()
    res = neo4j_connection.run_query(query)
    parsed_results = parse_results(res)

    if output_file:
        write_records_to_file(output_file + "aca.csv", parsed_results)
    else:
        pprint(parsed_results)
