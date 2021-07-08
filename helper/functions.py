import sys
import os
import csv

import yaml
from neo4j import GraphDatabase, basic_auth

mapping = {'alert': {'node_label': 'Alert', 'node_key': 'ALERT_ID'},
           'account': {'node_label': 'Account', 'node_key': 'ACCT_NBR'},
           'case': {'node_label': 'Case', 'node_key': 'CASE_IDENTIFIER'},
           'ssn': {'node_label': 'SSN', 'node_key': 'SSN'},
           'address': {'node_label': 'Address', 'node_key': 'ADDRESS'},
           'addressgc': {'node_label': 'AddressGC', 'node_key': 'ADDRESS_GC'},
           'fraudccase': {'node_label': 'FraudCCase', 'node_key': 'C_CASE_ID'},
           'fraudclead': {'node_label': 'FraudCLead', 'node_key': 'CUST_NBR'},
           'email': {'node_label': 'Email', 'node_key': 'EMAIL'},
           'ip': {'node_label': 'IP', 'node_key': 'IP_ADR'},
           'phone': {'node_label': 'Phone', 'node_key': 'PHONE'},
           'community': {'node_label': 'Community', 'node_key': 'COM_ID'},
           'hub': {'node_label': 'Hub', 'node_key': 'HUB_ID'},
           'fraudalead': {'node_label': 'FraudALead', 'node_key': 'PRSN_ID'},
           'fraudblead': {'node_label': 'FraudBLead', 'node_key': 'B_FRAUD_ID'},
           'sar': {'node_label': 'SAR', 'node_key': 'SAR_ID'},
           'name': {'node_label': 'Name', 'node_key': 'NAME_NODE'},
           'party': {'node_label': 'Party', 'node_key': 'PARTY_ID'},
           'externalaccount': {'node_label': 'ExternalAccount', 'node_key': 'CNTR_PRTY_ACCT'},
           'ad': {'node_label': 'Ad', 'node_key': 'AD_GROUP'},
           'application': {'node_label': 'Application', 'node_key': 'APP_ID'},
           'authbuy': {'node_label': 'AuthBuy', 'node_key': 'AUTH_BUY'},
           'ncfta': {'node_label': 'NCFTA', 'node_key': 'NCFTA_ID'},
           'counterparty': {'node_label': 'CounterParty', 'node_key': 'CNTR_PARTY_ID'},
           }

primary_key_map = {}
for label in mapping:
    node = mapping[label]
    primary_key_map[node['node_label']] = node['node_key']

def get_neo4j_labels_and_properties():
    return primary_key_map


def load_yaml(filename):
    """
    Loads the yaml file and returns the result as dictionary

    :param filename (str), yaml file name and full path

    : return  yaml_dic (dict), dictionary containing the yaml file parameters
    """
    yaml_dic = {}
    with open(filename, 'r') as stream:
        try:
            yaml_dic = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return yaml_dic


def check_file_path(file_path):
    if not(os.path.isfile(file_path)):
        print(f'file does not exist: {file_path}', file=sys.stderr)
        sys.exit(1)


def check_dir_path(dir_path):
    if not(os.path.isdir(dir_path)):
        print(f'dir does not exist: {dir_path}', file=sys.stderr)
        sys.exit(1)


def read_input_file(file_path):
    """
    Reads input file.
    Header is string containing neo4j node label.
    Remaining lines are read into a list. These are the values of the primary properties.
    """
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        neo4j_label = header[0].strip()
        node_ids = []
        for row in reader:
            node_ids.append(str(row[0].strip()))
    return neo4j_label, node_ids


def write_records_to_file(file_path, records):
    """
    Reads input file.
    Header is string containing neo4j node label.
    Remaining lines are read into a list. These are the values of the primary properties.
    """
    with open(file_path, 'a') as f:
        for record in records:
            f.write(str(record) + '\n')


if __name__ == '__main__':
    pass
