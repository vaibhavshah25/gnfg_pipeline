from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

mapping = {
    'alert': {'node_label': 'Alert', 'node_key': 'ALERT_ID'},
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


class graph_driver():
    def __init__(self, uri_scheme='bolt', host='localhost', port='7687', username='neo4j', password=''):
        
        self.uri_scheme = uri_scheme
        self.host = host
        self.port = port

        self.username = username
        self.password = password

        self.connection_uri = "{uri_scheme}://{host}:{port}".format(uri_scheme=self.uri_scheme, host=self.host,
                                                                    port=self.port)
        self.auth = (self.username, self.password)
        self.driver = GraphDatabase.driver(self.connection_uri, auth=self.auth)

    def __del__(self):
        self._close_driver()

    def _close_driver(self):
        if self.driver:
            self.driver.close()

    def run_query(self, query):
        return self.run_single_query(query)

    def run_single_query(self, query):
        print(query)
        res = None
        with self.driver.session() as session:
            raw_res = session.run(query)
            res = self.format_raw_res(raw_res)
        return res

    def test_connection(self):
        return self.run_single_query("MATCH (n) RETURN COUNT(n) as nodes, sum(size((n)-[]->())) as relations")

    @staticmethod
    def format_raw_res(raw_res):
        res = []
        for r in raw_res:
            res.append(r)
        return res


def main():
    driver = graph_driver()
    res = driver.test_connection()
    print(res)


if __name__ == "__main__":
    main()
