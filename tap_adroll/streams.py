import singer

LOGGER = singer.get_logger()

class Advertisables:
    stream_id = 'Advertisables'
    stream_name = 'advertisables'
    endpoint = 'organization/get_advertisables'
    key_properties = ["eid"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state

    def get_all_advertisable_eids(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec['eid']

    def sync(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec

class Ads:
    stream_id = 'Ads'
    stream_name = 'ads'
    endpoint = 'advertisable/get_ads'
    key_properties = ["eid"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state

    def sync(self):
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec

class AdReports:
    key_properties = ["eid", "date"]


STREAM_OBJECTS = {
    'advertisables': Advertisables,
    'ads': Ads,
}
