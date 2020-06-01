from datetime import datetime, timedelta
from itertools import dropwhile
import singer

from singer import utils

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


    def sync(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec

        # singer.write_state(self.state)

STREAM_OBJECTS = {
    'advertisables': Advertisables,
 }
