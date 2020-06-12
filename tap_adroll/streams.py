import datetime

from singer import utils
import singer

LOGGER = singer.get_logger()

class Advertisables:
    stream_id = 'advertisables'
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
    stream_id = 'ads'
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
        # TODO: Adjust the parent child relationship?
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec


class AdReports:
    stream_id = 'ad_reports'
    stream_name = 'ad_reports'
    endpoint = 'report/ad'
    key_properties = ["eid", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]


    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def generate_daily_date_windows(self):
        bookmark = singer.bookmarks.get_bookmark(self.state, self.stream_name, self.replication_keys[0])
        start = utils.strptime_to_utc(bookmark or self.config['start_date'])
        window_size = datetime.timedelta(days=1)
        now = utils.now() + datetime.timedelta(days=1) # Go until tomorrow for day-windows
        while start < now:
            end = start + window_size
            if end > now:
                end = now
            yield start, end
            start = end


    def sync(self):
        # TODO: Adjust the parent child relationship?
        advertisables = Advertisables(self.client, self.config, self.state)

        # Daily Pagination writes a bookmark after each day
        for start_date, end_date in self.generate_daily_date_windows():
            request_start = datetime.datetime.strftime(start_date, "%m-%d-%Y")
            request_end = datetime.datetime.strftime(end_date, "%m-%d-%Y")
            for advertisable_eid in advertisables.get_all_advertisable_eids():
                LOGGER.info("Syncing %s for advertisable %s between %s and %s", self.stream_id,
                            advertisable_eid, request_start, request_end)
                records = self.client.get(self.endpoint, params={
                    'advertisable': advertisable_eid,
                    'data_format': 'entity',
                    'start_date': request_start,
                    'end_date': request_end,
                })
                for rec in records.get('results'):
                    rec['date'] = utils.strftime(start_date)
                    yield rec

            # Write bookmark after syncing all Advertisables for the day
            singer.bookmarks.write_bookmark(self.state, self.stream_name, self.replication_keys[0], utils.strftime(start_date))
            singer.write_state(self.state)


class Segments:
    #advertisable/get_segments
    stream_id = 'segments'
    stream_name = 'segments'
    endpoint = 'advertisable/get_segments'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def sync(self):
        # TODO: Adjust the parent child relationship?
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec

class Campaigns:
    stream_id = 'campaigns'
    stream_name = 'campaigns'
    endpoint = 'advertisable/get_campaigns'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def sync(self):
        # TODO: Can switch on `is_active` by default "True" returning only active campaigns
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec


class AdGroups:
    stream_id = 'ad_groups'
    stream_name = 'ad_groups'
    endpoint = 'advertisable/get_adgroups'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


    def sync(self):
        # TODO: Can switch on `camp_active` by default "True" returning only for active campaigns
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec



STREAM_OBJECTS = {
    'advertisables': Advertisables,
    'ads': Ads,
    'ad_groups': AdGroups,
    'ad_reports': AdReports,
    'segments': Segments,
    'campaigns': Campaigns,
}
